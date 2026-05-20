use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedReceiver;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use tracing::{info, error};
use crate::proxy::ChatCompletionChunk;
use futures_util::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Uniform Telemetry Metrics record mapping to SQLite schemas
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryRecord {
    pub id: String,
    pub timestamp: i64,
    pub provider: String,
    pub model: String,
    pub status_code: u16,
    pub latency_ms: u64,
    pub ttft_ms: Option<u64>,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub estimated_cost: f64,
}

/// A wrapper stream that parses streamed chunks on-the-fly and records telemetry non-blockingly upon termination or drop.
pub struct TelemetryStream<S> {
    pub inner: S,
    pub record_template: TelemetryRecord,
    pub accumulated_chars: usize,
    pub start_time: Instant,
    pub tx: tokio::sync::mpsc::UnboundedSender<TelemetryRecord>,
}

impl<S> Stream for TelemetryStream<S>
where
    S: Stream<Item = Result<String, String>> + Unpin,
{
    type Item = Result<String, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(line))) => {
                if line.starts_with("data:") {
                    let content = line["data:".len()..].trim();
                    if content != "[DONE]" && !content.is_empty() {
                        if let Ok(chunk) = serde_json::from_str::<ChatCompletionChunk>(content) {
                            if let Some(choice) = chunk.choices.first() {
                                if let Some(text) = &choice.delta.content {
                                    self.accumulated_chars += text.len();
                                }
                            }
                        }
                    }
                }
                Poll::Ready(Some(Ok(line)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                self.emit_telemetry(200);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> TelemetryStream<S> {
    pub fn new(
        inner: S,
        record_template: TelemetryRecord,
        tx: tokio::sync::mpsc::UnboundedSender<TelemetryRecord>,
    ) -> Self {
        Self {
            inner,
            record_template,
            accumulated_chars: 0,
            start_time: Instant::now(),
            tx,
        }
    }

    fn emit_telemetry(&mut self, status_code: u16) {
        if self.record_template.prompt_tokens == 0 && self.accumulated_chars == 0 {
            return;
        }
        let (prompt_tokens, completion_tokens, estimated_cost) =
            calculate_estimated_cost(
                &self.record_template.model,
                self.record_template.prompt_tokens as usize * 4,
                self.accumulated_chars,
            );
        
        let mut record = self.record_template.clone();
        record.status_code = status_code;
        record.latency_ms = self.start_time.elapsed().as_millis() as u64;
        record.prompt_tokens = prompt_tokens;
        record.completion_tokens = completion_tokens;
        record.estimated_cost = estimated_cost;
        
        let _ = self.tx.send(record);
        
        // Zero out to prevent duplicate emissions
        self.record_template.prompt_tokens = 0;
        self.accumulated_chars = 0;
    }
}

impl<S> Drop for TelemetryStream<S> {
    fn drop(&mut self) {
        self.emit_telemetry(499); // 499 indicates Client Closed Connection early
    }
}

/// Initialize connection and execute high-speed connection pragmas
pub fn init_db_connection(conn: &mut Connection) -> Result<(), rusqlite::Error> {
    // Set Write-Ahead Logging (WAL) and synchronous optimization pragmas
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;"
    )?;

    // Create table if it doesn't exist
    conn.execute(
        "CREATE TABLE IF NOT EXISTS metrics (
            id TEXT PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            latency_ms INTEGER NOT NULL,
            ttft_ms INTEGER,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            estimated_cost REAL NOT NULL
        );",
        [],
    )?;

    info!("Telemetry Database connection initialized and table migrated successfully!");
    Ok(())
}

/// Dynamic Token and Cost Estimator
pub fn calculate_estimated_cost(model: &str, prompt_chars: usize, completion_chars: usize) -> (u32, u32, f64) {
    // Standard rule: 1 token is ~4 characters
    let prompt_tokens = (prompt_chars as f64 / 4.0).ceil() as u32;
    let completion_tokens = (completion_chars as f64 / 4.0).ceil() as u32;

    // Prices per million tokens
    let (prompt_price_per_m, completion_price_per_m) = match model.to_lowercase().as_str() {
        m if m.contains("gpt-4o-mini") => (0.15, 0.60),
        m if m.contains("gpt-4o") => (5.00, 15.00),
        m if m.contains("claude-3-5-sonnet") => (3.00, 15.00),
        m if m.contains("gemini-1.5-flash") => (0.075, 0.30),
        m if m.contains("gemini-1.5-pro") => (1.25, 3.75),
        _ => (1.00, 3.00), // Default conservative estimates
    };

    let prompt_cost = (prompt_tokens as f64 / 1_000_000.0) * prompt_price_per_m;
    let completion_cost = (completion_tokens as f64 / 1_000_000.0) * completion_price_per_m;
    let total_cost = prompt_cost + completion_cost;

    (prompt_tokens, completion_tokens, total_cost)
}

/// Flush batch using a single transaction to ensure maximum speed
fn flush_batch(conn: &mut Connection, batch: &mut Vec<TelemetryRecord>) -> Result<(), rusqlite::Error> {
    if batch.is_empty() {
        return Ok(());
    }

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO metrics (id, timestamp, provider, model, status_code, latency_ms, ttft_ms, prompt_tokens, completion_tokens, estimated_cost)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
        )?;

        for r in batch.iter() {
            stmt.execute((
                &r.id,
                r.timestamp,
                &r.provider,
                &r.model,
                r.status_code,
                r.latency_ms,
                r.ttft_ms,
                r.prompt_tokens,
                r.completion_tokens,
                r.estimated_cost,
            ))?;
        }
    }
    tx.commit()?;
    info!("Telemetry: Flushed batch of {} records to database.", batch.len());
    batch.clear();
    Ok(())
}

/// The main background worker loop that handles batch inserts cleanly without blocking Tokio threads
pub async fn start_telemetry_worker(
    mut rx: UnboundedReceiver<TelemetryRecord>,
    db_path: String,
    broadcast_tx: tokio::sync::broadcast::Sender<TelemetryRecord>,
) {
    info!("Starting telemetry background worker...");
    let mut conn = match Connection::open(&db_path) {
        Ok(c) => c,
        Err(e) => {
            error!("CRITICAL: Telemetry background worker failed to open SQLite db '{}': {}", db_path, e);
            return;
        }
    };

    if let Err(e) = init_db_connection(&mut conn) {
        error!("CRITICAL: Telemetry background worker failed to initialize DB schemas: {}", e);
        return;
    }

    let mut batch = Vec::with_capacity(10);
    let mut last_flush = Instant::now();
    let flush_interval = Duration::from_millis(500);

    loop {
        let recv_timeout = if batch.is_empty() {
            Duration::from_secs(3600 * 24) // 1 day
        } else {
            let elapsed = last_flush.elapsed();
            if elapsed >= flush_interval {
                Duration::from_millis(0)
            } else {
                flush_interval - elapsed
            }
        };

        let msg = tokio::select! {
            res = rx.recv() => res,
            _ = tokio::time::sleep(recv_timeout) => None,
        };

        if let Some(record) = msg {
            // Broadcast the record to all live SSE subscribers
            let _ = broadcast_tx.send(record.clone());
            
            batch.push(record);
            if batch.len() >= 10 {
                if let Err(e) = flush_batch(&mut conn, &mut batch) {
                    error!("Telemetry database batch write failed: {}", e);
                }
                last_flush = Instant::now();
            }
        } else {
            if rx.is_closed() && batch.is_empty() {
                break;
            }
            if !batch.is_empty() {
                if let Err(e) = flush_batch(&mut conn, &mut batch) {
                    error!("Telemetry database flush failed: {}", e);
                }
                last_flush = Instant::now();
            }
            if rx.is_closed() {
                break;
            }
        }
    }
    info!("Telemetry background worker shut down gracefully.");
}

/// Query recent telemetry records from the database
pub fn get_recent_records(db_path: &str, limit: usize) -> Result<Vec<TelemetryRecord>, rusqlite::Error> {
    let conn = Connection::open(db_path)?;
    let mut stmt = conn.prepare(
        "SELECT id, timestamp, provider, model, status_code, latency_ms, ttft_ms, prompt_tokens, completion_tokens, estimated_cost
         FROM metrics ORDER BY timestamp DESC LIMIT ?"
    )?;
    let rows = stmt.query_map([limit], |row| {
        Ok(TelemetryRecord {
            id: row.get(0)?,
            timestamp: row.get(1)?,
            provider: row.get(2)?,
            model: row.get(3)?,
            status_code: row.get(4)?,
            latency_ms: row.get(5)?,
            ttft_ms: row.get(6)?,
            prompt_tokens: row.get(7)?,
            completion_tokens: row.get(8)?,
            estimated_cost: row.get(9)?,
        })
    })?;

    let mut records = Vec::new();
    for r in rows {
        records.push(r?);
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_cost_estimator() {
        let (prompt_tok, comp_tok, cost) = calculate_estimated_cost("gpt-4o", 4000, 8000);
        assert_eq!(prompt_tok, 1000);
        assert_eq!(comp_tok, 2000);
        assert!((cost - 0.035).abs() < 1e-6);
    }

    #[test]
    fn test_sqlite_in_memory_connection() {
        let mut conn = Connection::open_in_memory().unwrap();
        assert!(init_db_connection(&mut conn).is_ok());

        let record = TelemetryRecord {
            id: "test-uuid-123".to_string(),
            timestamp: 1715800000,
            provider: "openai".to_string(),
            model: "gpt-4o".to_string(),
            status_code: 200,
            latency_ms: 250,
            ttft_ms: Some(100),
            prompt_tokens: 15,
            completion_tokens: 30,
            estimated_cost: 0.0005,
        };

        let mut batch = vec![record];
        assert!(flush_batch(&mut conn, &mut batch).is_ok());

        let mut stmt = conn.prepare("SELECT COUNT(*) FROM metrics").unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
        assert_eq!(count, 1);
    }
}
