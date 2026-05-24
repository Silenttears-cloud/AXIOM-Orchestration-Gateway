use std::sync::Arc;
use std::collections::HashMap;
use std::convert::Infallible;
use axum::{
    routing::post,
    Router,
    Json,
    extract::State,
    http::{StatusCode, HeaderMap},
    response::sse::{Event, Sse},
    response::IntoResponse,
};
use tracing::{info, warn, error, Level};
use tracing_subscriber::FmtSubscriber;
use futures_util::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tower_http::cors::CorsLayer;

mod config;
mod proxy;
mod rate_limiter;
mod circuit_breaker;
mod router;
mod telemetry;

use crate::config::AppConfig;
use crate::proxy::{
    ChatCompletionRequest,
    ProviderProxy,
    openai::OpenAIProxy,
    anthropic::AnthropicProxy,
    gemini::GeminiProxy,
};
use crate::rate_limiter::RateLimiter;
use crate::circuit_breaker::CircuitBreaker;

/// Shared Application State
struct AppState {
    config: AppConfig,
    client: reqwest::Client,
    rate_limiter: RateLimiter,
    circuit_breakers: HashMap<String, CircuitBreaker>,
    router: router::SmartRouter,
    telemetry_tx: tokio::sync::mpsc::UnboundedSender<telemetry::TelemetryRecord>,
    telemetry_broadcast: tokio::sync::broadcast::Sender<telemetry::TelemetryRecord>,
}

#[tokio::main]
async fn main() {
    // Initialize the colorized structured tracing subscriber
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to initialize tracing logging subscriber");

    info!("============================================================");
    info!("   AXIOM: AI Agent Orchestration Gateway — Bootstrapping   ");
    info!("============================================================");

    // Load configuration
    let mut config = AppConfig::load("config.yaml")
        .expect("CRITICAL: Failed to load or validate config.yaml");
    info!("Successfully parsed configuration file!");

    // Override API keys from Environment Variables if present (for secure production deployment)
    if let Ok(key) = std::env::var("OPENAI_API_KEY") {
        if let Some(provider) = config.providers.get_mut("openai") {
            provider.api_keys = vec![key];
            info!("OpenAI API key overridden from environment variable.");
        }
    }
    if let Ok(key) = std::env::var("GEMINI_API_KEY") {
        if let Some(provider) = config.providers.get_mut("gemini") {
            provider.api_keys = vec![key];
            info!("Gemini API key overridden from environment variable.");
        }
    }
    if let Ok(key) = std::env::var("ANTHROPIC_API_KEY") {
        if let Some(provider) = config.providers.get_mut("anthropic") {
            provider.api_keys = vec![key];
            info!("Anthropic API key overridden from environment variable.");
        }
    }

    // Override host & port for container/cloud environments (e.g. Render dynamic ports)
    let host = std::env::var("HOST")
        .unwrap_or_else(|_| "0.0.0.0".to_string());
    
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(config.gateway.port);


    info!("Gateway Server Bind: http://{}:{}", host, port);
    info!("Rate Limiting Status: [Enabled: {}]", config.rate_limiting.enabled);
    
    let active_providers: Vec<&str> = config.providers.keys().map(|s| s.as_str()).collect();
    info!("Registered AI Providers: {:?}", active_providers);
    info!("Active Routing Policy: '{}'", config.routing.default_policy);

    // Initialize Rate Limiter from config
    let rate_limiter = RateLimiter::new(
        config.rate_limiting.burst_capacity,
        config.rate_limiting.tokens_per_minute,
    );
    info!(
        "Rate Limiter Initialized: [Burst: {}, Refill: {}/min]",
        config.rate_limiting.burst_capacity,
        config.rate_limiting.tokens_per_minute
    );

    // Initialize per-provider Circuit Breakers from config
    let mut circuit_breakers = HashMap::new();
    for (name, provider_cfg) in &config.providers {
        let cb = CircuitBreaker::new(
            provider_cfg.circuit_breaker.failure_threshold,
            provider_cfg.circuit_breaker.cooldown_seconds,
        );
        info!(
            "Circuit Breaker [{}]: [Threshold: {}, Cooldown: {}s]",
            name,
            provider_cfg.circuit_breaker.failure_threshold,
            provider_cfg.circuit_breaker.cooldown_seconds
        );
        circuit_breakers.insert(name.clone(), cb);
    }

    // Initialize reqwest HTTP client with pooled connections
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(10)
        .build()
        .expect("Failed to build HTTP Client");

    // Initialize Latency Tracker & Smart Router
    let latency_tracker = router::LatencyTracker::new();
    let smart_router = router::SmartRouter::new(latency_tracker);

    // Initialize Asynchronous Telemetry Stack & Real-Time Broadcast
    let (telemetry_tx, telemetry_rx) = tokio::sync::mpsc::unbounded_channel();
    let (telemetry_broadcast, _) = tokio::sync::broadcast::channel(100);
    
    let db_path = std::env::var("AXIOM_DB_PATH").unwrap_or_else(|_| "telemetry.db".to_string());
    tokio::spawn(telemetry::start_telemetry_worker(
        telemetry_rx, 
        db_path, 
        telemetry_broadcast.clone()
    ));


    let shared_state = Arc::new(AppState {
        config,
        client,
        rate_limiter,
        circuit_breakers,
        router: smart_router,
        telemetry_tx,
        telemetry_broadcast,
    });

    // CORS configurations for dashboard frontend
    let cors = CorsLayer::permissive();

    // Build the Axum router
    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completions))
        .route("/v1/telemetry/stream", axum::routing::get(handle_telemetry_stream))
        .route("/v1/telemetry/history", axum::routing::get(handle_get_telemetry_history))
        .route("/v1/circuit-breakers", axum::routing::get(handle_get_circuit_breakers))
        .route("/v1/circuit-breakers/:provider/reset", post(handle_reset_circuit_breaker))
        .route("/dashboard", axum::routing::get(serve_dashboard))
        .route("/dashboard/*path", axum::routing::get(serve_dashboard))
        .layer(cors)
        .with_state(shared_state);

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", host, port))
        .await
        .unwrap_or_else(|e| panic!("CRITICAL: Failed to bind TCP listener on port {}: {}", port, e));

    info!("✓ AXIOM Phase 5 Gateway Server Active & Listening!");

    axum::serve(listener, app)
        .await
        .expect("CRITICAL: Gateway server encountered a runtime crash");
}

/// Handler for chat completions requests
async fn handle_chat_completions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<ChatCompletionRequest>,
) -> impl IntoResponse {
    // ── Step 1: Authenticate Request ──
    let auth_header = match headers.get("authorization") {
        Some(value) => match value.to_str() {
            Ok(str_val) => str_val,
            Err(_) => return (StatusCode::UNAUTHORIZED, "Invalid authorization header encoding").into_response(),
        },
        None => return (StatusCode::UNAUTHORIZED, "Authorization header is missing").into_response(),
    };

    let expected_auth = format!("Bearer {}", state.config.gateway.admin_api_key);
    if auth_header != expected_auth {
        error!("Unauthorized access attempt!");
        return (StatusCode::UNAUTHORIZED, "Unauthorized admin_api_key").into_response();
    }

    // ── Step 2: Rate Limit Check ──
    if state.config.rate_limiting.enabled && !state.rate_limiter.try_acquire() {
        warn!(
            "Rate limit exceeded! Available tokens: {:.1}",
            state.rate_limiter.available_tokens()
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "Rate limit exceeded. Please slow down."
        ).into_response();
    }

    // Estimate prompt size for cost estimation
    let prompt_chars: usize = payload.messages.iter().map(|m| m.content.len()).sum();

    // ── Step 3: Resolve Primary Route and Failover Chain ──
    let default_policy = &state.config.routing.default_policy;
    let (primary_model, primary_provider) = state.router.resolve_route(&payload, default_policy);

    // Build the fallback chain
    let mut attempts = vec![(primary_model.clone(), primary_provider.clone())];

    // If there is a fallback chain configured for the originally requested model, append those options
    if let Some(chain) = state.config.routing.fallback_chains.get(&payload.model) {
        for fallback_model in chain {
            let fallback_provider = router::SmartRouter::get_provider_for_model(fallback_model);
            // Avoid duplicate attempts
            if !attempts.iter().any(|(m, p)| m == fallback_model && p == &fallback_provider) {
                attempts.push((fallback_model.clone(), fallback_provider));
            }
        }
    }

    info!("Routing attempts list: {:?}", attempts);

    let mut errors = Vec::new();
    let start_time = std::time::Instant::now();

    // ── Step 4: Dispatch Request Loop with Automatic Failover ──
    for (model, provider) in attempts {
        let provider_config = match state.config.providers.get(&provider) {
            Some(cfg) => cfg,
            None => {
                let err_msg = format!("Provider '{}' is not configured for model '{}'", provider, model);
                warn!("{}", err_msg);
                errors.push(err_msg);
                continue;
            }
        };

        // Check Circuit Breaker
        if let Some(cb) = state.circuit_breakers.get(&provider) {
            if let Err(msg) = cb.check() {
                let err_msg = format!("Circuit breaker OPEN for provider '{}' (model '{}'): {}", provider, model, msg);
                warn!("{}", err_msg);
                errors.push(err_msg);
                continue; // Tripped! Failover to next candidate
            }
        }

        // Instantiate proxy driver
        let proxy_driver: Box<dyn ProviderProxy + Send + Sync> = match provider.as_str() {
            "openai" => Box::new(OpenAIProxy),
            "anthropic" => Box::new(AnthropicProxy),
            "gemini" => Box::new(GeminiProxy),
            _ => Box::new(OpenAIProxy),
        };

        let mut attempt_payload = payload.clone();
        attempt_payload.model = model.clone();

        info!(
            "Attempting route: [{}] -> [{}] [Stream: {}]",
            model, provider, payload.stream.unwrap_or(false)
        );

        let attempt_start = std::time::Instant::now();

        if payload.stream.unwrap_or(false) {
            match proxy_driver.proxy_stream(&state.client, provider_config, &attempt_payload).await {
                Ok(stream) => {
                    // Record success & latency (TTFT)
                    let elapsed = attempt_start.elapsed().as_millis() as f64;
                    state.router.latency_tracker.record_latency(&provider, elapsed);
                    if let Some(cb) = state.circuit_breakers.get(&provider) {
                        cb.record_success();
                    }

                    // Prepare template telemetry record
                    let record_template = telemetry::TelemetryRecord {
                        id: uuid::Uuid::new_v4().to_string(),
                        timestamp: chrono::Utc::now().timestamp(),
                        provider: provider.clone(),
                        model: model.clone(),
                        status_code: 200,
                        latency_ms: 0,
                        ttft_ms: Some(elapsed as u64),
                        prompt_tokens: (prompt_chars as f64 / 4.0).ceil() as u32,
                        completion_tokens: 0,
                        estimated_cost: 0.0,
                    };

                    // Wrap stream with dynamic telemetry metrics collector
                    let telemetry_stream = telemetry::TelemetryStream::new(stream, record_template, state.telemetry_tx.clone());

                    let sse_stream = telemetry_stream.map(move |res| {
                        match res {
                            Ok(line) => {
                                if line.starts_with("data:") {
                                    let content = line["data:".len()..].trim().to_string();
                                    Ok::<_, Infallible>(Event::default().data(content))
                                } else {
                                    Ok::<_, Infallible>(Event::default().data(line))
                                }
                            }
                            Err(e) => {
                                error!("Stream chunk error: {}", e);
                                Ok::<_, Infallible>(Event::default().event("error").data(e))
                            }
                        }
                    });

                    return Sse::new(sse_stream).into_response();
                }
                Err(e) => {
                    // Record failure on circuit breaker
                    if let Some(cb) = state.circuit_breakers.get(&provider) {
                        cb.record_failure();
                        warn!(
                            "Circuit breaker [{}]: failure #{} / threshold {}",
                            provider, cb.failure_count(), 
                            provider_config.circuit_breaker.failure_threshold
                        );
                    }
                    let err_msg = format!("Upstream stream failed for model '{}' on provider '{}': {}", model, provider, e);
                    error!("{}", err_msg);
                    errors.push(err_msg);
                }
            }
        } else {
            match proxy_driver.proxy_json(&state.client, provider_config, &attempt_payload).await {
                Ok(response) => {
                    // Record success & latency
                    let elapsed = attempt_start.elapsed().as_millis() as f64;
                    state.router.latency_tracker.record_latency(&provider, elapsed);
                    if let Some(cb) = state.circuit_breakers.get(&provider) {
                        cb.record_success();
                    }

                    // Record telemetry metrics
                    let completion_chars: usize = response.choices.iter().map(|c| c.message.content.len()).sum();
                    let (prompt_tokens, completion_tokens, estimated_cost) = 
                        telemetry::calculate_estimated_cost(&model, prompt_chars, completion_chars);

                    let record = telemetry::TelemetryRecord {
                        id: uuid::Uuid::new_v4().to_string(),
                        timestamp: chrono::Utc::now().timestamp(),
                        provider: provider.clone(),
                        model: model.clone(),
                        status_code: StatusCode::OK.as_u16(),
                        latency_ms: start_time.elapsed().as_millis() as u64,
                        ttft_ms: None,
                        prompt_tokens,
                        completion_tokens,
                        estimated_cost,
                    };
                    let _ = state.telemetry_tx.send(record);

                    return Json(response).into_response();
                }
                Err(e) => {
                    // Record failure on circuit breaker
                    if let Some(cb) = state.circuit_breakers.get(&provider) {
                        cb.record_failure();
                        warn!(
                            "Circuit breaker [{}]: failure #{} / threshold {}",
                            provider, cb.failure_count(),
                            provider_config.circuit_breaker.failure_threshold
                        );
                    }
                    let err_msg = format!("Upstream JSON failed for model '{}' on provider '{}': {}", model, provider, e);
                    error!("{}", err_msg);
                    errors.push(err_msg);
                }
            }
        }
    }

    // All routes failed — log failure telemetry record in DB
    let record = telemetry::TelemetryRecord {
        id: uuid::Uuid::new_v4().to_string(),
        timestamp: chrono::Utc::now().timestamp(),
        provider: primary_provider.clone(),
        model: primary_model.clone(),
        status_code: StatusCode::BAD_GATEWAY.as_u16(),
        latency_ms: start_time.elapsed().as_millis() as u64,
        ttft_ms: None,
        prompt_tokens: (prompt_chars as f64 / 4.0).ceil() as u32,
        completion_tokens: 0,
        estimated_cost: 0.0,
    };
    let _ = state.telemetry_tx.send(record);

    error!("All routing and fallback attempts failed! Errors: {:?}", errors);
    (
        StatusCode::BAD_GATEWAY,
        format!("All routing attempts failed. Details:\n- {}", errors.join("\n- "))
    ).into_response()
}

/// SSE Real-time Telemetry Stream Endpoint
async fn handle_telemetry_stream(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let rx = state.telemetry_broadcast.subscribe();
    let stream = BroadcastStream::new(rx).map(|res| {
        match res {
            Ok(record) => {
                if let Ok(json_str) = serde_json::to_string(&record) {
                    Ok::<_, Infallible>(Event::default().data(json_str))
                } else {
                    Ok::<_, Infallible>(Event::default().data("{}"))
                }
            }
            Err(e) => {
                warn!("Telemetry stream lagging or skipped: {}", e);
                Ok::<_, Infallible>(Event::default().event("error").data(e.to_string()))
            }
        }
    });

    Sse::new(stream).into_response()
}

/// GET /v1/telemetry/history
async fn handle_get_telemetry_history(
    State(_state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let db_path = std::env::var("AXIOM_DB_PATH").unwrap_or_else(|_| "telemetry.db".to_string());

    let records_res = tokio::task::spawn_blocking(move || {
        telemetry::get_recent_records(&db_path, 100)
    }).await;

    match records_res {
        Ok(Ok(records)) => (StatusCode::OK, Json(records)).into_response(),
        Ok(Err(e)) => {
            error!("Database telemetry history query failed: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Database query error").into_response()
        }
        Err(e) => {
            error!("Blocking task join failed for telemetry history: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Thread join error").into_response()
        }
    }
}

/// GET /v1/circuit-breakers
async fn handle_get_circuit_breakers(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let mut cb_states = HashMap::new();
    for (name, cb) in &state.circuit_breakers {
        cb_states.insert(name.clone(), serde_json::json!({
            "state": cb.get_state(),
            "failure_count": cb.failure_count(),
        }));
    }
    Json(cb_states)
}

/// POST /v1/circuit-breakers/:provider/reset
async fn handle_reset_circuit_breaker(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(provider): axum::extract::Path<String>,
) -> impl IntoResponse {
    if let Some(cb) = state.circuit_breakers.get(&provider) {
        cb.record_success(); // Resets the circuit breaker and opens flow
        info!("Manual Override: Reset circuit breaker for provider '{}' to CLOSED", provider);
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "success",
                "message": format!("Circuit breaker for '{}' reset to CLOSED successfully", provider)
            }))
        ).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "status": "error",
                "message": format!("Provider '{}' not found in active configurations", provider)
            }))
        ).into_response()
    }
}

// Embedded Static Assets
#[derive(rust_embed::RustEmbed)]
#[folder = "dashboard-ui/dist/"]
struct Assets;

/// Serve Embedded Dashboard SPA with HTML5 routing fallback
async fn serve_dashboard(uri: axum::http::Uri) -> impl IntoResponse {
    let path_str = uri.path();
    let mut path = path_str.trim_start_matches('/');
    
    // Trim dashboard prefix if it's there
    if path.starts_with("dashboard") {
        path = &path["dashboard".len()..];
    }
    let mut path = path.trim_start_matches('/');

    if path.is_empty() || path == "/" {
        path = "index.html";
    }

    match Assets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            axum::response::Response::builder()
                .header(axum::http::header::CONTENT_TYPE, mime.as_ref())
                .body(axum::body::Body::from(content.data))
                .unwrap()
        }
        None => {
            // SPA fallback routing
            match Assets::get("index.html") {
                Some(content) => axum::response::Response::builder()
                    .header(axum::http::header::CONTENT_TYPE, "text/html")
                    .body(axum::body::Body::from(content.data))
                    .unwrap(),
                None => (
                    StatusCode::NOT_FOUND,
                    "Embedded Dashboard Asset index.html was not found"
                ).into_response(),
            }
        }
    }
}
