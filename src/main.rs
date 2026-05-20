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
    let config = AppConfig::load("config.yaml")
        .expect("CRITICAL: Failed to load or validate config.yaml");
    info!("Successfully parsed configuration file!");

    let host = config.gateway.host.clone();
    let port = config.gateway.port;

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

    // Initialize Asynchronous Telemetry Stack
    let (telemetry_tx, telemetry_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(telemetry::start_telemetry_worker(telemetry_rx, "telemetry.db".to_string()));

    let shared_state = Arc::new(AppState {
        config,
        client,
        rate_limiter,
        circuit_breakers,
        router: smart_router,
        telemetry_tx,
    });

    // Build the Axum router
    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completions))
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
