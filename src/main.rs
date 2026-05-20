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

    let shared_state = Arc::new(AppState {
        config,
        client,
        rate_limiter,
        circuit_breakers,
    });

    // Build the Axum router
    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completions))
        .with_state(shared_state);

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", host, port))
        .await
        .unwrap_or_else(|e| panic!("CRITICAL: Failed to bind TCP listener on port {}: {}", port, e));

    info!("✓ AXIOM Phase 3 Gateway Server Active & Listening!");

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

    // ── Step 3: Route to Provider ──
    let model_lower = payload.model.to_lowercase();
    let provider_name = if model_lower.contains("gpt") {
        "openai"
    } else if model_lower.contains("claude") {
        "anthropic"
    } else if model_lower.contains("gemini") {
        "gemini"
    } else {
        state.config.providers.keys().next().map(|s| s.as_str()).unwrap_or("openai")
    };

    let provider_config = match state.config.providers.get(provider_name) {
        Some(cfg) => cfg,
        None => return (
            StatusCode::BAD_REQUEST,
            format!("Provider '{}' is not configured", provider_name)
        ).into_response(),
    };

    // ── Step 4: Circuit Breaker Check ──
    if let Some(cb) = state.circuit_breakers.get(provider_name) {
        if let Err(msg) = cb.check() {
            warn!("Circuit breaker OPEN for provider '{}': {}", provider_name, msg);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Provider '{}' is temporarily unavailable. {}", provider_name, msg)
            ).into_response();
        }
    }

    // Instantiate proxy driver
    let proxy_driver: Box<dyn ProviderProxy + Send + Sync> = match provider_name {
        "openai" => Box::new(OpenAIProxy),
        "anthropic" => Box::new(AnthropicProxy),
        "gemini" => Box::new(GeminiProxy),
        _ => Box::new(OpenAIProxy),
    };

    info!(
        "Routing: [{}] -> [{}] [Stream: {}]",
        payload.model, provider_name, payload.stream.unwrap_or(false)
    );

    // ── Step 5: Dispatch Request ──
    if payload.stream.unwrap_or(false) {
        match proxy_driver.proxy_stream(&state.client, provider_config, &payload).await {
            Ok(stream) => {
                // Record success on circuit breaker
                if let Some(cb) = state.circuit_breakers.get(provider_name) {
                    cb.record_success();
                }

                let sse_stream = stream.map(|res| {
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

                Sse::new(sse_stream).into_response()
            }
            Err(e) => {
                // Record failure on circuit breaker
                if let Some(cb) = state.circuit_breakers.get(provider_name) {
                    cb.record_failure();
                    warn!(
                        "Circuit breaker [{}]: failure #{} / threshold {}",
                        provider_name, cb.failure_count(), 
                        provider_config.circuit_breaker.failure_threshold
                    );
                }
                error!("Upstream stream failed: {}", e);
                (StatusCode::BAD_GATEWAY, format!("Upstream stream failed: {}", e)).into_response()
            }
        }
    } else {
        match proxy_driver.proxy_json(&state.client, provider_config, &payload).await {
            Ok(response) => {
                // Record success on circuit breaker
                if let Some(cb) = state.circuit_breakers.get(provider_name) {
                    cb.record_success();
                }
                Json(response).into_response()
            }
            Err(e) => {
                // Record failure on circuit breaker
                if let Some(cb) = state.circuit_breakers.get(provider_name) {
                    cb.record_failure();
                    warn!(
                        "Circuit breaker [{}]: failure #{} / threshold {}",
                        provider_name, cb.failure_count(),
                        provider_config.circuit_breaker.failure_threshold
                    );
                }
                error!("Upstream JSON failed: {}", e);
                (StatusCode::BAD_GATEWAY, format!("Upstream request failed: {}", e)).into_response()
            }
        }
    }
}
