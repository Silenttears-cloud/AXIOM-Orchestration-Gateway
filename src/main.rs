use std::sync::Arc;
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
use tracing::{info, error, Level};
use tracing_subscriber::FmtSubscriber;
use futures_util::StreamExt;

mod config;
mod proxy;

use crate::config::AppConfig;
use crate::proxy::{
    ChatCompletionRequest,
    ProviderProxy,
    openai::OpenAIProxy,
    anthropic::AnthropicProxy,
    gemini::GeminiProxy,
};

/// Shared Application State
struct AppState {
    config: AppConfig,
    client: reqwest::Client,
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

    // Initialize reqwest HTTP client with pooled connections for max performance
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(10)
        .build()
        .expect("Failed to build HTTP Client");

    let shared_state = Arc::new(AppState {
        config,
        client,
    });

    // Build the Axum router
    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completions))
        .with_state(shared_state);

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", host, port))
        .await
        .unwrap_or_else(|e| panic!("CRITICAL: Failed to bind TCP listener on port {}: {}", port, e));

    info!("✓ AXIOM Phase 2 Gateway Server Active & Listening!");

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
    // 1. Authenticate Request
    let auth_header = match headers.get("authorization") {
        Some(value) => match value.to_str() {
            Ok(str_val) => str_val,
            Err(_) => return (StatusCode::UNAUTHORIZED, "Invalid authorization header encoding").into_response(),
        },
        None => return (StatusCode::UNAUTHORIZED, "Authorization header is missing").into_response(),
    };

    let expected_auth = format!("Bearer {}", state.config.gateway.admin_api_key);
    if auth_header != expected_auth {
        error!("Unauthorized access attempt! Expected admin_api_key validation.");
        return (StatusCode::UNAUTHORIZED, "Unauthorized admin_api_key").into_response();
    }

    // 2. Select Upstream Provider dynamically based on requested model
    let model_lower = payload.model.to_lowercase();
    let provider_name = if model_lower.contains("gpt") {
        "openai"
    } else if model_lower.contains("claude") {
        "anthropic"
    } else if model_lower.contains("gemini") {
        "gemini"
    } else {
        // Fallback to the first available provider configured
        state.config.providers.keys().next().map(|s| s.as_str()).unwrap_or("openai")
    };

    let provider_config = match state.config.providers.get(provider_name) {
        Some(cfg) => cfg,
        None => return (
            StatusCode::BAD_REQUEST, 
            format!("The routed provider '{}' is not configured in config.yaml", provider_name)
        ).into_response(),
    };

    // Instantiate respective proxy driver dynamically
    let proxy_driver: Box<dyn ProviderProxy + Send + Sync> = match provider_name {
        "openai" => Box::new(OpenAIProxy),
        "anthropic" => Box::new(AnthropicProxy),
        "gemini" => Box::new(GeminiProxy),
        _ => Box::new(OpenAIProxy),
    };

    info!(
        "Routing LLM request: [Model: {}] -> [Provider: {}] -> [Streaming: {}]", 
        payload.model, 
        provider_name, 
        payload.stream.unwrap_or(false)
    );

    // 3. Dispatch to Stream Proxy or JSON Proxy
    if payload.stream.unwrap_or(false) {
        match proxy_driver.proxy_stream(&state.client, provider_config, &payload).await {
            Ok(stream) => {
                let sse_stream = stream.map(|res| {
                    match res {
                        Ok(line) => {
                            // Extract standard SSE data content if line starts with "data:"
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
                error!("Upstream stream request failed: {}", e);
                (StatusCode::BAD_GATEWAY, format!("Upstream stream failed: {}", e)).into_response()
            }
        }
    } else {
        match proxy_driver.proxy_json(&state.client, provider_config, &payload).await {
            Ok(response) => Json(response).into_response(),
            Err(e) => {
                error!("Upstream JSON request failed: {}", e);
                (StatusCode::BAD_GATEWAY, format!("Upstream request failed: {}", e)).into_response()
            }
        }
    }
}
