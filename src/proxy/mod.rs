pub mod openai;
pub mod anthropic;
pub mod gemini;

use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use futures_util::Stream;
use std::pin::Pin;
use crate::config::ProviderConfig;

/// Uniform Chat Request Payload matching the standard OpenAI Completions API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<Message>,
    #[serde(default)]
    pub stream: Option<bool>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u32>,
}

/// A standard chat message payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}

/// Standard Chat Response Payload returned to clients
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub model: String,
    pub choices: Vec<Choice>,
    pub usage: Option<Usage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Choice {
    pub index: usize,
    pub message: Message,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

/// Uniform Chunk Payload sent down the Server-Sent Events (SSE) pipe during streaming
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionChunk {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub model: String,
    pub choices: Vec<ChunkChoice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkChoice {
    pub index: usize,
    pub delta: ChunkDelta,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkDelta {
    pub content: Option<String>,
}

/// Pin-boxed Stream Type returned by asynchronous stream operations
pub type BoxedStream = Pin<Box<dyn Stream<Item = Result<String, String>> + Send>>;

/// The unified asynchronous proxy trait that all AI provider integrations must implement
#[async_trait]
pub trait ProviderProxy {
    /// Proxy a standard blocking JSON completions request to the upstream provider
    async fn proxy_json(
        &self,
        client: &reqwest::Client,
        config: &ProviderConfig,
        request: &ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse, String>;

    /// Proxy a streaming completions request to the upstream provider, returning a raw token stream
    async fn proxy_stream(
        &self,
        client: &reqwest::Client,
        config: &ProviderConfig,
        request: &ChatCompletionRequest,
    ) -> Result<BoxedStream, String>;
}
