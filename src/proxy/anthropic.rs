use async_trait::async_trait;
use crate::config::ProviderConfig;
use crate::proxy::{ProviderProxy, ChatCompletionRequest, ChatCompletionResponse, Choice, Message, Usage, BoxedStream, ChatCompletionChunk, ChunkChoice, ChunkDelta};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use futures_util::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use serde::{Deserialize, Serialize};

pub struct AnthropicProxy;

#[derive(Serialize)]
struct AnthropicRequest {
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    messages: Vec<AnthropicMessage>,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone)]
struct AnthropicMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct AnthropicResponse {
    id: String,
    model: String,
    content: Vec<AnthropicContentBlock>,
    stop_reason: Option<String>,
    usage: AnthropicUsage,
}

#[derive(Deserialize)]
struct AnthropicContentBlock {
    #[serde(rename = "type")]
    _block_type: String,
    text: String,
}

#[derive(Deserialize)]
struct AnthropicUsage {
    input_tokens: u32,
    output_tokens: u32,
}

// Stream Parser Structs for Anthropic SSE Events
#[derive(Deserialize)]
#[serde(tag = "type")]
enum AnthropicStreamEvent {
    #[serde(rename = "message_start")]
    MessageStart { message: AnthropicStreamMessageMetadata },
    #[serde(rename = "content_block_start")]
    ContentBlockStart,
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta { delta: AnthropicTextDelta },
    #[serde(rename = "content_block_stop")]
    ContentBlockStop,
    #[serde(rename = "message_delta")]
    MessageDelta,
    #[serde(rename = "message_stop")]
    MessageStop,
    #[serde(rename = "ping")]
    Ping,
}

#[derive(Deserialize)]
struct AnthropicStreamMessageMetadata {
    id: String,
    model: String,
}

#[derive(Deserialize)]
struct AnthropicTextDelta {
    text: String,
}

#[async_trait]
impl ProviderProxy for AnthropicProxy {
    async fn proxy_json(
        &self,
        client: &reqwest::Client,
        config: &ProviderConfig,
        request: &ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse, String> {
        let url = format!("{}/v1/messages", config.base_url);
        let api_key = config.api_keys.first()
            .ok_or_else(|| "No API keys configured for Anthropic".to_string())?;

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_str(api_key).map_err(|e| format!("Invalid API Key: {}", e))?);
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        // Extract system prompt from OpenAI messages, concatenate multiple system prompts if present
        let mut system_prompts = Vec::new();
        let mut anthropic_messages = Vec::new();

        for msg in &request.messages {
            if msg.role.to_lowercase() == "system" {
                system_prompts.push(msg.content.clone());
            } else {
                let role = match msg.role.to_lowercase().as_str() {
                    "assistant" => "assistant",
                    _ => "user", // Default unrecognized roles to user
                };
                anthropic_messages.push(AnthropicMessage {
                    role: role.to_string(),
                    content: msg.content.clone(),
                });
            }
        }

        let system_prompt = if system_prompts.is_empty() {
            None
        } else {
            Some(system_prompts.join("\n\n"))
        };

        // Determine dynamic model mapping (e.g. gpt-4o -> claude-3-5-sonnet)
        // For direct mapping, if user requested gpt-4o but configured to Anthropic provider, use sonnet
        let model = if request.model.contains("claude") {
            request.model.clone()
        } else {
            "claude-3-5-sonnet-20241022".to_string()
        };

        let anthropic_req = AnthropicRequest {
            model,
            system: system_prompt,
            messages: anthropic_messages,
            max_tokens: request.max_tokens.unwrap_or(4096),
            temperature: request.temperature,
            stream: Some(false),
        };

        let response = client.post(&url)
            .headers(headers)
            .json(&anthropic_req)
            .send()
            .await
            .map_err(|e| format!("Failed to send request to Anthropic: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(format!("Anthropic upstream error ({}): {}", status, error_body));
        }

        let anthropic_resp: AnthropicResponse = response.json()
            .await
            .map_err(|e| format!("Failed to parse Anthropic JSON response: {}", e))?;

        // Format back to OpenAI standard response
        let content_text = anthropic_resp.content.iter()
            .map(|c| c.text.clone())
            .collect::<Vec<String>>()
            .join("\n");

        let finish_reason = match anthropic_resp.stop_reason.as_deref() {
            Some("end_turn") => Some("stop".to_string()),
            Some("max_tokens") => Some("length".to_string()),
            Some(other) => Some(other.to_string()),
            None => None,
        };

        let choice = Choice {
            index: 0,
            message: Message {
                role: "assistant".to_string(),
                content: content_text,
            },
            finish_reason,
        };

        let total_tokens = anthropic_resp.usage.input_tokens + anthropic_resp.usage.output_tokens;
        let usage = Usage {
            prompt_tokens: anthropic_resp.usage.input_tokens,
            completion_tokens: anthropic_resp.usage.output_tokens,
            total_tokens,
        };

        Ok(ChatCompletionResponse {
            id: anthropic_resp.id,
            object: "chat.completion".to_string(),
            created: chrono::Utc::now().timestamp(),
            model: anthropic_resp.model,
            choices: vec![choice],
            usage: Some(usage),
        })
    }

    async fn proxy_stream(
        &self,
        client: &reqwest::Client,
        config: &ProviderConfig,
        request: &ChatCompletionRequest,
    ) -> Result<BoxedStream, String> {
        let url = format!("{}/v1/messages", config.base_url);
        let api_key = config.api_keys.first()
            .ok_or_else(|| "No API keys configured for Anthropic".to_string())?;

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_str(api_key).map_err(|e| format!("Invalid API Key: {}", e))?);
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let mut system_prompts = Vec::new();
        let mut anthropic_messages = Vec::new();

        for msg in &request.messages {
            if msg.role.to_lowercase() == "system" {
                system_prompts.push(msg.content.clone());
            } else {
                let role = match msg.role.to_lowercase().as_str() {
                    "assistant" => "assistant",
                    _ => "user",
                };
                anthropic_messages.push(AnthropicMessage {
                    role: role.to_string(),
                    content: msg.content.clone(),
                });
            }
        }

        let system_prompt = if system_prompts.is_empty() {
            None
        } else {
            Some(system_prompts.join("\n\n"))
        };

        let model = if request.model.contains("claude") {
            request.model.clone()
        } else {
            "claude-3-5-sonnet-20241022".to_string()
        };

        let anthropic_req = AnthropicRequest {
            model,
            system: system_prompt,
            messages: anthropic_messages,
            max_tokens: request.max_tokens.unwrap_or(4096),
            temperature: request.temperature,
            stream: Some(true),
        };

        let response = client.post(&url)
            .headers(headers)
            .json(&anthropic_req)
            .send()
            .await
            .map_err(|e| format!("Failed to initiate Anthropic stream: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(format!("Anthropic upstream stream failure ({}): {}", status, error_body));
        }

        let mut byte_stream = response.bytes_stream();
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            let mut buffer = String::new();
            let mut current_msg_id = "msg_anthropic_stream".to_string();
            let mut current_model = "claude-3-5-sonnet".to_string();

            while let Some(chunk_res) = byte_stream.next().await {
                match chunk_res {
                    Ok(bytes) => {
                        let chunk_str = String::from_utf8_lossy(&bytes);
                        buffer.push_str(&chunk_str);

                        // Process full SSE lines from Anthropic (events are split across multiple lines)
                        while let Some(pos) = buffer.find("\n\n") {
                            let event_block = buffer.drain(..pos + 2).collect::<String>();
                            
                            // Anthropic SSE outputs:
                            // event: <event_type>
                            // data: <json_data>
                            let mut event_type = "";
                            let mut data_json = "";

                            for line in event_block.lines() {
                                if line.starts_with("event:") {
                                    event_type = line["event:".len()..].trim();
                                } else if line.starts_with("data:") {
                                    data_json = line["data:".len()..].trim();
                                }
                            }

                            if event_type.is_empty() || data_json.is_empty() {
                                continue;
                            }

                            if let Ok(event) = serde_json::from_str::<AnthropicStreamEvent>(data_json) {
                                match event {
                                    AnthropicStreamEvent::MessageStart { message } => {
                                        current_msg_id = message.id;
                                        current_model = message.model;
                                    }
                                    AnthropicStreamEvent::ContentBlockDelta { delta } => {
                                        // Package the Claude text delta into a standard OpenAI ChatCompletionChunk
                                        let openai_chunk = ChatCompletionChunk {
                                            id: current_msg_id.clone(),
                                            object: "chat.completion.chunk".to_string(),
                                            created: chrono::Utc::now().timestamp(),
                                            model: current_model.clone(),
                                            choices: vec![ChunkChoice {
                                                index: 0,
                                                delta: ChunkDelta {
                                                    content: Some(delta.text),
                                                },
                                                finish_reason: None,
                                            }],
                                        };

                                        if let Ok(chunk_json) = serde_json::to_string(&openai_chunk) {
                                            let sse_line = format!("data: {}", chunk_json);
                                            if tx.send(Ok(sse_line)).await.is_err() {
                                                return; // Downstream disconnected
                                            }
                                        }
                                    }
                                    AnthropicStreamEvent::MessageStop => {
                                        let _ = tx.send(Ok("data: [DONE]".to_string())).await;
                                        return;
                                    }
                                    _ => {} // Skip starts, stops, message_deltas, pings
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(format!("Anthropic stream read error: {}", e))).await;
                        return;
                    }
                }
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}
