use async_trait::async_trait;
use crate::config::ProviderConfig;
use crate::proxy::{ProviderProxy, ChatCompletionRequest, ChatCompletionResponse, Choice, Message, Usage, BoxedStream, ChatCompletionChunk, ChunkChoice, ChunkDelta};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use futures_util::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use serde::{Deserialize, Serialize};

pub struct GeminiProxy;

#[derive(Serialize)]
struct GeminiRequest {
    contents: Vec<GeminiContent>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "systemInstruction")]
    system_instruction: Option<GeminiSystemInstruction>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "generationConfig")]
    generation_config: Option<GeminiGenerationConfig>,
}

#[derive(Serialize, Deserialize, Clone)]
struct GeminiContent {
    role: String,
    parts: Vec<GeminiPart>,
}

#[derive(Serialize, Deserialize, Clone)]
struct GeminiPart {
    text: String,
}

#[derive(Serialize)]
struct GeminiSystemInstruction {
    parts: Vec<GeminiPart>,
}

#[derive(Serialize)]
struct GeminiGenerationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "maxOutputTokens")]
    max_output_tokens: Option<u32>,
}

#[derive(Deserialize)]
struct GeminiResponse {
    candidates: Option<Vec<GeminiCandidate>>,
    #[serde(rename = "usageMetadata")]
    usage_metadata: Option<GeminiUsageMetadata>,
}

#[derive(Deserialize)]
struct GeminiCandidate {
    content: Option<GeminiContent>,
    #[serde(rename = "finishReason")]
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct GeminiUsageMetadata {
    #[serde(rename = "promptTokenCount")]
    prompt_token_count: u32,
    #[serde(rename = "candidatesTokenCount")]
    candidates_token_count: u32,
    #[serde(rename = "totalTokenCount")]
    total_token_count: u32,
}

#[async_trait]
impl ProviderProxy for GeminiProxy {
    async fn proxy_json(
        &self,
        client: &reqwest::Client,
        config: &ProviderConfig,
        request: &ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse, String> {
        let api_key = config.api_keys.first()
            .ok_or_else(|| "No API keys configured for Gemini".to_string())?;

        // Determine dynamic model mapping (e.g. gpt-4o -> gemini-1.5-pro)
        let model = if request.model.contains("gemini") {
            request.model.clone()
        } else {
            "gemini-1.5-pro".to_string()
        };

        // Construct standard Gemini URL
        let url = format!(
            "{}/models/{}:generateContent?key={}", 
            config.base_url, 
            model, 
            api_key
        );

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let mut system_prompts = Vec::new();
        let mut contents = Vec::new();

        for msg in &request.messages {
            if msg.role.to_lowercase() == "system" {
                system_prompts.push(msg.content.clone());
            } else {
                let role = match msg.role.to_lowercase().as_str() {
                    "assistant" => "model",
                    _ => "user",
                };
                contents.push(GeminiContent {
                    role: role.to_string(),
                    parts: vec![GeminiPart { text: msg.content.clone() }],
                });
            }
        }

        let system_instruction = if system_prompts.is_empty() {
            None
        } else {
            Some(GeminiSystemInstruction {
                parts: vec![GeminiPart { text: system_prompts.join("\n\n") }],
            })
        };

        let gemini_req = GeminiRequest {
            contents,
            system_instruction,
            generation_config: Some(GeminiGenerationConfig {
                temperature: request.temperature,
                max_output_tokens: request.max_tokens,
            }),
        };

        let response = client.post(&url)
            .headers(headers)
            .json(&gemini_req)
            .send()
            .await
            .map_err(|e| format!("Failed to send request to Gemini: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(format!("Gemini upstream error ({}): {}", status, error_body));
        }

        let gemini_resp: GeminiResponse = response.json()
            .await
            .map_err(|e| format!("Failed to parse Gemini JSON response: {}", e))?;

        let candidate = gemini_resp.candidates
            .as_ref()
            .and_then(|c| c.first())
            .ok_or_else(|| "No candidates returned by Gemini".to_string())?;

        let content_text = candidate.content
            .as_ref()
            .and_then(|c| c.parts.first())
            .map(|p| p.text.clone())
            .unwrap_or_default();

        let finish_reason = match candidate.finish_reason.as_deref() {
            Some("STOP") => Some("stop".to_string()),
            Some("MAX_TOKENS") => Some("length".to_string()),
            Some(other) => Some(other.to_lowercase()),
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

        let usage = gemini_resp.usage_metadata.map(|u| Usage {
            prompt_tokens: u.prompt_token_count,
            completion_tokens: u.candidates_token_count,
            total_tokens: u.total_token_count,
        }).unwrap_or(Usage {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
        });

        Ok(ChatCompletionResponse {
            id: format!("gemini-msg-{}", uuid::Uuid::new_v4()),
            object: "chat.completion".to_string(),
            created: chrono::Utc::now().timestamp(),
            model,
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
        let api_key = config.api_keys.first()
            .ok_or_else(|| "No API keys configured for Gemini".to_string())?;

        let model = if request.model.contains("gemini") {
            request.model.clone()
        } else {
            "gemini-1.5-pro".to_string()
        };

        // Construct standard Gemini Streaming URL
        let url = format!(
            "{}/models/{}:streamGenerateContent?key={}", 
            config.base_url, 
            model, 
            api_key
        );

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let mut system_prompts = Vec::new();
        let mut contents = Vec::new();

        for msg in &request.messages {
            if msg.role.to_lowercase() == "system" {
                system_prompts.push(msg.content.clone());
            } else {
                let role = match msg.role.to_lowercase().as_str() {
                    "assistant" => "model",
                    _ => "user",
                };
                contents.push(GeminiContent {
                    role: role.to_string(),
                    parts: vec![GeminiPart { text: msg.content.clone() }],
                });
            }
        }

        let system_instruction = if system_prompts.is_empty() {
            None
        } else {
            Some(GeminiSystemInstruction {
                parts: vec![GeminiPart { text: system_prompts.join("\n\n") }],
            })
        };

        let gemini_req = GeminiRequest {
            contents,
            system_instruction,
            generation_config: Some(GeminiGenerationConfig {
                temperature: request.temperature,
                max_output_tokens: request.max_tokens,
            }),
        };

        let response = client.post(&url)
            .headers(headers)
            .json(&gemini_req)
            .send()
            .await
            .map_err(|e| format!("Failed to initiate Gemini stream: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(format!("Gemini upstream stream failure ({}): {}", status, error_body));
        }

        let mut byte_stream = response.bytes_stream();
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            let mut buffer = String::new();
            let current_msg_id = format!("gemini-stream-{}", uuid::Uuid::new_v4());
            let current_model = model.clone();

            while let Some(chunk_res) = byte_stream.next().await {
                match chunk_res {
                    Ok(bytes) => {
                        let chunk_str = String::from_utf8_lossy(&bytes);
                        buffer.push_str(&chunk_str);

                        // Clean and parse the streaming JSON array block-by-block.
                        // Gemini's stream format is:
                        // [\n  { candidate1 },\n  { candidate2 }\n]
                        while let Some(pos) = buffer.find('\n') {
                            let line = buffer.drain(..pos + 1).collect::<String>();
                            let mut trimmed = line.trim().to_string();

                            // Strip dynamic array prefix "[" or array commas "," or array suffix "]"
                            if trimmed.starts_with('[') {
                                trimmed = trimmed[1..].trim().to_string();
                            }
                            if trimmed.starts_with(',') {
                                trimmed = trimmed[1..].trim().to_string();
                            }
                            if trimmed.ends_with(']') {
                                trimmed = trimmed[..trimmed.len() - 1].trim().to_string();
                            }

                            if trimmed.is_empty() {
                                continue;
                            }

                            // If we have a valid JSON block, parse it
                            if let Ok(block) = serde_json::from_str::<GeminiResponse>(&trimmed) {
                                if let Some(candidates) = block.candidates {
                                    if let Some(candidate) = candidates.first() {
                                        if let Some(content) = &candidate.content {
                                            if let Some(part) = content.parts.first() {
                                                // Wrap text chunk in OpenAI compatible structure
                                                let openai_chunk = ChatCompletionChunk {
                                                    id: current_msg_id.clone(),
                                                    object: "chat.completion.chunk".to_string(),
                                                    created: chrono::Utc::now().timestamp(),
                                                    model: current_model.clone(),
                                                    choices: vec![ChunkChoice {
                                                        index: 0,
                                                        delta: ChunkDelta {
                                                            content: Some(part.text.clone()),
                                                        },
                                                        finish_reason: None,
                                                    }],
                                                };

                                                if let Ok(chunk_json) = serde_json::to_string(&openai_chunk) {
                                                    let sse_line = format!("data: {}", chunk_json);
                                                    if tx.send(Ok(sse_line)).await.is_err() {
                                                        return; // Client closed connection
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(format!("Gemini stream read error: {}", e))).await;
                        return;
                    }
                }
            }
            
            // Stream complete
            let _ = tx.send(Ok("data: [DONE]".to_string())).await;
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}
