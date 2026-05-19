use async_trait::async_trait;
use crate::config::ProviderConfig;
use crate::proxy::{ProviderProxy, ChatCompletionRequest, ChatCompletionResponse, BoxedStream};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use futures_util::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

pub struct OpenAIProxy;

#[async_trait]
impl ProviderProxy for OpenAIProxy {
    async fn proxy_json(
        &self,
        client: &reqwest::Client,
        config: &ProviderConfig,
        request: &ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse, String> {
        let url = format!("{}/chat/completions", config.base_url);
        let api_key = config.api_keys.first()
            .ok_or_else(|| "No API keys configured for OpenAI".to_string())?;

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION, 
            HeaderValue::from_str(&format!("Bearer {}", api_key))
                .map_err(|e| format!("Invalid Auth Header: {}", e))?
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let response = client.post(&url)
            .headers(headers)
            .json(request)
            .send()
            .await
            .map_err(|e| format!("Failed to send request to OpenAI upstream: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(format!("OpenAI upstream error status ({}): {}", status, error_body));
        }

        let resp_payload: ChatCompletionResponse = response.json()
            .await
            .map_err(|e| format!("Failed to parse OpenAI JSON response payload: {}", e))?;

        Ok(resp_payload)
    }

    async fn proxy_stream(
        &self,
        client: &reqwest::Client,
        config: &ProviderConfig,
        request: &ChatCompletionRequest,
    ) -> Result<BoxedStream, String> {
        let url = format!("{}/chat/completions", config.base_url);
        let api_key = config.api_keys.first()
            .ok_or_else(|| "No API keys configured for OpenAI".to_string())?;

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION, 
            HeaderValue::from_str(&format!("Bearer {}", api_key))
                .map_err(|e| format!("Invalid Auth Header: {}", e))?
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        // Enforce upstream streaming flag to active
        let mut stream_req = request.clone();
        stream_req.stream = Some(true);

        let response = client.post(&url)
            .headers(headers)
            .json(&stream_req)
            .send()
            .await
            .map_err(|e| format!("Failed to initialize OpenAI stream connection: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(format!("OpenAI upstream stream failure ({}): {}", status, error_body));
        }

        let mut byte_stream = response.bytes_stream();
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        // Spawn a high-speed background task to pipe raw stream lines directly
        tokio::spawn(async move {
            let mut buffer = String::new();
            while let Some(chunk_res) = byte_stream.next().await {
                match chunk_res {
                    Ok(bytes) => {
                        let chunk_str = String::from_utf8_lossy(&bytes);
                        buffer.push_str(&chunk_str);
                        
                        // Parse and drain complete line breaks from current buffer
                        while let Some(pos) = buffer.find('\n') {
                            let line = buffer.drain(..pos + 1).collect::<String>();
                            let trimmed = line.trim().to_string();
                            if !trimmed.is_empty() {
                                if tx.send(Ok(trimmed)).await.is_err() {
                                    return; // Downstream client connection closed
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(format!("Upstream read error: {}", e))).await;
                        return;
                    }
                }
            }
            
            // Flush final characters if remaining in buffer
            let final_trimmed = buffer.trim().to_string();
            if !final_trimmed.is_empty() {
                let _ = tx.send(Ok(final_trimmed)).await;
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}
