use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::proxy::ChatCompletionRequest;
use tracing::info;

/// Dynamic Moving Average TTFT (Time To First Token) Tracker
#[derive(Debug, Clone)]
pub struct LatencyTracker {
    // Maps provider name (e.g., "openai") to moving average TTFT in milliseconds
    averages: Arc<RwLock<HashMap<String, f64>>>,
    alpha: f64,
}

impl LatencyTracker {
    pub fn new() -> Self {
        let mut initial_map = HashMap::new();
        // Seed with reasonable baseline expectations
        initial_map.insert("openai".to_string(), 150.0);
        initial_map.insert("anthropic".to_string(), 180.0);
        initial_map.insert("gemini".to_string(), 120.0);

        Self {
            averages: Arc::new(RwLock::new(initial_map)),
            alpha: 0.2, // EMA smoothing factor
        }
    }

    /// Records a new TTFT or latency sample and updates the moving average
    pub fn record_latency(&self, provider: &str, sample_ms: f64) {
        let mut avgs = self.averages.write().unwrap();
        let entry = avgs.entry(provider.to_string()).or_insert(sample_ms);
        *entry = self.alpha * sample_ms + (1.0 - self.alpha) * *entry;
        info!(
            "LatencyTracker: Updated [{}] average latency to {:.2}ms (latest sample: {:.1}ms)",
            provider, *entry, sample_ms
        );
    }

    /// Retrieves the current average latency for a provider
    pub fn get_latency(&self, provider: &str) -> f64 {
        let avgs = self.averages.read().unwrap();
        *avgs.get(provider).unwrap_or(&150.0)
    }
}

/// Dynamic Smart Router Manager
pub struct SmartRouter {
    pub latency_tracker: LatencyTracker,
    round_robin_counter: AtomicUsize,
}

impl SmartRouter {
    pub fn new(latency_tracker: LatencyTracker) -> Self {
        Self {
            latency_tracker,
            round_robin_counter: AtomicUsize::new(0),
        }
    }

    /// Helper to map a model name to its corresponding provider name
    pub fn get_provider_for_model(model: &str) -> String {
        let model_lower = model.to_lowercase();
        if model_lower.contains("gpt") {
            "openai".to_string()
        } else if model_lower.contains("claude") {
            "anthropic".to_string()
        } else if model_lower.contains("gemini") {
            "gemini".to_string()
        } else {
            "openai".to_string() // Default fallback provider
        }
    }

    /// Helper to map a provider name to its default model name
    pub fn get_default_model_for_provider(provider: &str) -> String {
        match provider {
            "openai" => "gpt-4o".to_string(),
            "anthropic" => "claude-3-5-sonnet".to_string(),
            "gemini" => "gemini-1.5-flash".to_string(),
            _ => "gpt-4o".to_string(),
        }
    }

    /// Resolves the primary route (model, provider) based on configured policy
    pub fn resolve_route(&self, payload: &ChatCompletionRequest, policy: &str) -> (String, String) {
        let requested_model = &payload.model;
        let requested_provider = Self::get_provider_for_model(requested_model);

        match policy {
            "direct" => {
                (requested_model.clone(), requested_provider)
            }
            "cost_aware" => {
                // Calculate total prompt characters
                let total_chars: usize = payload.messages.iter().map(|m| m.content.len()).sum();
                
                let is_expensive = requested_model.contains("gpt-4o") || requested_model.contains("claude-3-5-sonnet");
                
                if is_expensive && total_chars < 500 {
                    let remapped_model = if requested_model.contains("gpt-4o") {
                        "gpt-4o-mini".to_string()
                    } else {
                        "gemini-1.5-flash".to_string()
                    };
                    let remapped_provider = Self::get_provider_for_model(&remapped_model);
                    info!(
                        "SmartRouter [cost_aware]: Remapped '{}' -> '{}' due to short prompt ({} chars < 500)",
                        requested_model, remapped_model, total_chars
                    );
                    (remapped_model, remapped_provider)
                } else {
                    (requested_model.clone(), requested_provider)
                }
            }
            "latency_aware" => {
                // Choose the provider with the lowest tracked moving average latency
                let providers = vec!["gemini", "openai", "anthropic"];
                let mut best_provider = "gemini";
                let mut best_latency = f64::MAX;

                for p in providers {
                    let lat = self.latency_tracker.get_latency(p);
                    if lat < best_latency {
                        best_latency = lat;
                        best_provider = p;
                    }
                }

                let target_model = Self::get_default_model_for_provider(best_provider);
                info!(
                    "SmartRouter [latency_aware]: Selected provider '{}' with lowest average latency ({:.1}ms). Model set to '{}'. Originally requested: '{}'",
                    best_provider, best_latency, target_model, requested_model
                );
                (target_model, best_provider.to_string())
            }
            "load_balanced" => {
                let providers = vec!["openai", "gemini", "anthropic"];
                let idx = self.round_robin_counter.fetch_add(1, Ordering::SeqCst);
                let chosen_provider = providers[idx % providers.len()];
                let target_model = Self::get_default_model_for_provider(chosen_provider);
                info!(
                    "SmartRouter [load_balanced]: Balanced request to provider '{}'. Model set to '{}'",
                    chosen_provider, target_model
                );
                (target_model, chosen_provider.to_string())
            }
            _ => {
                // Default back to direct routing
                (requested_model.clone(), requested_provider)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::Message;

    #[test]
    fn test_latency_tracker_ema() {
        let tracker = LatencyTracker::new();
        // Baseline initial check
        assert!(tracker.get_latency("openai") == 150.0);
        
        // Record new latency and check calculation
        // 0.2 * 100.0 + 0.8 * 150.0 = 20.0 + 120.0 = 140.0
        tracker.record_latency("openai", 100.0);
        assert_eq!(tracker.get_latency("openai"), 140.0);
    }

    #[test]
    fn test_cost_aware_routing() {
        let tracker = LatencyTracker::new();
        let router = SmartRouter::new(tracker);

        // Short prompt targeting gpt-4o -> should map to gpt-4o-mini
        let payload_short = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![Message {
                role: "user".to_string(),
                content: "Hello".to_string(),
            }],
            stream: Some(false),
            temperature: None,
            max_tokens: None,
        };
        let (model, provider) = router.resolve_route(&payload_short, "cost_aware");
        assert_eq!(model, "gpt-4o-mini");
        assert_eq!(provider, "openai");

        // Long prompt targeting gpt-4o -> should keep gpt-4o
        let payload_long = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![Message {
                role: "user".to_string(),
                content: "a".repeat(600),
            }],
            stream: Some(false),
            temperature: None,
            max_tokens: None,
        };
        let (model, provider) = router.resolve_route(&payload_long, "cost_aware");
        assert_eq!(model, "gpt-4o");
        assert_eq!(provider, "openai");
    }

    #[test]
    fn test_load_balanced_routing() {
        let tracker = LatencyTracker::new();
        let router = SmartRouter::new(tracker);

        let payload = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: vec![],
            stream: Some(false),
            temperature: None,
            max_tokens: None,
        };

        // Check sequence of load balanced selections
        let (m1, _) = router.resolve_route(&payload, "load_balanced");
        let (m2, _) = router.resolve_route(&payload, "load_balanced");
        let (m3, _) = router.resolve_route(&payload, "load_balanced");
        let (m4, _) = router.resolve_route(&payload, "load_balanced");

        // Round robin should cycle across providers: openai -> gemini -> anthropic -> openai
        assert_eq!(m1, "gpt-4o");
        assert_eq!(m2, "gemini-1.5-flash");
        assert_eq!(m3, "claude-3-5-sonnet");
        assert_eq!(m4, "gpt-4o");
    }
}
