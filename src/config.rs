use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub gateway: GatewayConfig,
    pub rate_limiting: RateLimitingConfig,
    pub providers: HashMap<String, ProviderConfig>,
    pub routing: RoutingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    pub host: String,
    pub port: u16,
    pub admin_api_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitingConfig {
    pub enabled: bool,
    pub burst_capacity: f64,
    pub tokens_per_minute: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub base_url: String,
    pub api_keys: Vec<String>,
    pub circuit_breaker: CircuitBreakerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: usize,
    pub cooldown_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    pub default_policy: String,
    pub fallback_chains: HashMap<String, Vec<String>>,
}

impl AppConfig {
    /// Loads and parses the configuration YAML file from the specified path.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let path_ref = path.as_ref();
        let mut file = File::open(path_ref)
            .map_err(|e| format!("Failed to open config file '{}': {}", path_ref.display(), e))?;
        
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|e| format!("Failed to read config file '{}': {}", path_ref.display(), e))?;
        
        let config: AppConfig = serde_yaml::from_str(&contents)
            .map_err(|e| format!("Failed to parse config YAML: {}", e))?;
            
        Ok(config)
    }
}
