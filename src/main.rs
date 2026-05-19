mod config;

use tracing::{info, error, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    // Initialize standard formatted tracing subscriber with environmental filters
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
        
    tracing::subscriber::set_global_default(subscriber)
        .expect("Setting default tracing subscriber failed");
        
    info!("============================================================");
    info!("   AXIOM: AI Agent Orchestration Gateway — Bootstrapping   ");
    info!("============================================================");
    
    // Load config.yaml from current working directory
    let config_path = "config.yaml";
    match config::AppConfig::load(config_path) {
        Ok(cfg) => {
            info!("Successfully parsed configuration file!");
            info!("Gateway Server Bind: http://{}:{}", cfg.gateway.host, cfg.gateway.port);
            info!("Rate Limiting Status: [Enabled: {}]", cfg.rate_limiting.enabled);
            info!("Registered AI Providers: {:?}", cfg.providers.keys().collect::<Vec<_>>());
            info!("Active Routing Policy: '{}'", cfg.routing.default_policy);
            info!("✓ AXIOM Phase 1 Architecture Validation Completed!");
        }
        Err(e) => {
            error!("✖ Fatal Configuration Bootstrap Error: {}", e);
            std::process::exit(1);
        }
    }
}
