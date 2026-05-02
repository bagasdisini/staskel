//! CLI entry point for the staskel load balancer.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use staskel::balancer::Balancer;
use staskel::config::Config;

/// Staskel — A high-performance Layer 4 (TCP/UDP) load balancer.
#[derive(Parser, Debug)]
#[command(name = "staskel", version, about, long_about = None)]
struct Cli {
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialise structured logging with env-filter support.
    // Default to INFO level; override with RUST_LOG env var.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .compact()
        .init();

    let cli = Cli::parse();

    info!(config = %cli.config.display(), "loading configuration");

    let config = Config::from_file(&cli.config)
        .with_context(|| format!("failed to load config from {}", cli.config.display()))?;

    info!(
        frontends = config.frontends.len(),
        pools = config.backend_pools.len(),
        "configuration loaded successfully"
    );

    let balancer = Balancer::new(config);
    balancer.run().await
}
