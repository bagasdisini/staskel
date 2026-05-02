//! Custom error types for the staskel load balancer.

use std::net::SocketAddr;

/// Errors that can occur within the staskel load balancer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A configuration file could not be parsed or contains invalid values.
    #[error("configuration error: {0}")]
    Config(String),

    /// An I/O operation failed (network bind, connect, read, write).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// No healthy backend servers are available to handle a request.
    #[error("no healthy backends available for pool '{0}'")]
    NoHealthyBackends(String),

    /// A health check probe failed for a specific backend.
    #[error("health check failed for {addr}: {reason}")]
    HealthCheckFailed {
        addr: SocketAddr,
        reason: String,
    },

    /// The YAML configuration could not be deserialized.
    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

/// Convenience type alias for results within the staskel crate.
pub type Result<T> = std::result::Result<T, Error>;
