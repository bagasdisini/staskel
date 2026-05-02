//! Top-level configuration loading and validation for the load balancer.

use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;

use crate::error::{Error, Result};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub frontends: Vec<FrontendConfig>,
    pub backend_pools: HashMap<String, BackendPoolConfig>,
    #[serde(default)]
    pub health_check: HealthCheckConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FrontendConfig {
    pub name: String,
    pub listen: SocketAddr,
    pub protocol: Protocol,
    pub backends: String,
    pub algorithm: Algorithm,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BackendPoolConfig {
    pub backends: Vec<SocketAddr>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HealthCheckConfig {
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "default_unhealthy_threshold")]
    pub unhealthy_threshold: u32,
    #[serde(default = "default_healthy_threshold")]
    pub healthy_threshold: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_interval(),
            timeout_secs: default_timeout(),
            unhealthy_threshold: default_unhealthy_threshold(),
            healthy_threshold: default_healthy_threshold(),
        }
    }
}

fn default_interval() -> u64 {
    10
}
fn default_timeout() -> u64 {
    3
}
fn default_unhealthy_threshold() -> u32 {
    3
}
fn default_healthy_threshold() -> u32 {
    1
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Tcp,
    Udp,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    RoundRobin,
    LeastConnections,
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("failed to read {}: {e}", path.display())))?;
        let config: Config = serde_yaml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.frontends.is_empty() {
            return Err(Error::Config(
                "at least one frontend must be defined".into(),
            ));
        }

        for frontend in &self.frontends {
            if !self.backend_pools.contains_key(&frontend.backends) {
                return Err(Error::Config(format!(
                    "frontend '{}' references unknown backend pool '{}'",
                    frontend.name, frontend.backends
                )));
            }
        }

        for (name, pool) in &self.backend_pools {
            if pool.backends.is_empty() {
                return Err(Error::Config(format!(
                    "backend pool '{name}' has no backends defined"
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_valid_config() {
        let yaml = r#"
frontends:
  - name: web
    listen: "0.0.0.0:8080"
    protocol: tcp
    backends: web_pool
    algorithm: round_robin

backend_pools:
  web_pool:
    backends:
      - "127.0.0.1:3001"
      - "127.0.0.1:3002"

health_check:
  interval_secs: 5
  timeout_secs: 2
  unhealthy_threshold: 3
  healthy_threshold: 1
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.frontends.len(), 1);
        assert_eq!(config.frontends[0].protocol, Protocol::Tcp);
        assert_eq!(config.frontends[0].algorithm, Algorithm::RoundRobin);
        assert_eq!(config.backend_pools["web_pool"].backends.len(), 2);
    }

    #[test]
    fn validate_rejects_missing_pool() {
        let yaml = r#"
frontends:
  - name: web
    listen: "0.0.0.0:8080"
    protocol: tcp
    backends: nonexistent
    algorithm: round_robin

backend_pools:
  web_pool:
    backends:
      - "127.0.0.1:3001"
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_pool() {
        let yaml = r#"
frontends:
  - name: web
    listen: "0.0.0.0:8080"
    protocol: tcp
    backends: web_pool
    algorithm: round_robin

backend_pools:
  web_pool:
    backends: []
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn default_health_check_values() {
        let yaml = r#"
frontends:
  - name: web
    listen: "0.0.0.0:8080"
    protocol: tcp
    backends: web_pool
    algorithm: least_connections

backend_pools:
  web_pool:
    backends:
      - "127.0.0.1:3001"
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.health_check.interval_secs, 10);
        assert_eq!(config.health_check.timeout_secs, 3);
        assert_eq!(config.health_check.unhealthy_threshold, 3);
        assert_eq!(config.health_check.healthy_threshold, 1);
    }
}
