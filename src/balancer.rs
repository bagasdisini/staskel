//! Top-level orchestrator that wires together all components.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::net::{TcpListener, UdpSocket};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::{Algorithm, Config, Protocol};
use crate::health;
use crate::proxy;
use crate::routing::{LeastConnections, RoundRobin, Router};
use crate::state::{Backend, Pool};

pub struct Balancer {
    config: Config,
}

impl Balancer {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Run the load balancer until a shutdown signal is received.
    /// This is the main entry point.
    pub async fn run(&self) -> anyhow::Result<()> {
        let cancel = CancellationToken::new();

        // Build pools
        let pools = self.build_pools();
        info!(pool_count = pools.len(), "backend pools initialised");

        // Health checkers
        let mut health_handles = Vec::new();
        for pool in pools.values() {
            let handle = health::spawn_health_checker(
                Arc::clone(pool),
                self.config.health_check.clone(),
                cancel.clone(),
            );
            health_handles.push(handle);
        }
        info!("health checkers started");

        // Frontend listeners
        let mut proxy_handles = Vec::new();
        for frontend in &self.config.frontends {
            let pool = pools
                .get(&frontend.backends)
                .expect("validated in config")
                .clone();

            let router: Arc<dyn Router> = match frontend.algorithm {
                Algorithm::RoundRobin => Arc::new(RoundRobin::new()),
                Algorithm::LeastConnections => Arc::new(LeastConnections::new()),
            };

            match frontend.protocol {
                Protocol::Tcp => {
                    let listener = TcpListener::bind(frontend.listen).await?;
                    info!(
                        name = %frontend.name,
                        listen = %frontend.listen,
                        protocol = "TCP",
                        algorithm = ?frontend.algorithm,
                        pool = %frontend.backends,
                        "frontend bound"
                    );
                    let child_cancel = cancel.clone();
                    let handle = tokio::spawn(async move {
                        proxy::tcp::run(listener, pool, router, child_cancel).await;
                    });
                    proxy_handles.push(handle);
                }
                Protocol::Udp => {
                    let socket = UdpSocket::bind(frontend.listen).await?;
                    info!(
                        name = %frontend.name,
                        listen = %frontend.listen,
                        protocol = "UDP",
                        algorithm = ?frontend.algorithm,
                        pool = %frontend.backends,
                        "frontend bound"
                    );
                    let frontend_socket = Arc::new(socket);
                    let child_cancel = cancel.clone();
                    let handle = tokio::spawn(async move {
                        proxy::udp::run(frontend_socket, pool, router, child_cancel).await;
                    });
                    proxy_handles.push(handle);
                }
            }
        }

        info!(
            frontend_count = self.config.frontends.len(),
            "all frontends started — load balancer is ready"
        );

        // Wait for shutdown signal
        wait_for_shutdown_signal().await;
        info!("shutdown signal received — draining connections");

        // Cancel and drain
        cancel.cancel();

        // Wait for proxy tasks to finish (connections drain naturally).
        for handle in proxy_handles {
            if let Err(e) = handle.await {
                error!(error = %e, "proxy task panicked");
            }
        }

        // Wait for health checkers.
        for handle in health_handles {
            if let Err(e) = handle.await {
                error!(error = %e, "health checker task panicked");
            }
        }

        info!("shutdown complete");
        Ok(())
    }

    /// Build the backend pools from the configuration.
    /// Each pool is an `Arc<Pool>` so it can be shared between the
    /// health checker and the proxy tasks without cloning the backends.
    fn build_pools(&self) -> HashMap<String, Arc<Pool>> {
        self.config
            .backend_pools
            .iter()
            .map(|(name, pool_config)| {
                let backends = pool_config
                    .backends
                    .iter()
                    .map(|addr| Arc::new(Backend::new(*addr)))
                    .collect();

                let pool = Arc::new(Pool {
                    name: name.clone(),
                    backends,
                });

                (name.clone(), pool)
            })
            .collect()
    }
}

/// Wait for a shutdown signal from the operating system.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to register SIGINT");
        let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM");
        tokio::select! {
            _ = sigint.recv() => info!("received SIGINT"),
            _ = sigterm.recv() => info!("received SIGTERM"),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for Ctrl-C");
        info!("received Ctrl-C");
    }
}
