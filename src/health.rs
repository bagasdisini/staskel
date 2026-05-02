//! Active health checking for backend servers.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::HealthCheckConfig;
use crate::state::Pool;

struct ProbeState {
    successes: u32,
    failures: u32,
}

impl ProbeState {
    fn new() -> Self {
        Self {
            successes: 0,
            failures: 0,
        }
    }

    fn record_success(&mut self) {
        self.successes += 1;
        self.failures = 0;
    }

    fn record_failure(&mut self) {
        self.failures += 1;
        self.successes = 0;
    }
}

/// Spawn a health-checking background task for a single backend pool.
pub fn spawn_health_checker(
    pool: Arc<Pool>,
    config: HealthCheckConfig,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_secs(config.interval_secs);
        let timeout = Duration::from_secs(config.timeout_secs);

        // Track consecutive results per backend address.
        let mut probe_states: HashMap<SocketAddr, ProbeState> = pool
            .backends
            .iter()
            .map(|b| (b.addr, ProbeState::new()))
            .collect();

        let mut ticker = time::interval(interval);
        // The first tick completes immediately — skip it so we don't
        // probe before backends have had time to start.
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!(pool = %pool.name, "health checker shutting down");
                    return;
                }
                _ = ticker.tick() => {
                    for backend in &pool.backends {
                        let healthy = probe_backend(backend.addr, timeout).await;
                        let state = probe_states
                            .entry(backend.addr)
                            .or_insert_with(ProbeState::new);

                        if healthy {
                            state.record_success();
                            debug!(
                                pool = %pool.name,
                                backend = %backend.addr,
                                consecutive_successes = state.successes,
                                "health check passed"
                            );

                            if !backend.is_healthy()
                                && state.successes >= config.healthy_threshold
                            {
                                info!(
                                    pool = %pool.name,
                                    backend = %backend.addr,
                                    "backend marked HEALTHY"
                                );
                                backend.set_healthy(true);
                            }
                        } else {
                            state.record_failure();
                            warn!(
                                pool = %pool.name,
                                backend = %backend.addr,
                                consecutive_failures = state.failures,
                                "health check failed"
                            );

                            if backend.is_healthy()
                                && state.failures >= config.unhealthy_threshold
                            {
                                warn!(
                                    pool = %pool.name,
                                    backend = %backend.addr,
                                    "backend marked UNHEALTHY"
                                );
                                backend.set_healthy(false);
                            }
                        }
                    }
                }
            }
        }
    })
}

/// Attempt a TCP connection to the given address within the timeout.
async fn probe_backend(addr: SocketAddr, timeout: Duration) -> bool {
    match time::timeout(timeout, TcpStream::connect(addr)).await {
        Ok(Ok(_stream)) => true,
        Ok(Err(_)) | Err(_) => false,
    }
}
