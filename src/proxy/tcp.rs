//! TCP proxy — accepts connections and relays bytes bidirectionally.

use std::sync::Arc;

use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn, Instrument};

use crate::routing::Router;
use crate::state::Pool;

/// Run a TCP proxy that listens for inbound connections and forwards
/// them to backends selected by the provided router.
pub async fn run(
    listener: TcpListener,
    pool: Arc<Pool>,
    router: Arc<dyn Router>,
    cancel: CancellationToken,
) {
    let local_addr = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());

    info!(
        listen = %local_addr,
        pool = %pool.name,
        "TCP proxy listening"
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(listen = %local_addr, "TCP proxy shutting down");
                return;
            }
            result = listener.accept() => {
                let (inbound, client_addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!(error = %e, "failed to accept TCP connection");
                        continue;
                    }
                };

                let backend = match router.select(&pool) {
                    Some(b) => b,
                    None => {
                        warn!(
                            pool = %pool.name,
                            client = %client_addr,
                            "no healthy backends — dropping connection"
                        );
                        continue;
                    }
                };

                let span = tracing::info_span!(
                    "tcp_proxy",
                    client = %client_addr,
                    backend = %backend.addr,
                );

                // Track the connection for least-connections routing.
                let guard = backend.track_connection();
                let child_cancel = cancel.clone();

                tokio::spawn(
                    async move {
                        debug!("proxying connection");
                        if let Err(e) = proxy_connection(
                            inbound,
                            guard.backend().addr,
                            child_cancel,
                        )
                        .await
                        {
                            debug!(error = %e, "connection ended with error");
                        }
                        // `guard` is dropped here, decrementing the count.
                        drop(guard);
                        debug!("connection closed");
                    }
                    .instrument(span),
                );
            }
        }
    }
}

/// Proxy a single TCP connection by connecting to the backend and
/// copying bytes in both directions.
async fn proxy_connection(
    mut inbound: TcpStream,
    backend_addr: std::net::SocketAddr,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    let mut outbound = TcpStream::connect(backend_addr).await?;

    tokio::select! {
        _ = cancel.cancelled() => {
            debug!("connection cancelled by shutdown signal");
            Ok(())
        }
        result = copy_bidirectional(&mut inbound, &mut outbound) => {
            match result {
                Ok((client_to_backend, backend_to_client)) => {
                    debug!(
                        client_to_backend,
                        backend_to_client,
                        "transfer complete"
                    );
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
    }
}
