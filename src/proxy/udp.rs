//! UDP proxy — relays datagrams between clients and backends.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::net::UdpSocket;
use tokio::time::{self, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn, Instrument};

use crate::routing::Router;
use crate::state::Pool;

const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_DATAGRAM_SIZE: usize = 65535;

struct Session {
    backend_socket: Arc<UdpSocket>,
    last_active: Instant,
}

/// Run a UDP proxy that receives datagrams on the frontend socket and
/// relays them to backends selected by the provided router.
pub async fn run(
    frontend: Arc<UdpSocket>,
    pool: Arc<Pool>,
    router: Arc<dyn Router>,
    cancel: CancellationToken,
) {
    let local_addr = frontend
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());

    info!(
        listen = %local_addr,
        pool = %pool.name,
        "UDP proxy listening"
    );

    let sessions: Arc<DashMap<SocketAddr, Session>> = Arc::new(DashMap::new());

    // Spawn a periodic cleanup task for expired sessions.
    let cleanup_sessions = Arc::clone(&sessions);
    let cleanup_cancel = cancel.clone();
    tokio::spawn(async move {
        let mut ticker = time::interval(SESSION_IDLE_TIMEOUT);
        loop {
            tokio::select! {
                _ = cleanup_cancel.cancelled() => return,
                _ = ticker.tick() => {
                    cleanup_sessions.retain(|_addr, session| {
                        session.last_active.elapsed() < SESSION_IDLE_TIMEOUT
                    });
                }
            }
        }
    });

    let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(listen = %local_addr, "UDP proxy shutting down");
                return;
            }
            result = frontend.recv_from(&mut buf) => {
                let (len, client_addr) = match result {
                    Ok(r) => r,
                    Err(e) => {
                        error!(error = %e, "failed to receive UDP datagram");
                        continue;
                    }
                };

                let data = &buf[..len];

                // Check if we have an existing session for this client.
                let needs_new_session = !sessions.contains_key(&client_addr);

                if needs_new_session {
                    let backend = match router.select(&pool) {
                        Some(b) => b,
                        None => {
                            warn!(
                                pool = %pool.name,
                                client = %client_addr,
                                "no healthy backends — dropping datagram"
                            );
                            continue;
                        }
                    };

                    let backend_socket = match create_backend_socket(backend.addr).await {
                        Ok(s) => Arc::new(s),
                        Err(e) => {
                            error!(
                                error = %e,
                                backend = %backend.addr,
                                "failed to create backend socket"
                            );
                            continue;
                        }
                    };

                    let _guard = backend.track_connection();

                    sessions.insert(
                        client_addr,
                        Session {
                            backend_socket: Arc::clone(&backend_socket),
                            last_active: Instant::now(),
                        },
                    );

                    // Spawn a task to relay responses from this backend
                    // back to the client through the frontend socket.
                    let frontend_clone = Arc::clone(&frontend);
                    let sessions_clone = Arc::clone(&sessions);
                    let child_cancel = cancel.clone();
                    let span = tracing::info_span!(
                        "udp_relay",
                        client = %client_addr,
                        backend = %backend.addr,
                    );

                    tokio::spawn(
                        async move {
                            relay_backend_to_client(
                                &backend_socket,
                                &frontend_clone,
                                client_addr,
                                &sessions_clone,
                                child_cancel,
                            )
                            .await;
                            // Session cleanup: remove when relay ends.
                            sessions_clone.remove(&client_addr);
                            drop(_guard);
                        }
                        .instrument(span),
                    );
                }

                // Forward the datagram to the backend.
                if let Some(mut session) = sessions.get_mut(&client_addr) {
                    session.last_active = Instant::now();
                    if let Err(e) = session.backend_socket.send(data).await {
                        debug!(
                            client = %client_addr,
                            error = %e,
                            "failed to forward datagram to backend"
                        );
                    }
                }
            }
        }
    }
}

/// Create a UDP socket bound to an ephemeral port, connected to the backend.
async fn create_backend_socket(backend_addr: SocketAddr) -> std::io::Result<UdpSocket> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(backend_addr).await?;
    Ok(socket)
}

/// Relay datagrams from a backend socket back to the client via the frontend socket.
async fn relay_backend_to_client(
    backend_socket: &UdpSocket,
    frontend: &UdpSocket,
    client_addr: SocketAddr,
    sessions: &DashMap<SocketAddr, Session>,
    cancel: CancellationToken,
) {
    let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            result = time::timeout(SESSION_IDLE_TIMEOUT, backend_socket.recv(&mut buf)) => {
                match result {
                    Ok(Ok(len)) => {
                        // Update the session's last-active timestamp.
                        if let Some(mut session) = sessions.get_mut(&client_addr) {
                            session.last_active = Instant::now();
                        }

                        if let Err(e) = frontend.send_to(&buf[..len], client_addr).await {
                            debug!(
                                client = %client_addr,
                                error = %e,
                                "failed to relay response to client"
                            );
                        }
                    }
                    Ok(Err(e)) => {
                        debug!(
                            client = %client_addr,
                            error = %e,
                            "backend socket recv error"
                        );
                        return;
                    }
                    Err(_) => {
                        // Timeout — session is idle, clean up.
                        debug!(client = %client_addr, "UDP session timed out");
                        return;
                    }
                }
            }
        }
    }
}
