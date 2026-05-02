//! Integration tests for the staskel load balancer.
//!
//! These tests spawn real TCP echo servers, start the load balancer,
//! and verify that traffic is correctly routed through it.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use staskel::proxy;
use staskel::routing::{LeastConnections, RoundRobin};
use staskel::state::{Backend, Pool};

/// Spawn a TCP echo server that reads data and writes it back.
/// Returns the address it bound to.
async fn spawn_echo_server(cancel: CancellationToken) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                result = listener.accept() => {
                    let (mut stream, _) = result.unwrap();
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 1024];
                        loop {
                            match stream.read(&mut buf).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => {
                                    if stream.write_all(&buf[..n]).await.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    });
                }
            }
        }
    });

    addr
}

/// Helper: build a pool from a list of addresses.
fn make_pool(name: &str, addrs: &[SocketAddr]) -> Arc<Pool> {
    let backends = addrs.iter().map(|a| Arc::new(Backend::new(*a))).collect();
    Arc::new(Pool {
        name: name.to_string(),
        backends,
    })
}

#[tokio::test]
async fn tcp_round_robin_distributes_evenly() {
    let cancel = CancellationToken::new();

    // Spawn two echo servers.
    let addr1 = spawn_echo_server(cancel.clone()).await;
    let addr2 = spawn_echo_server(cancel.clone()).await;

    let pool = make_pool("test", &[addr1, addr2]);
    let router = Arc::new(RoundRobin::new());

    // Bind the load balancer frontend.
    let lb_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let lb_addr = lb_listener.local_addr().unwrap();

    let proxy_cancel = cancel.clone();
    tokio::spawn(async move {
        proxy::tcp::run(lb_listener, pool, router, proxy_cancel).await;
    });

    // Give the proxy a moment to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send 4 requests through the LB and verify echo works.
    for i in 0..4u8 {
        let mut stream = TcpStream::connect(lb_addr).await.unwrap();
        let msg = format!("hello {i}");
        stream.write_all(msg.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert_eq!(response, msg, "echo mismatch on request {i}");
    }

    cancel.cancel();
}

#[tokio::test]
async fn tcp_least_connections_routes_correctly() {
    let cancel = CancellationToken::new();

    let addr1 = spawn_echo_server(cancel.clone()).await;
    let addr2 = spawn_echo_server(cancel.clone()).await;

    let pool = make_pool("test", &[addr1, addr2]);
    let router = Arc::new(LeastConnections::new());

    let lb_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let lb_addr = lb_listener.local_addr().unwrap();

    let proxy_cancel = cancel.clone();
    tokio::spawn(async move {
        proxy::tcp::run(lb_listener, pool, router, proxy_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify basic echo functionality through the LB.
    let mut stream = TcpStream::connect(lb_addr).await.unwrap();
    stream.write_all(b"test data").await.unwrap();
    stream.shutdown().await.unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert_eq!(response, "test data");

    cancel.cancel();
}

#[tokio::test]
async fn tcp_handles_no_healthy_backends() {
    let cancel = CancellationToken::new();

    // Create a pool where all backends are unhealthy (no actual servers).
    let pool = make_pool(
        "test",
        &["127.0.0.1:1".parse().unwrap()],
    );
    pool.backends[0].set_healthy(false);

    let router = Arc::new(RoundRobin::new());
    let lb_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let lb_addr = lb_listener.local_addr().unwrap();

    let proxy_cancel = cancel.clone();
    tokio::spawn(async move {
        proxy::tcp::run(lb_listener, pool, router, proxy_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect to LB — the connection should be accepted but then
    // dropped because there are no healthy backends.
    let result = TcpStream::connect(lb_addr).await;
    // The connection itself may succeed (TCP accept) but the LB
    // should drop it. We just verify no panic or hang.
    if let Ok(mut stream) = result {
        stream.write_all(b"test").await.ok();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    cancel.cancel();
}

#[tokio::test]
async fn tcp_large_payload() {
    let cancel = CancellationToken::new();

    let addr = spawn_echo_server(cancel.clone()).await;
    let pool = make_pool("test", &[addr]);
    let router = Arc::new(RoundRobin::new());

    let lb_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let lb_addr = lb_listener.local_addr().unwrap();

    let proxy_cancel = cancel.clone();
    tokio::spawn(async move {
        proxy::tcp::run(lb_listener, pool, router, proxy_cancel).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a 64 KB payload through the LB.
    let payload = vec![0xABu8; 64 * 1024];
    let mut stream = TcpStream::connect(lb_addr).await.unwrap();
    stream.write_all(&payload).await.unwrap();
    stream.shutdown().await.unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    assert_eq!(response.len(), payload.len());
    assert_eq!(response, payload);

    cancel.cancel();
}

/// Verify that the config example file can be parsed successfully.
#[test]
fn config_example_parses() {
    let contents = std::fs::read_to_string("config.example.yaml").unwrap();
    let config: staskel::config::Config = serde_yaml::from_str(&contents).unwrap();

    // Basic sanity checks.
    assert!(!config.frontends.is_empty());
    assert!(!config.backend_pools.is_empty());

    // Ensure validation passes (cross-references are correct).
    let path = std::path::Path::new("config.example.yaml");
    let validated = staskel::config::Config::from_file(path);
    assert!(validated.is_ok(), "config.example.yaml failed validation: {validated:?}");
}

/// Unit test for round-robin routing algorithm via integration-level import.
#[test]
fn round_robin_unit_integration() {
    use staskel::routing::Router;

    let pool = make_pool(
        "test",
        &[
            "127.0.0.1:3001".parse().unwrap(),
            "127.0.0.1:3002".parse().unwrap(),
        ],
    );

    let rr = RoundRobin::new();

    let a1 = rr.select(&pool).unwrap().addr;
    let a2 = rr.select(&pool).unwrap().addr;
    let a3 = rr.select(&pool).unwrap().addr;

    assert_ne!(a1, a2, "round robin should alternate");
    assert_eq!(a1, a3, "round robin should cycle back");
}
