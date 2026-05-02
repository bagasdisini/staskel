//! Shared state management for backend server pools.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct Backend {
    pub addr: SocketAddr,
    healthy: AtomicBool,
    active_connections: AtomicUsize,
}

impl Backend {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            healthy: AtomicBool::new(true),
            active_connections: AtomicUsize::new(0),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::Release);
    }

    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }

    pub fn track_connection(self: &Arc<Self>) -> ConnectionGuard {
        self.active_connections.fetch_add(1, Ordering::AcqRel);
        ConnectionGuard {
            backend: Arc::clone(self),
        }
    }
}

#[derive(Debug)]
pub struct ConnectionGuard {
    backend: Arc<Backend>,
}

impl ConnectionGuard {
    pub fn backend(&self) -> &Arc<Backend> {
        &self.backend
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.backend
            .active_connections
            .fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
pub struct Pool {
    pub name: String,
    pub backends: Vec<Arc<Backend>>,
}

impl Pool {
    /// Returns only the backends that are currently marked healthy.
    pub fn healthy_backends(&self) -> Vec<Arc<Backend>> {
        self.backends
            .iter()
            .filter(|b| b.is_healthy())
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_health_toggle() {
        let backend = Backend::new("127.0.0.1:3000".parse().unwrap());
        assert!(backend.is_healthy());

        backend.set_healthy(false);
        assert!(!backend.is_healthy());

        backend.set_healthy(true);
        assert!(backend.is_healthy());
    }

    #[test]
    fn connection_guard_tracks_correctly() {
        let backend = Arc::new(Backend::new("127.0.0.1:3000".parse().unwrap()));
        assert_eq!(backend.active_connections(), 0);

        let guard1 = backend.track_connection();
        assert_eq!(backend.active_connections(), 1);

        let guard2 = backend.track_connection();
        assert_eq!(backend.active_connections(), 2);

        drop(guard1);
        assert_eq!(backend.active_connections(), 1);

        drop(guard2);
        assert_eq!(backend.active_connections(), 0);
    }

    #[test]
    fn pool_filters_unhealthy_backends() {
        let b1 = Arc::new(Backend::new("127.0.0.1:3001".parse().unwrap()));
        let b2 = Arc::new(Backend::new("127.0.0.1:3002".parse().unwrap()));
        let b3 = Arc::new(Backend::new("127.0.0.1:3003".parse().unwrap()));

        b2.set_healthy(false);

        let pool = Pool {
            name: "test".into(),
            backends: vec![b1, b2, b3],
        };

        let healthy = pool.healthy_backends();
        assert_eq!(healthy.len(), 2);
        assert_eq!(healthy[0].addr, "127.0.0.1:3001".parse().unwrap());
        assert_eq!(healthy[1].addr, "127.0.0.1:3003".parse().unwrap());
    }

    #[test]
    fn pool_returns_empty_when_all_unhealthy() {
        let b1 = Arc::new(Backend::new("127.0.0.1:3001".parse().unwrap()));
        b1.set_healthy(false);

        let pool = Pool {
            name: "test".into(),
            backends: vec![b1],
        };

        assert!(pool.healthy_backends().is_empty());
    }
}
