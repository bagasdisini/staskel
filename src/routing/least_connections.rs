//! Least-connections load-balancing strategy.

use std::sync::Arc;

use crate::state::{Backend, Pool};

use super::Router;

/// Routes to the backend with the minimum number of active connections.
///
/// # Algorithm
/// On each call to [`select`](Router::select):
/// 1. Filter the pool to only healthy backends.
/// 2. Read each backend's `active_connections()` atomically.
/// 3. Return the backend with the smallest value (first wins on tie).
///
/// This is O(n) in the number of backends, which is acceptable because
/// backend pools are typically small (< 100 servers).
#[derive(Debug)]
pub struct LeastConnections;

impl LeastConnections {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LeastConnections {
    fn default() -> Self {
        Self::new()
    }
}

impl Router for LeastConnections {
    fn select(&self, pool: &Pool) -> Option<Arc<Backend>> {
        pool.healthy_backends()
            .into_iter()
            .min_by_key(|b| b.active_connections())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Pool;

    fn make_pool(addrs: &[&str]) -> Pool {
        let backends = addrs
            .iter()
            .map(|a| Arc::new(Backend::new(a.parse().unwrap())))
            .collect();
        Pool {
            name: "test".into(),
            backends,
        }
    }

    #[test]
    fn selects_backend_with_fewest_connections() {
        let pool = make_pool(&["127.0.0.1:3001", "127.0.0.1:3002", "127.0.0.1:3003"]);

        // Simulate connections: b0 has 5, b1 has 2, b2 has 8
        let _guards0: Vec<_> = (0..5).map(|_| pool.backends[0].track_connection()).collect();
        let _guards1: Vec<_> = (0..2).map(|_| pool.backends[1].track_connection()).collect();
        let _guards2: Vec<_> = (0..8).map(|_| pool.backends[2].track_connection()).collect();

        let lc = LeastConnections::new();
        let selected = lc.select(&pool).unwrap();

        // Should pick :3002 with 2 connections
        assert_eq!(selected.addr, "127.0.0.1:3002".parse().unwrap());
    }

    #[test]
    fn picks_first_on_tie() {
        let pool = make_pool(&["127.0.0.1:3001", "127.0.0.1:3002"]);
        // Both have 0 connections — should pick the first one
        let lc = LeastConnections::new();
        let selected = lc.select(&pool).unwrap();
        assert_eq!(selected.addr, "127.0.0.1:3001".parse().unwrap());
    }

    #[test]
    fn updates_after_connection_drop() {
        let pool = make_pool(&["127.0.0.1:3001", "127.0.0.1:3002"]);
        let lc = LeastConnections::new();

        // Give b0 a connection
        let guard = pool.backends[0].track_connection();
        let selected = lc.select(&pool).unwrap();
        assert_eq!(selected.addr, "127.0.0.1:3002".parse().unwrap());

        // Drop the connection — b0 should be selectable again
        drop(guard);
        let selected = lc.select(&pool).unwrap();
        // Both at 0 now, first wins
        assert_eq!(selected.addr, "127.0.0.1:3001".parse().unwrap());
    }

    #[test]
    fn skips_unhealthy_even_if_fewer_connections() {
        let pool = make_pool(&["127.0.0.1:3001", "127.0.0.1:3002"]);
        // b0 has 0 connections but is unhealthy
        pool.backends[0].set_healthy(false);
        let _guard = pool.backends[1].track_connection();

        let lc = LeastConnections::new();
        let selected = lc.select(&pool).unwrap();
        assert_eq!(selected.addr, "127.0.0.1:3002".parse().unwrap());
    }

    #[test]
    fn returns_none_when_all_unhealthy() {
        let pool = make_pool(&["127.0.0.1:3001"]);
        pool.backends[0].set_healthy(false);

        let lc = LeastConnections::new();
        assert!(lc.select(&pool).is_none());
    }
}
