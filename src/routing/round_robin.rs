//! Round-robin load-balancing strategy.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::state::{Backend, Pool};

use super::Router;

/// Distributes traffic evenly across backends in a rotating sequence.
///
/// # Algorithm
/// On each call to [`select`](Router::select):
/// 1. Filter the pool to only healthy backends.
/// 2. Compute `index = counter % healthy_count`.
/// 3. Atomically increment the counter (wraps naturally via modulo).
/// 4. Return the backend at that index.
#[derive(Debug)]
pub struct RoundRobin {
    counter: AtomicUsize,
}

impl RoundRobin {
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobin {
    fn default() -> Self {
        Self::new()
    }
}

impl Router for RoundRobin {
    fn select(&self, pool: &Pool) -> Option<Arc<Backend>> {
        let healthy = pool.healthy_backends();
        if healthy.is_empty() {
            return None;
        }
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) % healthy.len();
        Some(Arc::clone(&healthy[idx]))
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
    fn cycles_through_backends() {
        let pool = make_pool(&["127.0.0.1:3001", "127.0.0.1:3002", "127.0.0.1:3003"]);
        let rr = RoundRobin::new();

        let addrs: Vec<_> = (0..6).map(|_| rr.select(&pool).unwrap().addr).collect();

        // Should cycle: 0, 1, 2, 0, 1, 2
        assert_eq!(addrs[0], addrs[3]);
        assert_eq!(addrs[1], addrs[4]);
        assert_eq!(addrs[2], addrs[5]);
        // All three are distinct
        assert_ne!(addrs[0], addrs[1]);
        assert_ne!(addrs[1], addrs[2]);
    }

    #[test]
    fn skips_unhealthy_backends() {
        let pool = make_pool(&["127.0.0.1:3001", "127.0.0.1:3002", "127.0.0.1:3003"]);
        pool.backends[1].set_healthy(false);

        let rr = RoundRobin::new();
        let addrs: Vec<_> = (0..4).map(|_| rr.select(&pool).unwrap().addr).collect();

        // Only :3001 and :3003 should appear
        for addr in &addrs {
            assert_ne!(*addr, "127.0.0.1:3002".parse().unwrap());
        }
    }

    #[test]
    fn returns_none_when_all_unhealthy() {
        let pool = make_pool(&["127.0.0.1:3001"]);
        pool.backends[0].set_healthy(false);

        let rr = RoundRobin::new();
        assert!(rr.select(&pool).is_none());
    }

    #[test]
    fn returns_none_for_empty_pool() {
        let pool = Pool {
            name: "empty".into(),
            backends: vec![],
        };
        let rr = RoundRobin::new();
        assert!(rr.select(&pool).is_none());
    }
}
