//! Load-balancing routing strategies.

pub mod least_connections;
pub mod round_robin;

use std::sync::Arc;

use crate::state::{Backend, Pool};

pub use least_connections::LeastConnections;
pub use round_robin::RoundRobin;

/// Trait for selecting a backend server from a pool.
///
/// Implementations must be `Send + Sync` because routers are shared
/// across all worker tasks for a given frontend. Each call to [`select`]
/// should be O(n) in the number of backends at worst.
///
/// [`select`]: Router::select
pub trait Router: Send + Sync + 'static {
    fn select(&self, pool: &Pool) -> Option<Arc<Backend>>;
}
