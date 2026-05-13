//! `DMap` trait: the single-partition op surface (Phase 1 scope).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::cursor::{ScanCursor, ScanOptions};
use crate::error::Result;
use crate::lock::LockContext;
use crate::types::{GetResponse, PutOptions};

/// Distributed map handle.
///
/// In Phase 1 (`EmbeddedSolo`) there is one fragment per DMap and "the
/// partition primary" is always the local process. The trait signatures
/// already match what the multi-node implementation will need.
#[async_trait]
pub trait DMap: Send + Sync + std::fmt::Debug {
    /// Stable name (the same string passed to `Client::new_dmap`).
    fn name(&self) -> &str;

    /// Store a key/value pair.
    async fn put(&self, key: &str, value: &[u8], options: PutOptions) -> Result<()>;

    /// Retrieve a value. Returns [`crate::Error::KeyNotFound`] when missing.
    async fn get(&self, key: &str) -> Result<GetResponse>;

    /// Delete a single key. Returns `true` if a live entry was removed.
    async fn delete(&self, key: &str) -> Result<bool>;

    /// Atomically increment an integer counter by `delta`. Creates a fresh
    /// counter at `0 + delta` if the key did not exist.
    async fn incr(&self, key: &str, delta: i64) -> Result<i64>;

    /// Atomically decrement an integer counter by `delta`.
    async fn decr(&self, key: &str, delta: i64) -> Result<i64>;

    /// Atomically increment a float by `delta`. Creates the entry at `delta`
    /// if missing.
    async fn incr_by_float(&self, key: &str, delta: f64) -> Result<f64>;

    /// Replace the value for `key` and return the previous one (if any).
    async fn get_put(&self, key: &str, value: &[u8]) -> Result<Option<GetResponse>>;

    /// Update only the TTL of a key. Returns [`crate::Error::KeyNotFound`] if
    /// the key is missing.
    async fn expire(&self, key: &str, duration: Duration) -> Result<()>;

    /// Acquire a lock on `key` (no auto-expiry). See safety note in
    /// `docs/10-distributed-locking.md`.
    async fn lock(self: Arc<Self>, key: &str, deadline: Duration) -> Result<LockContext>;

    /// Acquire a lock on `key` with auto-expiring lease. **Recommended.**
    async fn lock_with_timeout(
        self: Arc<Self>,
        key: &str,
        lease: Duration,
        deadline: Duration,
    ) -> Result<LockContext>;

    /// Single-partition cursor scan.
    async fn scan(&self, partition_id: u32, options: ScanOptions) -> Result<Box<dyn ScanCursor>>;

    /// Delete every entry in this DMap.
    async fn destroy(&self) -> Result<()>;

    // ---- internal hooks for LockContext --------------------------------------------------

    /// Release a previously-acquired lock. Token-checked; do not call directly.
    #[doc(hidden)]
    async fn unlock_internal(&self, key: &str, token: &[u8]) -> Result<()>;

    /// Extend the lease on a previously-acquired lock. Token-checked.
    #[doc(hidden)]
    async fn lease_internal(&self, key: &str, token: &[u8], duration: Duration) -> Result<()>;
}
