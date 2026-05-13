#![allow(clippy::type_complexity)] // the registry shape is the public contract

//! `Locker` — distributed-lock building block.
//!
//! Per `docs/07-concurrency.md`:
//!
//! - The outer `HashMap<(dmap, key), Arc<LockEntry>>` is guarded by a
//!   `parking_lot::Mutex` (sync, **never held across `.await`**).
//! - Each `LockEntry` holds a `tokio::sync::Mutex<()>` that *can* be held
//!   across `.await` (it's the actual lock waiters contend on).
//! - Refcount-based cleanup: when the last waiter releases an entry the
//!   `Arc` count drops and the slot is reaped from the outer map.
//!
//! **Owner**: concurrency-layer agent.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex as SyncMutex;
use tokio::sync::Mutex as AsyncMutex;

/// One contended lock — the inner mutex is what waiters serialise on.
#[derive(Debug)]
pub struct LockEntry {
    pub(crate) mutex: AsyncMutex<()>,
}

/// Registry of currently-held locks per `(dmap, key)`.
#[derive(Debug, Default)]
pub struct Locker {
    pub(crate) locks: SyncMutex<HashMap<(String, Vec<u8>), Arc<LockEntry>>>,
}

impl Locker {
    /// Build an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}
