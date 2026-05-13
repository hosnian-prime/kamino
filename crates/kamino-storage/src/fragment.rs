#![allow(clippy::doc_markdown)] // contract doc references are intentionally bare

//! `Fragment` — `tokio::sync::RwLock<Box<dyn StorageEngine>>` wrapper.
//!
//! One `Fragment` per `(dmap, partition)` pair. The `RwLock` keeps reads
//! concurrent while serialising writes (per `docs/07-concurrency.md`).
//!
//! **Owner**: concurrency-layer agent.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::engine::StorageEngine;

/// Per-`(dmap, partition)` storage handle.
#[derive(Debug)]
pub struct Fragment {
    pub(crate) engine: RwLock<Box<dyn StorageEngine>>,
}

impl Fragment {
    /// Wrap an engine in a fresh fragment.
    /// **TODO** (concurrency-layer agent).
    #[must_use]
    pub fn new(engine: Box<dyn StorageEngine>) -> Arc<Self> {
        let _ = engine;
        unimplemented!("filled by concurrency-layer agent")
    }
}
