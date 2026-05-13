// Phase 1 contract: agents fill in the field-accessing logic. Allow the
// transient "field never read" warnings until the impls land.
#![allow(dead_code)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::unused_self)]

//! Storage engine surface for Kamino.
//!
//! The crate is structured per `docs/05-storage-engine.md`:
//!
//! - [`StorageEngine`] — async trait every engine implements. The default
//!   implementation is [`RamBlock`].
//! - [`Entry`] — owned key/value record with LWW + idle + TTL metadata.
//!   Binary layout: 29-byte fixed header + key + value (per docs/05).
//! - [`Table`] — append-only memory block with a `TableState` lifecycle
//!   (`ReadWrite` → `ReadOnly` → `Recycled`).
//! - [`RamBlock`] — Bitcask-inspired in-memory engine.
//! - [`Fragment`] — wraps a `Box<dyn StorageEngine>` in a `tokio::sync::RwLock`
//!   so reads stay concurrent (per `docs/07-concurrency.md`).
//! - [`Locker`] — distributed-lock primitive: `parking_lot::Mutex<HashMap>` map
//!   + `tokio::sync::Mutex` per-key `LockEntry` with refcount cleanup.
//! - [`eviction`] — TTL (20-sample probabilistic) + idle + LRU workers.
//! - [`compaction`] — `max_garbage_ratio` driven table rewrite.

pub mod compaction;
pub mod engine;
pub mod entry;
pub mod error;
pub mod eviction;
pub mod fragment;
pub mod locker;
pub mod ramblock;
pub mod table;

pub use engine::{ExportFormat, ScanCallback, StorageEngine, TableState};
pub use entry::{Entry, MAX_KEY_LEN, MAX_VALUE_LEN};
pub use error::{Error, Result};
pub use eviction::{IdleSweeper, LruSampler, TtlSweeper};
pub use fragment::Fragment;
pub use locker::{LockEntry, LockGuard, Locker, LockerError};
pub use ramblock::RamBlock;
pub use table::Table;
