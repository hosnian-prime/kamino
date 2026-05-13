//! Client-surface error enum (per `docs/08-api-design.md`).

use std::sync::Arc;

/// Convenience alias used across the client surface.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors surfaced by the [`crate::Client`] / [`crate::DMap`] traits.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Key not found in the DMap.
    #[error("key not found")]
    KeyNotFound,

    /// DMap name not registered.
    #[error("dmap not found: {0}")]
    DMapNotFound(String),

    /// `PutOptions { nx: true }` failed because the key already exists.
    #[error("key already exists")]
    KeyAlreadyExists,

    /// `PutOptions { xx: true }` failed because the key did not exist.
    #[error("key does not exist")]
    KeyNotExists,

    /// Lock could not be acquired within the supplied deadline.
    #[error("lock not acquired within deadline")]
    LockNotAcquired,

    /// Unlock failed because the token did not match (or the key was gone).
    #[error("no such lock for this token")]
    NoSuchLock,

    /// Scan cursor invalidated (partition migrated between scan calls).
    #[error("invalid scan cursor: partition migrated; restart the scan")]
    InvalidCursor,

    /// Operation timed out.
    #[error("operation timed out")]
    Timeout,

    /// Generic (de)serialization failure (e.g. UTF-8 / integer parse).
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The current value cannot be parsed as the requested numeric type
    /// (used by `incr` / `decr` / `incr_by_float`).
    #[error("value at key is not a {expected}: {got}")]
    NotANumber { expected: &'static str, got: String },

    /// Caller asked for a feature not yet implemented (e.g. cluster ops in
    /// `EmbeddedClient`).
    #[error("operation not supported in this client mode: {0}")]
    Unsupported(&'static str),

    /// Storage-engine error bubbled up.
    #[error("storage error: {0}")]
    Storage(Arc<kamino_storage::Error>),

    /// Configuration error bubbled up from `kamino-core`.
    #[error("config error: {0}")]
    Config(Arc<kamino_core::Error>),

    /// Invalid argument (e.g. both NX and XX set).
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The remote server is gone or closed the connection mid-flight.
    /// Phase 2: surfaced by `RemoteClient` on TCP disconnect or queue close.
    #[error("server is gone: {0}")]
    ServerGone(String),

    /// Wire-level protocol or transport error from `RemoteClient`.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Typed `-ERR <code> <msg>` error frame returned by the server.
    #[error("server error: {0}")]
    Server(String),

    /// Authentication required by the server, or supplied credentials were
    /// rejected.
    #[error("auth required: {0}")]
    Auth(String),

    /// Server returned `-MOVED <partition> <addr>` (per
    /// `docs/02-consistent-hashing.md`). The remote client refreshes its
    /// routing view and retries once before surfacing this to the caller.
    #[error("MOVED {partition} {addr}")]
    Moved { partition: u32, addr: String },

    /// `Subscription::recv` was called after the underlying registry
    /// dropped this connection's sender (service shut down, peer
    /// connection cleaned up, or the [`crate::PubSub`] handle was
    /// dropped). Phase 7 — `docs/11-pubsub.md` "Delivery Guarantees".
    #[error("pub/sub subscription closed")]
    SubscriptionClosed,
}

impl From<kamino_storage::Error> for Error {
    fn from(value: kamino_storage::Error) -> Self {
        Self::Storage(Arc::new(value))
    }
}

impl From<kamino_core::Error> for Error {
    fn from(value: kamino_core::Error) -> Self {
        Self::Config(Arc::new(value))
    }
}
