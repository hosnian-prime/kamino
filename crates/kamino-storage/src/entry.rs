// Phase 1 contract: codec bodies are `unimplemented!()` until the
// storage-internals agent fills them in. Suppress the predictable lints
// about unused params and "could be const" until then.
#![allow(
    clippy::needless_pass_by_ref_mut,
    clippy::missing_const_for_fn,
    clippy::doc_markdown
)]

//! `Entry` — the on-disk record stored by every `StorageEngine`.
//!
//! Binary layout (per `docs/05-storage-engine.md`):
//!
//! ```text
//! ┌───────────────┬──────────┬─────────┬───────────────┬──────────────────┬────────────────┬──────────────┐
//! │ KEY_LENGTH(1B)│ KEY(var) │ TTL(8B) │ TIMESTAMP(8B) │ LAST_ACCESS(8B)  │ VAL_LEN(4B)    │ VALUE(var)   │
//! └───────────────┴──────────┴─────────┴───────────────┴──────────────────┴────────────────┴──────────────┘
//! ```
//!
//! All integers are little-endian. `ttl_nanos`, `timestamp_nanos` and
//! `last_access_nanos` are signed `i64` so the codec can represent both
//! "before epoch" times (for tests) and the sentinel value `0` ≡ "no TTL".
//!
//! **Contract**:
//! - `ttl_nanos == 0` → entry never expires.
//! - `ttl_nanos > 0` → absolute Unix-nanos expiry deadline. The choice of
//!   absolute (rather than duration-from-write) keeps eviction stateless
//!   under compaction — the deadline doesn't shift when entries are copied
//!   into a new table.
//! - `timestamp_nanos` is the primary's stamp used for LWW conflict
//!   resolution. Monotonic per primary (see `docs/04-replication.md`).
//! - `last_access_nanos` is bumped on every read/write/touch and drives
//!   both idle and LRU eviction.
//!
//! This module defines the type and codec **signatures** (Phase 1 contract).
//! Encoder/decoder bodies are filled in by the storage-internals owner.

/// Fixed overhead per entry: `key_len(1) + ttl(8) + ts(8) + last_access(8) + val_len(4)`.
pub const HEADER_OVERHEAD: usize = 1 + 8 + 8 + 8 + 4;

/// Maximum key size in bytes (capped by the `u8` length field).
pub const MAX_KEY_LEN: usize = u8::MAX as usize;

/// Maximum value size in bytes (capped by the `u32` length field).
pub const MAX_VALUE_LEN: usize = u32::MAX as usize;

/// Sentinel meaning "no expiry".
pub const NO_TTL: i64 = 0;

/// One stored record, owned (engine clones from its memory block on read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Raw key bytes (max [`MAX_KEY_LEN`]).
    pub key: Vec<u8>,
    /// Absolute expiry deadline in Unix nanoseconds. `0` ≡ never expires.
    pub ttl_nanos: i64,
    /// LWW timestamp in Unix nanoseconds.
    pub timestamp_nanos: i64,
    /// Last access timestamp in Unix nanoseconds (idle + LRU eviction).
    pub last_access_nanos: i64,
    /// Raw value bytes.
    pub value: Vec<u8>,
}

impl Entry {
    /// Serialized length: [`HEADER_OVERHEAD`] + `key.len()` + `value.len()`.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        HEADER_OVERHEAD + self.key.len() + self.value.len()
    }

    /// `true` if `ttl_nanos != 0` and the deadline is in the past.
    #[must_use]
    pub fn is_expired(&self, now_nanos: i64) -> bool {
        self.ttl_nanos != NO_TTL && now_nanos >= self.ttl_nanos
    }

    /// Encode into `buf` (appended). Owner: storage-internals agent.
    ///
    /// Errors with [`crate::Error::KeyTooLarge`] / [`crate::Error::ValueTooLarge`]
    /// if the limits are exceeded.
    pub fn encode_into(&self, buf: &mut Vec<u8>) -> crate::Result<()> {
        let _ = buf;
        unimplemented!("filled by storage-internals agent")
    }

    /// Decode the entry that starts at `&buf[0]`. Returns the entry and the
    /// number of bytes consumed. Owner: storage-internals agent.
    pub fn decode(buf: &[u8]) -> crate::Result<(Self, usize)> {
        let _ = buf;
        unimplemented!("filled by storage-internals agent")
    }
}
