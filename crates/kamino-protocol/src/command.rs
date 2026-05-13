//! Parsed command AST for every Phase 2 command (see
//! `docs/06-network-protocol.md` Command Set).
//!
//! Pub/sub, cluster (`CLUSTER.*`), `DM.LOCK*`, and `INTERNAL.NODE.*` land in
//! later phases; their variants are intentionally absent here so the dispatch
//! exhaustiveness check tells us when they show up.
//!
//! Phase 2 contract: variants and field names below are **frozen**. Adding
//! variants is backwards-compatible; renaming or removing is not.

use bytes::Bytes;

use crate::error::CommandError;
use crate::frame::Frame;
use crate::hello::HelloArgs;

/// All commands the Phase 2 server understands.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    // --- Utility ---
    /// `PING [message]`.
    Ping(Option<Bytes>),
    /// `AUTH password` (legacy) or `AUTH username password` (Redis 6+).
    Auth {
        username: Option<Bytes>,
        password: Bytes,
    },
    /// `HELLO [protover] [AUTH ...] [SETNAME ...]`.
    Hello(HelloArgs),
    /// `QUIT`.
    Quit,
    /// `STATS`.
    Stats,

    // --- DMap data ops ---
    /// `DM.PUT dmap key value [options]`.
    DmPut {
        dmap: Bytes,
        key: Bytes,
        value: Bytes,
        options: PutCommandOptions,
    },
    /// `DM.GET dmap key`.
    DmGet { dmap: Bytes, key: Bytes },
    /// `DM.DEL dmap key [key ...]` — Phase 2 only accepts a single key per
    /// request; the multi-key cross-partition fan-out lands in Phase 4 per
    /// `docs/06-network-protocol.md` §"Multi-Key Operations".
    DmDel { dmap: Bytes, keys: Vec<Bytes> },
    /// `DM.EXPIRE dmap key seconds`.
    DmExpire {
        dmap: Bytes,
        key: Bytes,
        seconds: u64,
    },
    /// `DM.PEXPIRE dmap key milliseconds`.
    DmPexpire {
        dmap: Bytes,
        key: Bytes,
        milliseconds: u64,
    },
    /// `DM.INCR dmap key delta`.
    DmIncr { dmap: Bytes, key: Bytes, delta: i64 },
    /// `DM.DECR dmap key delta`.
    DmDecr { dmap: Bytes, key: Bytes, delta: i64 },
    /// `DM.GETPUT dmap key value`.
    DmGetPut {
        dmap: Bytes,
        key: Bytes,
        value: Bytes,
    },
    /// `DM.INCRBYFLOAT dmap key delta`.
    DmIncrByFloat { dmap: Bytes, key: Bytes, delta: f64 },
    /// `DM.DESTROY dmap`.
    DmDestroy { dmap: Bytes },
    /// `DM.SCAN partID dmap cursor [MATCH pat] [COUNT n]`.
    DmScan {
        partition_id: u32,
        dmap: Bytes,
        cursor: u64,
        options: ScanCommandOptions,
    },
}

/// Optional flags for `DM.PUT`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutCommandOptions {
    /// `EX seconds`.
    pub ex: Option<u64>,
    /// `PX milliseconds`.
    pub px: Option<u64>,
    /// `EXAT unix-seconds`.
    pub exat: Option<u64>,
    /// `PXAT unix-milliseconds`.
    pub pxat: Option<u64>,
    /// `NX` — only set if missing.
    pub nx: bool,
    /// `XX` — only set if present.
    pub xx: bool,
    /// `TS unix-nanos` — client-supplied LWW timestamp override.
    pub timestamp: Option<i64>,
}

/// Optional flags for `DM.SCAN`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanCommandOptions {
    /// `MATCH pat` — glob pattern.
    pub match_pattern: Option<Bytes>,
    /// `COUNT n` — server-side hint.
    pub count: Option<u32>,
}

impl Command {
    /// Parse a top-level [`Frame::Array`] into a typed command.
    ///
    /// Errors:
    /// - [`CommandError::NotAnArray`] if the frame isn't an array of bulk
    ///   strings.
    /// - [`CommandError::UnknownCommand`] for unrecognised verbs.
    /// - [`CommandError::WrongArity`] / [`CommandError::InvalidArgument`]
    ///   for shape mismatches.
    pub fn parse(frame: Frame) -> Result<Self, CommandError> {
        let _ = frame;
        unimplemented!("filled by kamino-protocol agent")
    }

    /// Encode this command into a `Frame::Array` suitable for sending over
    /// the wire (client side). Inverse of [`Self::parse`].
    pub fn to_frame(&self) -> Frame {
        unimplemented!("filled by kamino-protocol agent")
    }
}
