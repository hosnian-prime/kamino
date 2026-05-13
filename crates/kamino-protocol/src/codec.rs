//! `tokio_util::codec::{Encoder, Decoder}` implementation for RESP2/3.
//!
//! The codec is **stateful** in exactly one dimension: the negotiated
//! protocol version. RESP3-only frame types are rejected on encode while
//! the version is `Resp2`. Decode accepts only RESP2 framing until upgraded;
//! after `HELLO 3` the decoder also recognises RESP3-specific prefixes.

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::ProtocolError;
use crate::frame::Frame;

/// Negotiated wire-protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolVersion {
    /// RESP2 baseline. Default for a freshly-accepted connection.
    #[default]
    Resp2,
    /// Upgraded after a successful `HELLO 3`.
    Resp3,
}

/// Maximum bulk-string size accepted by `decode`. Defends against a
/// malicious client claiming `$9223372036854775807\r\n`.
pub const DEFAULT_MAX_BULK_SIZE: u64 = 512 * 1024 * 1024; // 512 MiB

/// Maximum array length (number of elements). Same rationale as above.
pub const DEFAULT_MAX_ARRAY_LEN: u64 = 1_048_576; // 2^20

/// RESP codec implementing `tokio_util::codec::{Encoder, Decoder}`.
#[derive(Debug)]
pub struct RespCodec {
    /// Current negotiated version. Server flips this after a successful
    /// `HELLO 3`; client-side codecs typically set it themselves.
    pub(crate) version: ProtocolVersion,
    /// Maximum acceptable bulk size on decode.
    pub(crate) max_bulk_size: u64,
    /// Maximum acceptable array length on decode.
    pub(crate) max_array_len: u64,
}

impl Default for RespCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl RespCodec {
    /// Construct a fresh codec in `Resp2` mode with default limits.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            version: ProtocolVersion::Resp2,
            max_bulk_size: DEFAULT_MAX_BULK_SIZE,
            max_array_len: DEFAULT_MAX_ARRAY_LEN,
        }
    }

    /// Currently negotiated protocol version.
    #[must_use]
    pub const fn version(&self) -> ProtocolVersion {
        self.version
    }

    /// Mark the codec as RESP3 (call after a successful HELLO 3).
    pub const fn upgrade_to_resp3(&mut self) {
        self.version = ProtocolVersion::Resp3;
    }

    /// Set the maximum bulk size accepted on decode.
    pub const fn set_max_bulk_size(&mut self, bytes: u64) {
        self.max_bulk_size = bytes;
    }

    /// Set the maximum array length accepted on decode.
    pub const fn set_max_array_len(&mut self, len: u64) {
        self.max_array_len = len;
    }
}

impl Decoder for RespCodec {
    type Item = Frame;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
        let _ = src;
        unimplemented!("filled by kamino-protocol agent")
    }
}

impl Encoder<Frame> for RespCodec {
    type Error = ProtocolError;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), ProtocolError> {
        let _ = (frame, dst);
        unimplemented!("filled by kamino-protocol agent")
    }
}

// Allow encoding by reference so the server can re-use frames without moving.
impl Encoder<&Frame> for RespCodec {
    type Error = ProtocolError;

    fn encode(&mut self, frame: &Frame, dst: &mut BytesMut) -> Result<(), ProtocolError> {
        let _ = (frame, dst);
        unimplemented!("filled by kamino-protocol agent")
    }
}
