//! RESP2 + RESP3 frame definitions.
//!
//! `BulkString` wraps `bytes::Bytes` so the codec can hand out zero-copy
//! views into the receive buffer. Outgoing frames built by the client side
//! also accept owned `Vec<u8>` via `From`.

use bytes::Bytes;

/// Bulk-string payload. `None` represents the explicit "null bulk string"
/// (`$-1\r\n` in RESP2).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BulkString(pub Option<Bytes>);

impl BulkString {
    /// Construct from an owned byte buffer.
    #[must_use]
    pub fn from_bytes(b: impl Into<Bytes>) -> Self {
        Self(Some(b.into()))
    }

    /// Construct the explicit null bulk.
    #[must_use]
    pub const fn null() -> Self {
        Self(None)
    }

    /// Borrow the inner bytes if present.
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.0.as_deref()
    }

    /// `true` for the null bulk.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        self.0.is_none()
    }
}

impl From<&[u8]> for BulkString {
    fn from(b: &[u8]) -> Self {
        Self(Some(Bytes::copy_from_slice(b)))
    }
}

impl From<Vec<u8>> for BulkString {
    fn from(v: Vec<u8>) -> Self {
        Self(Some(Bytes::from(v)))
    }
}

impl From<&str> for BulkString {
    fn from(s: &str) -> Self {
        Self::from(s.as_bytes())
    }
}

impl From<String> for BulkString {
    fn from(s: String) -> Self {
        Self(Some(Bytes::from(s.into_bytes())))
    }
}

/// One RESP frame. RESP3-only variants (`Map`, `Set`, `Push`, `Boolean`,
/// `Double`, `BigNumber`, and `Null`) MUST NOT be emitted on a RESP2
/// connection — the codec returns [`crate::ProtocolError::Resp3Required`]
/// if you try.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// `+OK\r\n` etc.
    SimpleString(String),
    /// `-ERR ...\r\n`. The leading code (e.g. `"ERR"`, `"MOVED"`) is the
    /// first whitespace-separated token; the rest is human-readable.
    Error(String),
    /// `:123\r\n`.
    Integer(i64),
    /// `$5\r\nhello\r\n` or `$-1\r\n` for null.
    Bulk(BulkString),
    /// `*3\r\n...` or `*-1\r\n` for null array.
    Array(Option<Vec<Frame>>),
    /// RESP3 `%2\r\n...` map. Pairs preserve insertion order.
    Map(Vec<(Frame, Frame)>),
    /// RESP3 `~3\r\n...` set.
    Set(Vec<Frame>),
    /// RESP3 `>3\r\n...` push frame. Used for pub/sub delivery.
    Push(Vec<Frame>),
    /// RESP3 `#t` / `#f`.
    Boolean(bool),
    /// RESP3 `,3.14\r\n`.
    Double(f64),
    /// RESP3 `_\r\n`.
    Null,
    /// RESP3 `(123...\r\n`. Carried as the decoded textual form.
    BigNumber(String),
}

impl Frame {
    /// Convenience: `+OK\r\n`.
    #[must_use]
    pub fn ok() -> Self {
        Self::SimpleString("OK".into())
    }

    /// Convenience: build a generic `-ERR <msg>` error frame.
    pub fn error(msg: impl Into<String>) -> Self {
        Self::Error(format!("ERR {}", msg.into()))
    }

    /// `true` if this is a RESP3-only frame type.
    #[must_use]
    pub const fn requires_resp3(&self) -> bool {
        matches!(
            self,
            Self::Map(_)
                | Self::Set(_)
                | Self::Push(_)
                | Self::Boolean(_)
                | Self::Double(_)
                | Self::Null
                | Self::BigNumber(_)
        )
    }
}
