//! Typed error surfaces for the codec and the command parser.

/// Codec-layer errors. Surfaced via `tokio_util::codec::{Encoder, Decoder}`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// The buffer ended mid-frame. Caller should read more bytes and retry.
    /// This variant is intentionally **not** an error from the caller's POV
    /// — `Decoder::decode` returns `Ok(None)` in this case; this enum value
    /// exists so internal parsers can return it before the codec converts.
    #[error("incomplete frame")]
    Incomplete,

    /// The wire bytes were malformed (bad prefix, missing CRLF, invalid
    /// integer encoding, etc.).
    #[error("invalid encoding: {0}")]
    InvalidEncoding(String),

    /// A bulk string declared a size larger than the configured maximum.
    #[error("bulk too large: {size} bytes (max {max})")]
    BulkTooLarge { size: u64, max: u64 },

    /// An inline command was rejected (inline commands are not supported
    /// outside of `PING\r\n`).
    #[error("inline command not allowed: {0}")]
    InlineCommandRejected(&'static str),

    /// A RESP3-only frame type was emitted while the connection is still
    /// RESP2; programmer error in the server.
    #[error("frame type {0} requires RESP3 — upgrade via HELLO 3 first")]
    Resp3Required(&'static str),

    /// Underlying I/O failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Parser-layer errors. Returned by [`crate::Command::parse`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CommandError {
    /// The frame wasn't a `Frame::Array` of bulk strings (the only shape
    /// clients use to send commands).
    #[error("expected array of bulk strings")]
    NotAnArray,

    /// The command verb (first array element) is unknown.
    #[error("unknown command: {0}")]
    UnknownCommand(String),

    /// Wrong number of arguments for `command`.
    #[error("wrong number of arguments for {command}: expected {expected}, got {got}")]
    WrongArity {
        command: &'static str,
        expected: ArityHint,
        got: usize,
    },

    /// An argument couldn't be parsed (e.g. not a valid integer).
    #[error("invalid argument for {command}: {reason}")]
    InvalidArgument {
        command: &'static str,
        reason: String,
    },

    /// Mutually exclusive options were both supplied (e.g. NX + XX).
    #[error("conflicting options for {command}: {detail}")]
    ConflictingOptions {
        command: &'static str,
        detail: &'static str,
    },
}

/// Arity description used in error messages.
#[derive(Debug, Clone, Copy)]
pub enum ArityHint {
    /// Command takes exactly this many arguments (excluding the verb).
    Exactly(usize),
    /// Command takes at least this many arguments.
    AtLeast(usize),
    /// Command takes between `min` and `max` arguments (inclusive).
    Range { min: usize, max: usize },
}

impl std::fmt::Display for ArityHint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exactly(n) => write!(f, "{n}"),
            Self::AtLeast(n) => write!(f, "at least {n}"),
            Self::Range { min, max } => write!(f, "{min}..={max}"),
        }
    }
}
