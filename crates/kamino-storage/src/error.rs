//! Storage-engine error surface.

/// Convenience alias used inside `kamino-storage`.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors surfaced by the storage layer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Key exceeded the 255-byte limit (`u8` length field in the header).
    #[error("key too large: {got} bytes (max {max})")]
    KeyTooLarge { got: usize, max: usize },

    /// Value exceeded the 4 GiB limit (`u32` length field in the header).
    #[error("value too large: {got} bytes (max {max})")]
    ValueTooLarge { got: usize, max: usize },

    /// Decoder ran past the end of the input buffer.
    #[error("entry buffer truncated: need {need} more bytes")]
    Truncated { need: usize },

    /// Decoded bytes don't form a valid entry (e.g. value length overflows
    /// the buffer, or key length is inconsistent).
    #[error("corrupted entry: {0}")]
    Corruption(String),

    /// `scan_regex_match` pattern failed to compile.
    #[error("invalid regex pattern: {0}")]
    Regex(String),

    /// `import` payload didn't match the expected format.
    #[error("import format error: {0}")]
    ImportFormat(String),

    /// The engine reached its configured capacity.
    #[error("engine is full")]
    EngineFull,

    /// Error bubbled up from `kamino-core`.
    #[error(transparent)]
    Core(#[from] kamino_core::Error),
}
