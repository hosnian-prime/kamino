//! Typed error surface for `kamino-core`.

use crate::mode::Mode;

/// Convenience alias used across the workspace.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors surfaced by configuration loading and validation in Phase 0.
///
/// Subsystem-specific errors live in their own crates and are composed at the
/// umbrella layer; this enum stays focused on what `kamino-core` itself owns.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Generic configuration validation failure with a human-readable reason.
    #[error("invalid config: {0}")]
    Config(String),

    /// `Profile::Production` requires at least one backup replica.
    /// See `docs/09-configuration.md#production-recommended-defaults`.
    #[error(
        "production profile requires replica_count >= 2, got {got} \
         (override Profile::Custom if you accept silent data loss)"
    )]
    ProductionReplicaCount { got: u32 },

    /// `Profile::Production` requires `member_count_quorum` to form a
    /// strict majority. See `docs/04-replication.md`.
    #[error(
        "production profile requires member_count_quorum to form a majority \
         (>= replica_count/2 + 1), got {got}"
    )]
    ProductionQuorum { got: u32 },

    /// A TOML section is incompatible with the selected [`Mode`].
    /// Example: `[discovery]` set under `Mode::EmbeddedSolo`.
    #[error("section [{section}] is not relevant for mode {mode:?}")]
    IrrelevantSection { section: &'static str, mode: Mode },

    /// Attempt to change a bootstrap-only setting via reload.
    /// See `docs/16-config-architecture.md#reload-discipline`.
    #[error("cannot change {0} on a running node (bootstrap-only)")]
    BootstrapImmutable(&'static str),

    /// TOML parse failure when loading a config file.
    #[error("failed to parse TOML config: {0}")]
    TomlParse(#[from] toml::de::Error),

    /// I/O failure when reading a config file.
    #[error("config I/O error: {0}")]
    Io(#[from] std::io::Error),
}
