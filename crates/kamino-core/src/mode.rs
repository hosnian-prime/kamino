//! Deployment mode selector.
//!
//! See `docs/16-config-architecture.md`. The variants drive which TOML
//! sections are accepted and which validators run.

use serde::{Deserialize, Serialize};

/// How the runtime is wired up. Selected by `mode = "..."` in TOML or by
/// the corresponding `Kamino::embedded()` / `Kamino::serve()` builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Standalone server binary listening on the RESP port.
    Standalone,
    /// In-process single-node cache. No SWIM, no replication, no inter-node.
    EmbeddedSolo,
    /// In-process cache that participates in a multi-node cluster.
    EmbeddedClustered,
}

impl Mode {
    /// `true` if this mode needs SWIM, routing, and a discovery plugin.
    #[must_use]
    pub const fn requires_cluster(self) -> bool {
        matches!(self, Self::Standalone | Self::EmbeddedClustered)
    }

    /// `true` if this mode runs in-process (no TCP listener required).
    #[must_use]
    pub const fn is_embedded(self) -> bool {
        matches!(self, Self::EmbeddedSolo | Self::EmbeddedClustered)
    }

    /// Human-readable name for logs and error messages.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::EmbeddedSolo => "embedded_solo",
            Self::EmbeddedClustered => "embedded_clustered",
        }
    }
}

impl Default for Mode {
    /// Defaults to `EmbeddedSolo` — the safest no-network mode for tests
    /// and a single-process cache.
    fn default() -> Self {
        Self::EmbeddedSolo
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_cluster_modes() {
        assert!(Mode::Standalone.requires_cluster());
        assert!(Mode::EmbeddedClustered.requires_cluster());
        assert!(!Mode::EmbeddedSolo.requires_cluster());
    }

    #[test]
    fn classifies_embedded_modes() {
        assert!(Mode::EmbeddedSolo.is_embedded());
        assert!(Mode::EmbeddedClustered.is_embedded());
        assert!(!Mode::Standalone.is_embedded());
    }

    /// TOML's top-level value must be a table, so the round-trip test wraps
    /// the enum in a struct.
    #[test]
    fn round_trip_through_toml() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
        struct Wrapper {
            mode: Mode,
        }
        for m in [
            Mode::Standalone,
            Mode::EmbeddedSolo,
            Mode::EmbeddedClustered,
        ] {
            let s = toml::to_string(&Wrapper { mode: m }).unwrap();
            let parsed: Wrapper = toml::from_str(&s).unwrap();
            assert_eq!(parsed.mode, m);
        }
    }
}
