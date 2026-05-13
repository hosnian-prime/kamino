//! Configuration profile selector.
//!
//! See `docs/16-config-architecture.md`. `Production` injects safety hard
//! guards that turn the shipped dev defaults into errors rather than warnings.

use serde::{Deserialize, Serialize};

/// Top-level safety knob applied alongside the user's TOML / env / programmatic
/// overrides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    /// Single-node, no replication, no quorum. The shipped defaults.
    Development,
    /// Hard guards: rejects `replica_count == 1`, requires majority quorum.
    Production,
    /// Bring-your-own — neither set of guards runs; you own the validation.
    Custom,
}

impl Default for Profile {
    fn default() -> Self {
        Self::Development
    }
}

impl Profile {
    /// Whether [`Self::Production`] hard guards run on this profile.
    #[must_use]
    pub const fn enforces_production_guards(self) -> bool {
        matches!(self, Self::Production)
    }

    /// Human-readable name for logs and error messages.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::Production => "production",
            Self::Custom => "custom",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_enforces_guards() {
        assert!(Profile::Production.enforces_production_guards());
        assert!(!Profile::Development.enforces_production_guards());
        assert!(!Profile::Custom.enforces_production_guards());
    }
}
