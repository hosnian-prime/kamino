//! Public option / response value types (per `docs/08-api-design.md`).

use std::time::Duration;

use kamino_core::Clock;
use kamino_core::DMapConfig;

use crate::error::{Error, Result};

/// Options applied to a single `put` call.
#[derive(Debug, Default, Clone)]
pub struct PutOptions {
    /// TTL in seconds (relative).
    pub ex: Option<u64>,
    /// TTL in milliseconds (relative).
    pub px: Option<u64>,
    /// Absolute expiry, Unix seconds.
    pub exat: Option<u64>,
    /// Absolute expiry, Unix milliseconds.
    pub pxat: Option<u64>,
    /// Only set if the key does NOT already exist.
    pub nx: bool,
    /// Only set if the key ALREADY exists.
    pub xx: bool,
    /// Override the server-assigned LWW timestamp (Unix nanoseconds).
    /// `None` lets the partition primary stamp the entry on acceptance.
    pub timestamp: Option<i64>,
}

impl PutOptions {
    /// Validate mutually exclusive flags.
    pub fn validate(&self) -> Result<()> {
        if self.nx && self.xx {
            return Err(Error::InvalidArgument(
                "nx and xx are mutually exclusive".into(),
            ));
        }
        let count = [
            self.ex.is_some(),
            self.px.is_some(),
            self.exat.is_some(),
            self.pxat.is_some(),
        ]
        .into_iter()
        .filter(|b| *b)
        .count();
        if count > 1 {
            return Err(Error::InvalidArgument(
                "at most one of ex/px/exat/pxat may be set".into(),
            ));
        }
        Ok(())
    }

    /// Resolve the effective absolute TTL (Unix nanos) for this put. `0` means
    /// "no expiry". `default_ttl` is the DMap's configured TTL (or `None` if
    /// no per-DMap default).
    #[allow(clippy::option_if_let_else)] // chain is clearer than nested map_or_else
    pub fn resolve_ttl_nanos(
        &self,
        clock: &dyn Clock,
        default_ttl: Option<Duration>,
    ) -> Result<i64> {
        self.validate()?;
        let now_nanos = micros_to_nanos_i64(clock.now_micros());
        let nanos = if let Some(s) = self.ex {
            let dur = i64::try_from(s.saturating_mul(1_000_000_000)).unwrap_or(i64::MAX);
            now_nanos.saturating_add(dur)
        } else if let Some(ms) = self.px {
            let dur = i64::try_from(ms.saturating_mul(1_000_000)).unwrap_or(i64::MAX);
            now_nanos.saturating_add(dur)
        } else if let Some(s) = self.exat {
            i64::try_from(s.saturating_mul(1_000_000_000)).unwrap_or(i64::MAX)
        } else if let Some(ms) = self.pxat {
            i64::try_from(ms.saturating_mul(1_000_000)).unwrap_or(i64::MAX)
        } else if let Some(d) = default_ttl {
            if d.is_zero() {
                0
            } else {
                let nanos = i64::try_from(d.as_nanos()).unwrap_or(i64::MAX);
                now_nanos.saturating_add(nanos)
            }
        } else {
            0
        };
        Ok(nanos)
    }
}

/// Convert Unix microseconds to a saturating signed-nanos value.
pub(crate) fn micros_to_nanos_i64(micros: u64) -> i64 {
    i64::try_from(micros)
        .map(|m| m.saturating_mul(1_000))
        .unwrap_or(i64::MAX)
}

/// Response from a successful `get`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetResponse {
    /// Stored value.
    pub value: Vec<u8>,
    /// LWW timestamp the entry was written with (Unix nanoseconds).
    pub timestamp: i64,
    /// Remaining TTL, if the entry had one.
    pub ttl: Option<Duration>,
}

impl GetResponse {
    /// Decode the value as UTF-8.
    pub fn as_str(&self) -> Result<&str> {
        std::str::from_utf8(&self.value).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Parse the value as a signed 64-bit integer.
    pub fn as_i64(&self) -> Result<i64> {
        self.as_str()?
            .parse::<i64>()
            .map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Parse the value as a 64-bit float.
    pub fn as_f64(&self) -> Result<f64> {
        self.as_str()?
            .parse::<f64>()
            .map_err(|e| Error::Serialization(e.to_string()))
    }
}

/// Per-DMap options at creation time. Mirrors `kamino_core::DMapConfig` but
/// shaped for programmatic use.
#[derive(Debug, Default, Clone)]
pub struct DMapOptions {
    /// `None` = inherit cluster default. `Some(Duration::ZERO)` = no idle eviction.
    pub max_idle_duration: Option<Duration>,
    /// Default TTL applied to entries that don't override it via `PutOptions`.
    pub ttl: Option<Duration>,
    pub max_keys: Option<u64>,
    pub max_inuse: Option<u64>,
    pub lru_samples: Option<u32>,
    /// If `Some(EvictionPolicy::Lru)`, writers run the LRU sampler when limits
    /// are exceeded.
    pub eviction_policy: Option<kamino_core::EvictionPolicy>,
}

impl DMapOptions {
    /// Build from a parsed `DMapConfig`.
    #[must_use]
    pub fn from_config(cfg: &DMapConfig) -> Self {
        Self {
            max_idle_duration: cfg.max_idle_duration,
            ttl: cfg.ttl,
            max_keys: cfg.max_keys,
            max_inuse: cfg.max_inuse.map(|b| b.0),
            lru_samples: cfg.lru_samples,
            eviction_policy: cfg.eviction_policy,
        }
    }
}
