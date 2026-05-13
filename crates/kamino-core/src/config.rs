//! Configuration structure and validation.
//!
//! Sections mirror `docs/09-configuration.md` 1:1. Validation lives in
//! [`Config::validate`]; per-profile hard guards run alongside the
//! per-section validators (see `docs/16-config-architecture.md`).

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::mode::Mode;
use crate::profile::Profile;

pub mod env;

// ---------------------------------------------------------------------------
// Top-level Config
// ---------------------------------------------------------------------------

/// Complete runtime configuration. One [`Config`] per node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Deployment mode. Defaults to `EmbeddedSolo`.
    pub mode: Mode,
    /// Safety profile. Defaults to `Development`.
    pub profile: Profile,

    pub core: CoreConfig,
    pub network: NetworkConfig,
    pub auth: AuthConfig,
    pub discovery: DiscoveryConfig,
    pub swim: SwimConfig,
    pub storage: StorageConfig,
    pub eviction: EvictionConfig,
    pub balancer: BalancerConfig,
    pub routing: RoutingConfig,
    pub events: EventsConfig,
    pub hash: HashConfig,

    /// Per-DMap overrides keyed by name. Serializes back to the
    /// `[[dmaps]]` array-of-tables form so a round-trip is lossless.
    #[serde(
        rename = "dmaps",
        default,
        deserialize_with = "deserialize_dmaps",
        serialize_with = "serialize_dmaps"
    )]
    pub dmaps: HashMap<String, DMapConfig>,
}

impl Config {
    /// Parse a TOML file from disk and validate it.
    pub fn from_toml_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Self::from_toml_str(&text)
    }

    /// Parse a TOML string and validate it.
    pub fn from_toml_str(text: &str) -> Result<Self> {
        let cfg: Self = toml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Run validation rules.
    ///
    /// Order: structural (every section) → cross-section invariants →
    /// mode-relevance → profile hard guards. The first failure short-circuits;
    /// that's intentional (fix the first thing, re-run).
    pub fn validate(&self) -> Result<()> {
        self.core.validate()?;
        self.network.validate()?;
        self.discovery.validate(self.mode)?;
        self.swim.validate()?;
        self.storage.validate()?;
        self.eviction.validate()?;
        self.balancer.validate()?;
        self.routing.validate()?;
        self.hash.validate()?;
        for (name, dmap) in &self.dmaps {
            dmap.validate(name)?;
        }
        self.validate_cross_section()?;
        self.validate_mode_consistency()?;
        if self.profile.enforces_production_guards() {
            self.validate_production_guards()?;
        }
        Ok(())
    }

    /// Cross-section invariants that span multiple sub-configs.
    fn validate_cross_section(&self) -> Result<()> {
        if self.core.write_quorum > self.core.replica_count {
            return Err(Error::Config(format!(
                "write_quorum ({}) cannot exceed replica_count ({})",
                self.core.write_quorum, self.core.replica_count,
            )));
        }
        if self.core.read_quorum > self.core.replica_count {
            return Err(Error::Config(format!(
                "read_quorum ({}) cannot exceed replica_count ({})",
                self.core.read_quorum, self.core.replica_count,
            )));
        }
        Ok(())
    }

    /// Reject TOML sections that don't make sense for the selected mode.
    /// See `docs/16-config-architecture.md`.
    fn validate_mode_consistency(&self) -> Result<()> {
        if self.mode == Mode::EmbeddedSolo {
            if !self.discovery.peers.is_empty() {
                return Err(Error::IrrelevantSection {
                    section: "discovery.peers",
                    mode: self.mode,
                });
            }
            if self.core.replica_count > 1 {
                return Err(Error::Config(format!(
                    "mode = embedded_solo cannot have replica_count > 1 (got {})",
                    self.core.replica_count,
                )));
            }
            if self.core.member_count_quorum > 1 {
                return Err(Error::Config(format!(
                    "mode = embedded_solo cannot have member_count_quorum > 1 (got {})",
                    self.core.member_count_quorum,
                )));
            }
        }
        Ok(())
    }

    /// [`Profile::Production`] hard guards (see `docs/09-configuration.md`).
    #[allow(clippy::missing_const_for_fn)] // future-proof: errors may carry allocations
    fn validate_production_guards(&self) -> Result<()> {
        if self.core.replica_count < 2 {
            return Err(Error::ProductionReplicaCount {
                got: self.core.replica_count,
            });
        }
        // Majority of replica_count: ceil(replica_count / 2) + (replica_count % 2 == 0)
        // For replica_count=2, majority=2. For 3, majority=2. For 5, majority=3.
        let majority = self.core.replica_count / 2 + 1;
        if self.core.member_count_quorum < majority {
            return Err(Error::ProductionQuorum {
                got: self.core.member_count_quorum,
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Env overlay convenience method (Phase 2).
// ---------------------------------------------------------------------------

impl Config {
    /// Apply `KAMINO_*` environment-variable overrides on top of this config
    /// (per `docs/16-config-architecture.md` — double-underscore separator
    /// between section and field, lower-cased). Validates after merging.
    pub fn with_env_overlay(mut self) -> Result<Self> {
        env::overlay_from_env(&mut self)?;
        Ok(self)
    }
}

// ---------------------------------------------------------------------------
// [core]
// ---------------------------------------------------------------------------

/// `[core]` section: data shape, replication and quorum.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CoreConfig {
    /// Hash-ring partitions. Should be prime. **Immutable after first write.**
    /// See `docs/02-consistent-hashing.md`.
    pub partition_count: u32,
    /// Replicas per partition (1 = primary only).
    pub replica_count: u32,
    /// Minimum successful writes before acknowledging. `<= replica_count`.
    pub write_quorum: u32,
    /// Minimum reads before returning. `<= replica_count`.
    pub read_quorum: u32,
    /// Minimum cluster members to accept operations.
    pub member_count_quorum: u32,
    /// Synchronous or asynchronous replication.
    pub replication_mode: ReplicationMode,
    /// Compare replicas on read and repair stale copies.
    pub read_repair: bool,
    /// Bounded-load consistent hashing factor (Mirrokni 2016). `>= 1.0`.
    pub load_factor: f64,
    /// Virtual nodes per physical member on the consistent-hash ring. More
    /// virtual nodes smooth the partition distribution at the cost of routing
    /// table memory. See `docs/02-consistent-hashing.md`.
    pub virtual_nodes_per_member: u32,
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            partition_count: 271,
            replica_count: 1,
            write_quorum: 1,
            read_quorum: 1,
            member_count_quorum: 1,
            replication_mode: ReplicationMode::Sync,
            read_repair: false,
            load_factor: 1.25,
            virtual_nodes_per_member: 20,
        }
    }
}

impl CoreConfig {
    fn validate(&self) -> Result<()> {
        if self.partition_count == 0 {
            return Err(Error::Config("partition_count must be > 0".into()));
        }
        if self.replica_count == 0 {
            return Err(Error::Config("replica_count must be >= 1".into()));
        }
        if self.write_quorum == 0 {
            return Err(Error::Config("write_quorum must be >= 1".into()));
        }
        if self.read_quorum == 0 {
            return Err(Error::Config("read_quorum must be >= 1".into()));
        }
        if self.member_count_quorum == 0 {
            return Err(Error::Config("member_count_quorum must be >= 1".into()));
        }
        if self.load_factor < 1.0 {
            return Err(Error::Config(format!(
                "load_factor must be >= 1.0, got {}",
                self.load_factor,
            )));
        }
        if !self.load_factor.is_finite() {
            return Err(Error::Config("load_factor must be finite".into()));
        }
        if self.virtual_nodes_per_member == 0 {
            return Err(Error::Config(
                "virtual_nodes_per_member must be >= 1".into(),
            ));
        }
        Ok(())
    }
}

/// Sync (block until quorum acks) vs async (fire-and-forget) replication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationMode {
    Sync,
    Async,
}

impl Default for ReplicationMode {
    fn default() -> Self {
        Self::Sync
    }
}

// ---------------------------------------------------------------------------
// [network]
// ---------------------------------------------------------------------------

/// `[network]` section: RESP listener tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkConfig {
    pub bind_addr: IpAddr,
    pub bind_port: u16,
    #[serde(with = "humantime_serde")]
    pub keep_alive_period: Duration,
    /// `Duration::ZERO` means idle close disabled.
    #[serde(with = "humantime_serde")]
    pub idle_close: Duration,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            bind_addr: IpAddr::from([0, 0, 0, 0]),
            bind_port: 3320,
            keep_alive_period: Duration::from_secs(300),
            idle_close: Duration::ZERO,
        }
    }
}

impl NetworkConfig {
    fn validate(&self) -> Result<()> {
        if self.bind_port == 0 {
            return Err(Error::Config("network.bind_port must be non-zero".into()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// [auth]
// ---------------------------------------------------------------------------

/// `[auth]` section: passwords and inter-node secret.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    /// Empty = no client auth.
    pub password: String,
    /// Empty = no inter-node auth. Must match across cluster.
    pub cluster_secret: String,
}

// ---------------------------------------------------------------------------
// [discovery]
// ---------------------------------------------------------------------------

/// `[discovery]` section: SWIM bind + bootstrap peer list.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscoveryConfig {
    pub bind_addr: IpAddr,
    pub bind_port: u16,
    pub peers: Vec<String>,
    pub max_join_attempts: u32,
    #[serde(with = "humantime_serde")]
    pub join_retry_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub bootstrap_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub leave_timeout: Duration,
    /// Optional discovery plugin name: `"static"`, `"dns"`, `"kubernetes"`,
    /// `"consul"`. `None` = static peers only.
    pub plugin: Option<String>,
    /// When `plugin = "kubernetes"`, also use `notReadyAddresses` from the
    /// Endpoints API. Required for first-pod bootstrap; see
    /// `docs/13-kubernetes.md`.
    pub include_not_ready: bool,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            bind_addr: IpAddr::from([0, 0, 0, 0]),
            bind_port: 3322,
            peers: Vec::new(),
            max_join_attempts: 10,
            join_retry_interval: Duration::from_secs(1),
            bootstrap_timeout: Duration::from_secs(10),
            leave_timeout: Duration::from_secs(5),
            plugin: None,
            include_not_ready: true,
        }
    }
}

impl DiscoveryConfig {
    fn validate(&self, mode: Mode) -> Result<()> {
        if mode.requires_cluster() && self.bind_port == 0 {
            return Err(Error::Config("discovery.bind_port must be non-zero".into()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// [swim]
// ---------------------------------------------------------------------------

/// `[swim]` section: failure-detector tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SwimConfig {
    #[serde(with = "humantime_serde")]
    pub probe_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub probe_timeout: Duration,
    pub indirect_probes: u32,
    pub suspicion_multiplier: u32,
}

impl Default for SwimConfig {
    fn default() -> Self {
        Self {
            probe_interval: Duration::from_secs(1),
            probe_timeout: Duration::from_millis(500),
            indirect_probes: 3,
            suspicion_multiplier: 5,
        }
    }
}

impl SwimConfig {
    fn validate(&self) -> Result<()> {
        if self.probe_timeout >= self.probe_interval {
            return Err(Error::Config(
                "swim.probe_timeout must be < swim.probe_interval".into(),
            ));
        }
        if self.indirect_probes == 0 {
            return Err(Error::Config(
                "swim.indirect_probes must be >= 1 (set to 0 disables indirect probing)".into(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// [storage]
// ---------------------------------------------------------------------------

/// `[storage]` section: engine selection + `RamBlock` tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub engine: String,
    pub table_size: ByteSize,
    pub max_garbage_ratio: f64,
    #[serde(with = "humantime_serde")]
    pub trigger_compaction_interval: Duration,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            engine: "ramblock".into(),
            table_size: ByteSize(1024 * 1024),
            max_garbage_ratio: 0.40,
            trigger_compaction_interval: Duration::from_secs(600),
        }
    }
}

impl StorageConfig {
    fn validate(&self) -> Result<()> {
        if self.engine.is_empty() {
            return Err(Error::Config("storage.engine must be non-empty".into()));
        }
        if self.table_size.0 == 0 {
            return Err(Error::Config("storage.table_size must be > 0".into()));
        }
        if !(0.0..=1.0).contains(&self.max_garbage_ratio) {
            return Err(Error::Config(format!(
                "storage.max_garbage_ratio must be in [0.0, 1.0], got {}",
                self.max_garbage_ratio,
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// [eviction]
// ---------------------------------------------------------------------------

/// `[eviction]` section: global defaults; per-DMap overrides allowed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EvictionConfig {
    #[serde(alias = "num_eviction_workers")]
    pub num_workers: u32,
    pub policy: EvictionPolicy,
    pub lru_samples: u32,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            num_workers: 1,
            policy: EvictionPolicy::None,
            lru_samples: 5,
        }
    }
}

impl EvictionConfig {
    fn validate(&self) -> Result<()> {
        if self.num_workers == 0 {
            return Err(Error::Config("eviction.num_workers must be >= 1".into()));
        }
        if matches!(self.policy, EvictionPolicy::Lru) && self.lru_samples == 0 {
            return Err(Error::Config(
                "eviction.lru_samples must be >= 1 when policy = lru".into(),
            ));
        }
        Ok(())
    }
}

/// Eviction policy variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvictionPolicy {
    None,
    Lru,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::None
    }
}

// ---------------------------------------------------------------------------
// [balancer]
// ---------------------------------------------------------------------------

/// `[balancer]` section: rebalancer loop interval.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BalancerConfig {
    #[serde(with = "humantime_serde")]
    pub trigger_interval: Duration,
}

impl Default for BalancerConfig {
    fn default() -> Self {
        Self {
            trigger_interval: Duration::from_secs(15),
        }
    }
}

impl BalancerConfig {
    fn validate(&self) -> Result<()> {
        if self.trigger_interval.is_zero() {
            return Err(Error::Config(
                "balancer.trigger_interval must be > 0".into(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// [routing]
// ---------------------------------------------------------------------------

/// `[routing]` section: coordinator push cadence and cleanup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingConfig {
    #[serde(with = "humantime_serde")]
    pub push_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub check_empty_fragments_interval: Duration,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            push_interval: Duration::from_secs(60),
            check_empty_fragments_interval: Duration::from_secs(60),
        }
    }
}

impl RoutingConfig {
    fn validate(&self) -> Result<()> {
        if self.push_interval.is_zero() {
            return Err(Error::Config("routing.push_interval must be > 0".into()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// [events]
// ---------------------------------------------------------------------------

/// `[events]` section: cluster-event channel publishing.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EventsConfig {
    pub enable_cluster_events_channel: bool,
}

// ---------------------------------------------------------------------------
// [hash]
// ---------------------------------------------------------------------------

/// `[hash]` section: which hash function the consistent-hash ring uses.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HashConfig {
    /// `"xxhash"` (default) — runtime injection of a custom impl is via the
    /// programmatic `Hasher` trait, not TOML.
    pub function: String,
}

impl Default for HashConfig {
    fn default() -> Self {
        Self {
            function: "xxhash".into(),
        }
    }
}

impl HashConfig {
    fn validate(&self) -> Result<()> {
        if self.function.is_empty() {
            return Err(Error::Config("hash.function must be non-empty".into()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// [[dmaps]]
// ---------------------------------------------------------------------------

/// Per-DMap override.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DMapConfig {
    /// Skipped when serializing per-DMap config keyed by name.
    #[serde(default)]
    pub name: String,
    /// `None` = inherit, `Some(Duration::ZERO)` = disable idle eviction.
    #[serde(default, with = "humantime_serde")]
    pub max_idle_duration: Option<Duration>,
    /// `None` = inherit, `Some(Duration::ZERO)` = never expire.
    #[serde(default, with = "humantime_serde")]
    pub ttl: Option<Duration>,
    pub max_keys: Option<u64>,
    pub max_inuse: Option<ByteSize>,
    pub lru_samples: Option<u32>,
    pub eviction_policy: Option<EvictionPolicy>,
}

impl DMapConfig {
    fn validate(&self, name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(Error::Config("dmap.name must be non-empty".into()));
        }
        if matches!(self.eviction_policy, Some(EvictionPolicy::Lru))
            && self.lru_samples.is_some_and(|s| s == 0)
        {
            return Err(Error::Config(format!(
                "dmap[{name}].lru_samples must be >= 1 when eviction_policy = lru",
            )));
        }
        Ok(())
    }
}

// `[[dmaps]]` is a TOML array of tables where each entry has a `name`.
// We convert that into a `HashMap<String, DMapConfig>` keyed by name.
fn deserialize_dmaps<'de, D>(de: D) -> std::result::Result<HashMap<String, DMapConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;
    let entries = Vec::<DMapConfig>::deserialize(de)?;
    let mut map = HashMap::with_capacity(entries.len());
    for mut entry in entries {
        if entry.name.is_empty() {
            return Err(D::Error::custom("each [[dmaps]] entry needs a name"));
        }
        let name = std::mem::take(&mut entry.name);
        if map.insert(name.clone(), entry).is_some() {
            return Err(D::Error::custom(format!("duplicate dmap name: {name}")));
        }
    }
    Ok(map)
}

/// Inverse of [`deserialize_dmaps`]: rebuilds the `[[dmaps]]` array form.
/// Entries are sorted by name so the output is deterministic.
fn serialize_dmaps<S>(
    map: &HashMap<String, DMapConfig>,
    s: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeSeq as _;
    let mut names: Vec<&String> = map.keys().collect();
    names.sort();
    let mut seq = s.serialize_seq(Some(names.len()))?;
    for name in names {
        let mut entry = map[name].clone();
        entry.name.clone_from(name);
        seq.serialize_element(&entry)?;
    }
    seq.end()
}

// ---------------------------------------------------------------------------
// Byte-size newtype with humansize parsing
// ---------------------------------------------------------------------------

/// Byte quantity, parsed from TOML as either an integer (bytes) or a
/// human-readable string like `"512MB"`. Round-trips back to the integer
/// form on serialize; the human form is parse-only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ByteSize(pub u64);

impl Serialize for ByteSize {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = ByteSize;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("byte count (integer or human string like \"512MB\")")
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> std::result::Result<ByteSize, E> {
                Ok(ByteSize(v))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> std::result::Result<ByteSize, E> {
                u64::try_from(v).map(ByteSize).map_err(E::custom)
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<ByteSize, E> {
                parse_byte_size(v).map(ByteSize).map_err(E::custom)
            }
        }
        de.deserialize_any(Visitor)
    }
}

fn parse_byte_size(s: &str) -> std::result::Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty byte-size string".into());
    }
    let (num, suffix) = split_byte_size(s);
    let num: f64 = num
        .parse()
        .map_err(|e| format!("invalid number `{num}`: {e}"))?;
    let mult: u64 = match suffix.to_ascii_uppercase().as_str() {
        "" | "B" => 1,
        "K" | "KB" | "KIB" => 1024,
        "M" | "MB" | "MIB" => 1024 * 1024,
        "G" | "GB" | "GIB" => 1024 * 1024 * 1024,
        "T" | "TB" | "TIB" => 1024_u64.pow(4),
        other => return Err(format!("unknown byte-size suffix `{other}`")),
    };
    if num < 0.0 || !num.is_finite() {
        return Err(format!(
            "byte size must be a finite non-negative number, got {num}"
        ));
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    Ok((num * mult as f64) as u64)
}

fn split_byte_size(s: &str) -> (&str, &str) {
    let split = s.find(|c: char| c.is_alphabetic()).unwrap_or(s.len());
    let (num, suffix) = s.split_at(split);
    (num.trim(), suffix.trim())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)] // clearer in test bodies
mod tests {
    use super::*;

    #[test]
    fn default_config_validates() {
        Config::default()
            .validate()
            .expect("default config must validate");
    }

    #[test]
    fn write_quorum_must_not_exceed_replica_count() {
        let mut cfg = Config::default();
        cfg.core.replica_count = 1;
        cfg.core.write_quorum = 2;
        let err = cfg.validate().expect_err("invalid combo");
        assert!(matches!(err, Error::Config(msg) if msg.contains("write_quorum")));
    }

    #[test]
    fn read_quorum_must_not_exceed_replica_count() {
        let mut cfg = Config::default();
        cfg.core.replica_count = 1;
        cfg.core.read_quorum = 3;
        let err = cfg.validate().expect_err("invalid combo");
        assert!(matches!(err, Error::Config(msg) if msg.contains("read_quorum")));
    }

    #[test]
    fn partition_count_zero_rejected() {
        let mut cfg = Config::default();
        cfg.core.partition_count = 0;
        assert!(matches!(cfg.validate(), Err(Error::Config(_))));
    }

    #[test]
    fn load_factor_below_one_rejected() {
        let mut cfg = Config::default();
        cfg.core.load_factor = 0.9;
        assert!(matches!(cfg.validate(), Err(Error::Config(_))));
    }

    #[test]
    fn load_factor_nan_rejected() {
        let mut cfg = Config::default();
        cfg.core.load_factor = f64::NAN;
        assert!(matches!(cfg.validate(), Err(Error::Config(_))));
    }

    #[test]
    fn embedded_solo_rejects_peers() {
        let mut cfg = Config::default();
        cfg.mode = Mode::EmbeddedSolo;
        cfg.discovery.peers = vec!["10.0.1.1:3322".into()];
        let err = cfg.validate().expect_err("embedded_solo + peers");
        assert!(matches!(
            err,
            Error::IrrelevantSection { section, mode: Mode::EmbeddedSolo }
                if section == "discovery.peers"
        ));
    }

    #[test]
    fn embedded_solo_rejects_replica_gt_one() {
        let mut cfg = Config::default();
        cfg.mode = Mode::EmbeddedSolo;
        cfg.core.replica_count = 2;
        cfg.core.write_quorum = 1;
        assert!(matches!(cfg.validate(), Err(Error::Config(_))));
    }

    #[test]
    fn production_rejects_single_replica() {
        let mut cfg = Config::default();
        cfg.mode = Mode::EmbeddedClustered;
        cfg.profile = Profile::Production;
        cfg.core.replica_count = 1;
        cfg.core.member_count_quorum = 1;
        let err = cfg.validate().expect_err("prod + replica=1");
        assert!(matches!(err, Error::ProductionReplicaCount { got: 1 }));
    }

    #[test]
    fn production_rejects_minority_quorum() {
        let mut cfg = Config::default();
        cfg.mode = Mode::EmbeddedClustered;
        cfg.profile = Profile::Production;
        cfg.core.replica_count = 3;
        cfg.core.write_quorum = 2;
        cfg.core.member_count_quorum = 1;
        let err = cfg.validate().expect_err("prod + minority quorum");
        assert!(matches!(err, Error::ProductionQuorum { got: 1 }));
    }

    #[test]
    fn production_accepts_majority_quorum() {
        let mut cfg = Config::default();
        cfg.mode = Mode::EmbeddedClustered;
        cfg.profile = Profile::Production;
        cfg.core.replica_count = 3;
        cfg.core.write_quorum = 2;
        cfg.core.member_count_quorum = 2;
        cfg.validate().expect("majority quorum is valid in prod");
    }

    #[test]
    fn swim_timeout_must_be_less_than_interval() {
        let mut cfg = Config::default();
        cfg.swim.probe_timeout = Duration::from_secs(2);
        cfg.swim.probe_interval = Duration::from_secs(1);
        assert!(matches!(cfg.validate(), Err(Error::Config(_))));
    }

    #[test]
    fn parses_minimal_toml() {
        let toml = r#"
            mode = "standalone"
            profile = "development"

            [core]
            partition_count = 271
            replica_count = 1
        "#;
        let cfg = Config::from_toml_str(toml).expect("minimal toml parses");
        assert_eq!(cfg.mode, Mode::Standalone);
        assert_eq!(cfg.core.partition_count, 271);
    }

    #[test]
    fn rejects_unknown_field() {
        let toml = r"
            [core]
            partition_count = 271
            something_unknown = true
        ";
        assert!(matches!(
            Config::from_toml_str(toml),
            Err(Error::TomlParse(_))
        ));
    }

    #[test]
    fn parses_human_duration() {
        let toml = r#"
            [swim]
            probe_interval = "2s"
            probe_timeout = "500ms"
        "#;
        let cfg = Config::from_toml_str(toml).expect("durations parse");
        assert_eq!(cfg.swim.probe_interval, Duration::from_secs(2));
        assert_eq!(cfg.swim.probe_timeout, Duration::from_millis(500));
    }

    #[test]
    fn parses_byte_size_int() {
        let toml = r"
            [storage]
            table_size = 4096
        ";
        let cfg = Config::from_toml_str(toml).expect("bytes int parses");
        assert_eq!(cfg.storage.table_size, ByteSize(4096));
    }

    #[test]
    fn parses_byte_size_string() {
        let toml = r#"
            [storage]
            table_size = "2MB"
        "#;
        let cfg = Config::from_toml_str(toml).expect("bytes string parses");
        assert_eq!(cfg.storage.table_size, ByteSize(2 * 1024 * 1024));
    }

    #[test]
    fn parses_dmaps_into_map() {
        let toml = r#"
            [[dmaps]]
            name = "sessions"
            ttl = "1h"
            eviction_policy = "lru"
            lru_samples = 10

            [[dmaps]]
            name = "rate_limits"
            ttl = "1m"
        "#;
        let cfg = Config::from_toml_str(toml).expect("dmaps parse");
        assert_eq!(cfg.dmaps.len(), 2);
        let sessions = &cfg.dmaps["sessions"];
        assert_eq!(sessions.ttl, Some(Duration::from_secs(3600)));
        assert_eq!(sessions.eviction_policy, Some(EvictionPolicy::Lru));
    }

    #[test]
    fn duplicate_dmap_names_rejected() {
        let toml = r#"
            [[dmaps]]
            name = "x"
            [[dmaps]]
            name = "x"
        "#;
        assert!(matches!(
            Config::from_toml_str(toml),
            Err(Error::TomlParse(_))
        ));
    }

    #[test]
    fn config_roundtrips_through_toml() {
        let original = Config::default();
        let s = toml::to_string(&original).expect("serialize");
        let parsed: Config = toml::from_str(&s).expect("re-parse");
        parsed.validate().expect("re-parsed default still valid");
        assert_eq!(parsed.core.partition_count, original.core.partition_count);
        assert_eq!(parsed.mode, original.mode);
    }
}
