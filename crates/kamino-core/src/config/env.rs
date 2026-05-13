//! Environment-variable overlay for [`crate::Config`].
//!
//! Encoding (per `docs/16-config-architecture.md`):
//!
//! ```text
//! KAMINO_<SECTION>__<FIELD>=value         # e.g. KAMINO_CORE__REPLICA_COUNT=3
//! ```
//!
//! Strategy: collect matching env vars, build a partial TOML fragment from
//! them, parse it into an `Option`-shaped mirror of [`crate::Config`], then
//! merge into the caller-supplied base config and re-validate.
//!
//! Unknown sections (e.g. `KAMINO_NONEXISTENT__FIELD`) are dropped silently.
//! Values that fail to parse against a known field are returned as
//! `Error::Config`. Nested sub-tables beyond one level are not supported by
//! the overlay; per-DMap overrides go through the TOML file.

use std::collections::BTreeMap;
use std::env;

use serde::Deserialize;
use toml::Value;

use crate::config::Config;
use crate::error::{Error, Result};

/// Apply `KAMINO_*` env overrides on top of `config` in place.
pub fn overlay_from_env(config: &mut Config) -> Result<()> {
    overlay_from_iter(config, env::vars())
}

/// Same as [`overlay_from_env`] but takes a pre-collected iterator. Useful
/// for unit tests that want to inject a synthetic env without poisoning the
/// process-wide `std::env`.
pub fn overlay_from_iter<I, K, V>(config: &mut Config, vars: I) -> Result<()>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let pairs: Vec<(String, String)> = vars
        .into_iter()
        .filter_map(|(k, v)| {
            let k = k.as_ref();
            k.strip_prefix("KAMINO_")
                .map(|stripped| (stripped.to_string(), v.as_ref().to_string()))
        })
        .collect();

    if pairs.is_empty() {
        return Ok(());
    }

    let toml_fragment = build_toml(&pairs);
    if toml_fragment.trim().is_empty() {
        return Ok(());
    }

    let overlay: ConfigOverlay = toml::from_str(&toml_fragment).map_err(|e| {
        Error::Config(format!(
            "failed to parse env overlay: {e}; fragment was:\n{toml_fragment}"
        ))
    })?;
    overlay.merge_into(config);
    config.validate()?;
    Ok(())
}

fn build_toml(pairs: &[(String, String)]) -> String {
    // Group by leading segment (section name).
    let mut grouped: BTreeMap<String, Vec<(Vec<String>, &str)>> = BTreeMap::new();
    for (key, value) in pairs {
        let segments: Vec<String> = key.split("__").map(str::to_ascii_lowercase).collect();
        if segments.is_empty() || segments[0].is_empty() {
            continue;
        }
        if let Some((head, rest)) = segments.split_first() {
            grouped
                .entry(head.clone())
                .or_default()
                .push((rest.to_vec(), value.as_str()));
        }
    }

    let known_sections: &[&str] = &[
        "core",
        "network",
        "auth",
        "discovery",
        "swim",
        "storage",
        "eviction",
        "balancer",
        "routing",
        "events",
        "hash",
        "mode",
        "profile",
    ];

    let mut top_level_lines: Vec<String> = Vec::new();
    let mut section_blocks: Vec<String> = Vec::new();

    // `mode = ...` and `profile = ...` are top-level scalars (no section).
    for scalar in ["mode", "profile"] {
        if let Some(entries) = grouped.get(scalar) {
            if entries.len() == 1 && entries[0].0.is_empty() {
                let rhs = toml_scalar(entries[0].1);
                top_level_lines.push(format!("{scalar} = {rhs}"));
            }
        }
    }

    for (section, entries) in &grouped {
        if !known_sections.contains(&section.as_str()) {
            continue;
        }
        if section == "mode" || section == "profile" {
            continue;
        }
        let mut lines = Vec::new();
        for (segments, value) in entries {
            if segments.len() == 1 {
                let field = &segments[0];
                if field.is_empty() {
                    continue;
                }
                lines.push(format!("{} = {}", field, toml_scalar(value)));
            }
            // Deeper paths are dropped silently — TOML file covers them.
        }
        if !lines.is_empty() {
            section_blocks.push(format!("[{section}]\n{}", lines.join("\n")));
        }
    }

    let mut out = String::new();
    if !top_level_lines.is_empty() {
        out.push_str(&top_level_lines.join("\n"));
        out.push('\n');
    }
    if !section_blocks.is_empty() {
        out.push_str(&section_blocks.join("\n\n"));
    }
    out
}

/// Best-effort scalar encoder. Tries bool, integer, then float (only if the
/// input has a decimal/exponent so bare integers don't become floats), then
/// falls back to a TOML string literal.
fn toml_scalar(raw: &str) -> String {
    let trimmed = raw.trim();
    match trimmed.to_ascii_lowercase().as_str() {
        "true" => return "true".to_string(),
        "false" => return "false".to_string(),
        _ => {}
    }
    if let Ok(n) = trimmed.parse::<i64>() {
        return n.to_string();
    }
    if trimmed.contains('.') || trimmed.contains(['e', 'E']) {
        if let Ok(f) = trimmed.parse::<f64>() {
            if f.is_finite() {
                return format!("{f}");
            }
        }
    }
    Value::String(raw.to_string()).to_string()
}

/// Mirror of [`crate::Config`] used purely to absorb env overrides. Fields
/// are all `Option<...>` so missing keys mean "leave alone".
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ConfigOverlay {
    mode: Option<crate::mode::Mode>,
    profile: Option<crate::profile::Profile>,
    core: Option<CoreOverlay>,
    network: Option<NetworkOverlay>,
    auth: Option<AuthOverlay>,
    discovery: Option<DiscoveryOverlay>,
    swim: Option<SwimOverlay>,
    storage: Option<StorageOverlay>,
    eviction: Option<EvictionOverlay>,
    balancer: Option<BalancerOverlay>,
    routing: Option<RoutingOverlay>,
    events: Option<EventsOverlay>,
    hash: Option<HashOverlay>,
}

impl ConfigOverlay {
    fn merge_into(self, cfg: &mut Config) {
        if let Some(m) = self.mode {
            cfg.mode = m;
        }
        if let Some(p) = self.profile {
            cfg.profile = p;
        }
        if let Some(c) = self.core {
            c.merge_into(&mut cfg.core);
        }
        if let Some(n) = self.network {
            n.merge_into(&mut cfg.network);
        }
        if let Some(a) = self.auth {
            a.merge_into(&mut cfg.auth);
        }
        if let Some(d) = self.discovery {
            d.merge_into(&mut cfg.discovery);
        }
        if let Some(s) = self.swim {
            s.merge_into(&mut cfg.swim);
        }
        if let Some(s) = self.storage {
            s.merge_into(&mut cfg.storage);
        }
        if let Some(e) = self.eviction {
            e.merge_into(&mut cfg.eviction);
        }
        if let Some(b) = self.balancer {
            b.merge_into(&mut cfg.balancer);
        }
        if let Some(r) = self.routing {
            r.merge_into(&mut cfg.routing);
        }
        if let Some(e) = self.events {
            e.merge_into(&mut cfg.events);
        }
        if let Some(h) = self.hash {
            h.merge_into(&mut cfg.hash);
        }
    }
}

macro_rules! overlay_struct {
    ($name:ident { $( $field:ident : $ty:ty ),* $(,)? }) => {
        #[derive(Debug, Default, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        struct $name {
            $( $field: Option<$ty>, )*
        }
    };
}

overlay_struct!(CoreOverlay {
    partition_count: u32,
    replica_count: u32,
    write_quorum: u32,
    read_quorum: u32,
    member_count_quorum: u32,
    replication_mode: crate::config::ReplicationMode,
    read_repair: bool,
    load_factor: f64,
});

impl CoreOverlay {
    fn merge_into(self, dst: &mut crate::config::CoreConfig) {
        macro_rules! set {
            ($f:ident) => {
                if let Some(v) = self.$f {
                    dst.$f = v;
                }
            };
        }
        set!(partition_count);
        set!(replica_count);
        set!(write_quorum);
        set!(read_quorum);
        set!(member_count_quorum);
        set!(replication_mode);
        set!(read_repair);
        set!(load_factor);
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct NetworkOverlay {
    bind_addr: Option<std::net::IpAddr>,
    bind_port: Option<u16>,
    #[serde(default, with = "humantime_serde")]
    keep_alive_period: Option<std::time::Duration>,
    #[serde(default, with = "humantime_serde")]
    idle_close: Option<std::time::Duration>,
}

impl NetworkOverlay {
    fn merge_into(self, dst: &mut crate::config::NetworkConfig) {
        if let Some(v) = self.bind_addr {
            dst.bind_addr = v;
        }
        if let Some(v) = self.bind_port {
            dst.bind_port = v;
        }
        if let Some(v) = self.keep_alive_period {
            dst.keep_alive_period = v;
        }
        if let Some(v) = self.idle_close {
            dst.idle_close = v;
        }
    }
}

overlay_struct!(AuthOverlay {
    password: String,
    cluster_secret: String,
});

impl AuthOverlay {
    fn merge_into(self, dst: &mut crate::config::AuthConfig) {
        if let Some(v) = self.password {
            dst.password = v;
        }
        if let Some(v) = self.cluster_secret {
            dst.cluster_secret = v;
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DiscoveryOverlay {
    bind_addr: Option<std::net::IpAddr>,
    bind_port: Option<u16>,
    max_join_attempts: Option<u32>,
    #[serde(default, with = "humantime_serde")]
    join_retry_interval: Option<std::time::Duration>,
    #[serde(default, with = "humantime_serde")]
    bootstrap_timeout: Option<std::time::Duration>,
    #[serde(default, with = "humantime_serde")]
    leave_timeout: Option<std::time::Duration>,
    plugin: Option<String>,
    include_not_ready: Option<bool>,
}

impl DiscoveryOverlay {
    fn merge_into(self, dst: &mut crate::config::DiscoveryConfig) {
        if let Some(v) = self.bind_addr {
            dst.bind_addr = v;
        }
        if let Some(v) = self.bind_port {
            dst.bind_port = v;
        }
        if let Some(v) = self.max_join_attempts {
            dst.max_join_attempts = v;
        }
        if let Some(v) = self.join_retry_interval {
            dst.join_retry_interval = v;
        }
        if let Some(v) = self.bootstrap_timeout {
            dst.bootstrap_timeout = v;
        }
        if let Some(v) = self.leave_timeout {
            dst.leave_timeout = v;
        }
        if let Some(v) = self.plugin {
            dst.plugin = Some(v);
        }
        if let Some(v) = self.include_not_ready {
            dst.include_not_ready = v;
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SwimOverlay {
    #[serde(default, with = "humantime_serde")]
    probe_interval: Option<std::time::Duration>,
    #[serde(default, with = "humantime_serde")]
    probe_timeout: Option<std::time::Duration>,
    indirect_probes: Option<u32>,
    suspicion_multiplier: Option<u32>,
}

impl SwimOverlay {
    fn merge_into(self, dst: &mut crate::config::SwimConfig) {
        if let Some(v) = self.probe_interval {
            dst.probe_interval = v;
        }
        if let Some(v) = self.probe_timeout {
            dst.probe_timeout = v;
        }
        if let Some(v) = self.indirect_probes {
            dst.indirect_probes = v;
        }
        if let Some(v) = self.suspicion_multiplier {
            dst.suspicion_multiplier = v;
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct StorageOverlay {
    engine: Option<String>,
    table_size: Option<crate::config::ByteSize>,
    max_garbage_ratio: Option<f64>,
    #[serde(default, with = "humantime_serde")]
    trigger_compaction_interval: Option<std::time::Duration>,
}

impl StorageOverlay {
    fn merge_into(self, dst: &mut crate::config::StorageConfig) {
        if let Some(v) = self.engine {
            dst.engine = v;
        }
        if let Some(v) = self.table_size {
            dst.table_size = v;
        }
        if let Some(v) = self.max_garbage_ratio {
            dst.max_garbage_ratio = v;
        }
        if let Some(v) = self.trigger_compaction_interval {
            dst.trigger_compaction_interval = v;
        }
    }
}

overlay_struct!(EvictionOverlay {
    num_workers: u32,
    policy: crate::config::EvictionPolicy,
    lru_samples: u32,
});

impl EvictionOverlay {
    fn merge_into(self, dst: &mut crate::config::EvictionConfig) {
        if let Some(v) = self.num_workers {
            dst.num_workers = v;
        }
        if let Some(v) = self.policy {
            dst.policy = v;
        }
        if let Some(v) = self.lru_samples {
            dst.lru_samples = v;
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct BalancerOverlay {
    #[serde(default, with = "humantime_serde")]
    trigger_interval: Option<std::time::Duration>,
}

impl BalancerOverlay {
    fn merge_into(self, dst: &mut crate::config::BalancerConfig) {
        if let Some(v) = self.trigger_interval {
            dst.trigger_interval = v;
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RoutingOverlay {
    #[serde(default, with = "humantime_serde")]
    push_interval: Option<std::time::Duration>,
    #[serde(default, with = "humantime_serde")]
    check_empty_fragments_interval: Option<std::time::Duration>,
}

impl RoutingOverlay {
    fn merge_into(self, dst: &mut crate::config::RoutingConfig) {
        if let Some(v) = self.push_interval {
            dst.push_interval = v;
        }
        if let Some(v) = self.check_empty_fragments_interval {
            dst.check_empty_fragments_interval = v;
        }
    }
}

overlay_struct!(EventsOverlay {
    enable_cluster_events_channel: bool,
});

impl EventsOverlay {
    fn merge_into(self, dst: &mut crate::config::EventsConfig) {
        if let Some(v) = self.enable_cluster_events_channel {
            dst.enable_cluster_events_channel = v;
        }
    }
}

overlay_struct!(HashOverlay { function: String });

impl HashOverlay {
    fn merge_into(self, dst: &mut crate::config::HashConfig) {
        if let Some(v) = self.function {
            dst.function = v;
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn overrides_replica_count() {
        let mut cfg = Config::default();
        overlay_from_iter(&mut cfg, [("KAMINO_CORE__REPLICA_COUNT", "1")]).unwrap();
        assert_eq!(cfg.core.replica_count, 1);
        overlay_from_iter(
            &mut cfg,
            [
                ("KAMINO_MODE", "embedded_clustered"),
                ("KAMINO_CORE__REPLICA_COUNT", "3"),
                ("KAMINO_CORE__WRITE_QUORUM", "2"),
                ("KAMINO_CORE__READ_QUORUM", "2"),
                ("KAMINO_CORE__MEMBER_COUNT_QUORUM", "2"),
            ],
        )
        .unwrap();
        assert_eq!(cfg.core.replica_count, 3);
        assert_eq!(cfg.core.write_quorum, 2);
    }

    #[test]
    fn overrides_duration_field() {
        let mut cfg = Config::default();
        overlay_from_iter(&mut cfg, [("KAMINO_SWIM__PROBE_INTERVAL", "2s")]).unwrap();
        assert_eq!(cfg.swim.probe_interval, Duration::from_secs(2));
    }

    #[test]
    fn unknown_var_is_ignored() {
        let mut cfg = Config::default();
        overlay_from_iter(&mut cfg, [("KAMINO_NONEXISTENT__FIELD", "whatever")]).unwrap();
        assert_eq!(cfg.core.replica_count, 1);
    }

    #[test]
    fn unparseable_value_returns_config_error() {
        let mut cfg = Config::default();
        let err =
            overlay_from_iter(&mut cfg, [("KAMINO_CORE__REPLICA_COUNT", "not-a-number")]).err();
        assert!(matches!(err, Some(Error::Config(_))));
    }

    #[test]
    fn no_kamino_vars_is_noop() {
        let mut cfg = Config::default();
        overlay_from_iter(&mut cfg, [("PATH", "/usr/bin"), ("HOME", "/root")]).unwrap();
        assert_eq!(cfg.core.replica_count, 1);
    }

    #[test]
    fn auth_password_overlay() {
        let mut cfg = Config::default();
        overlay_from_iter(&mut cfg, [("KAMINO_AUTH__PASSWORD", "s3cr3t")]).unwrap();
        assert_eq!(cfg.auth.password, "s3cr3t");
    }

    #[test]
    fn network_port_overlay() {
        let mut cfg = Config::default();
        overlay_from_iter(&mut cfg, [("KAMINO_NETWORK__BIND_PORT", "3399")]).unwrap();
        assert_eq!(cfg.network.bind_port, 3399);
    }

    #[test]
    fn mode_overlay() {
        let mut cfg = Config::default();
        overlay_from_iter(&mut cfg, [("KAMINO_MODE", "standalone")]).unwrap();
        assert_eq!(cfg.mode, crate::mode::Mode::Standalone);
    }

    #[test]
    fn boolean_value() {
        let mut cfg = Config::default();
        overlay_from_iter(
            &mut cfg,
            [("KAMINO_EVENTS__ENABLE_CLUSTER_EVENTS_CHANNEL", "true")],
        )
        .unwrap();
        assert!(cfg.events.enable_cluster_events_channel);
    }
}
