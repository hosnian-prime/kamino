# Configuration Architecture

## Overview

This document defines **how** Kamino's configuration is shaped, layered, and reloaded. For the per-knob reference (defaults, units, scopes), see [Configuration](09-configuration.md).

The questions answered here:

- What does the type system enforce vs what does runtime validation enforce?
- Which knob lives where, in which mode?
- What is the source precedence, and how do TOML, env vars, and code interact?
- Which knobs can be hot-reloaded? How?

## The `Config` Type

A single `Config` value is the canonical representation. Every loader — TOML, env, programmatic builder — produces one of these. Every runtime component reads from this.

```rust
pub struct Config {
    pub core: CoreConfig,
    pub storage: StorageConfig,
    pub eviction: EvictionConfig,
    pub dmaps: BTreeMap<String, DMapConfig>,
    pub observability: ObservabilityConfig,
    pub mode: Mode,
}
```

`mode` is an enum, not a flag, because the **valid set of knobs differs by mode**. Encoding the difference as data instead of as runtime guards collapses an entire class of misconfigurations into compile errors.

## Modes

```rust
pub enum Mode {
    /// `kamino-server`: full network surface, listens for clients, gossips with peers.
    Standalone {
        network: NetworkConfig,
        discovery: DiscoveryConfig,
        auth: AuthConfig,
        swim: SwimConfig,
        balancer: BalancerConfig,
        routing: RoutingConfig,
    },

    /// Embedded library, single in-process node. No network, no peers, no SWIM.
    /// The simplest possible mode: a Kamino-flavored local cache.
    EmbeddedSolo,

    /// Embedded library that joins a SWIM cluster — typically a fleet of application
    /// processes sharing one cache layer without a separate server tier.
    EmbeddedClustered {
        discovery: DiscoveryConfig,
        cluster_auth: ClusterAuthConfig,
        swim: SwimConfig,
        balancer: BalancerConfig,
        routing: RoutingConfig,
        /// Optional: expose RESP for sidecars and CLI tools (`redis-cli`).
        /// Defaults to `None` — no external listener.
        resp_listener: Option<NetworkConfig>,
    },
}
```

### Mode Selection Cheat Sheet

| Goal | Mode | Notes |
|------|------|-------|
| Run Kamino as a service, clients connect via RESP | `Standalone` | The conventional deployment. |
| Add a cache to a single binary; no clustering | `EmbeddedSolo` | Like `Arc<Mutex<HashMap>>` with TTL, eviction, and the Kamino API. No replication — `replica_count` must equal 1. |
| Microservice fleet wants a shared cache, no separate server tier | `EmbeddedClustered` | Each app process is also a Kamino node. SWIM still does its job. |
| Embedded-clustered + `redis-cli` needs to inspect state | `EmbeddedClustered { resp_listener: Some(_) }` | Bind on `127.0.0.1` only unless you really mean to expose it. |

### Invariants the Type Forces

| Invariant | How |
|-----------|-----|
| Embedded modes never have a client `bind_addr` by accident | Field absent in `EmbeddedSolo`; opt-in `Option` in `EmbeddedClustered` |
| `EmbeddedSolo` cannot have peers | `discovery` field absent in the variant |
| `Standalone` cannot run without an `auth` decision | `auth: AuthConfig` is mandatory; `AuthConfig::disabled()` is the explicit way to opt out |
| Mode cannot change at runtime | `mode` is `#[bootstrap_only]` (see [Reload Discipline](#reload-discipline)) |

Each of these used to be a runtime check or a docs warning. The enum makes them unrepresentable in invalid form.

## Source Precedence

Config is layered. Higher layers override lower ones:

```
4. Programmatic overrides  (highest — explicit code wins)
3. Environment variables
2. TOML file
1. Profile defaults        (lowest — what you get if you specify nothing)
```

A typical server boot:

```rust
let cfg = Config::builder()
    .profile(Profile::Production)              // layer 1
    .load_toml("/etc/kamino.toml")?            // layer 2
    .with_env_overrides("KAMINO_")?            // layer 3
    .core(|c| c.partition_count(509))          // layer 4
    .build()?;                                 // validate, freeze, return
```

A typical embedded boot:

```rust
let node = Kamino::embedded()                  // EmbeddedSolo + Development profile
    .profile(Profile::Production)
    .with_dmap("sessions", |d| d.ttl_secs(3600))
    .start().await?;
```

The builder chain is explicit about which layer wins; no implicit precedence surprises.

### Env Var Encoding

Env vars use a flat double-underscore path:

```
KAMINO_CORE__REPLICA_COUNT=2
KAMINO_CORE__WRITE_QUORUM=2
KAMINO_OBSERVABILITY__OTLP_ENDPOINT=http://otel:4317
KAMINO_DMAPS__SESSIONS__TTL=24h
```

- Single underscore stays inside an identifier (`REPLICA_COUNT`).
- Double underscore is the path separator (`CORE__REPLICA_COUNT` → `config.core.replica_count`).
- Map keys (DMap names) participate in the path: `DMAPS__SESSIONS__TTL`.
- Mode-specific fields require the variant in the path: `KAMINO_MODE__STANDALONE__NETWORK__BIND_PORT=3320`. The mode itself is set with `KAMINO_MODE=standalone`.

This is the same scheme Vector and Spring Boot use; deliberately boring.

### TOML Mapping

```toml
mode = "standalone"          # or "embedded-solo" | "embedded-clustered"

[core]
partition_count = 271
replica_count = 2

[network]                    # gated on mode = "standalone" or "embedded-clustered" + resp_listener
bind_addr = "0.0.0.0"
bind_port = 3320

[discovery]                  # gated on modes that have it
peers = ["10.0.1.1:3322"]

[[dmaps]]
name = "sessions"
ttl = "24h"
```

Sections that don't apply to the chosen mode are a **hard error**, not silently ignored. `mode = "embedded-solo"` with a `[network]` section fails `Config::build()` with `ErrIrrelevantSection { section: "network", mode: "embedded-solo" }`. Silent ignore is the failure mode that bites at 3 AM when you typo a section name and your config doesn't take effect.

## Profiles

A profile is a named bundle of defaults. Profiles are first-class — not a side note in the docs but a value you set:

```rust
pub enum Profile {
    /// Single-node ergonomics. replica_count = 1, member_count_quorum = 1,
    /// no observability endpoints exposed. Suitable for `cargo run` and tests.
    Development,
    /// Survives one node failure, enforces split-brain protection, exposes
    /// Prometheus metrics by default.
    Production,
}
```

### Defaults by Profile

| Knob | Development | Production |
|------|-------------|------------|
| `replica_count` | 1 | 2 |
| `write_quorum` | 1 | 2 |
| `member_count_quorum` | 1 | majority (≥ 2) |
| `read_repair` | false | true |
| `replication_mode` | Sync | Sync |
| `observability.prometheus_addr` | disabled | `:9128` |
| Slow command log | disabled | enabled, threshold = 100ms |

A node booting without an explicit profile logs **one** WARN at startup:

```
WARN no profile selected; using Development defaults —
     set Profile::Production or KAMINO_PROFILE=production for any non-dev deployment
```

Not fail-closed because that would break first-run quickstarts. Loud enough that operators see it on their first dry-run.

## Reload Discipline

Every knob is annotated in source as **bootstrap-only** or **reloadable**. The annotation drives runtime behavior; it is not just documentation.

### Bootstrap-only

A knob is `#[bootstrap_only]` when changing it after a node is running would either corrupt data or require a coordinated cluster-wide change. Baked in at `Kamino::new(config)` / `Kamino::serve(config)`. Examples:

- `core.partition_count` (immutable; see [Consistent Hashing](02-consistent-hashing.md#immutability-of-partition_count))
- `core.replica_count` (changing it requires coordinator-led re-replication, not a per-node reload)
- `mode`
- `network.bind_port`, `discovery.bind_port` (port juggling is uglier than a restart)
- `storage.engine` (must be chosen at first allocation; switching needs migration)
- `auth.cluster_secret` (must change in lockstep across all members)

### Reloadable

A knob is `#[reloadable]` when changing it on a live node is correct and lock-free. Examples:

- `observability.slow_command_threshold_ms`, `observability.log_level`
- `core.member_count_quorum` (operators sometimes legitimately need to relax this during an outage — it's a policy knob)
- `core.read_repair`
- `balancer.trigger_interval`, `routing.push_interval`
- Per-DMap `ttl`, `max_keys`, `max_inuse`, `eviction_policy` (eviction reacts on next pass)
- `auth.password` (rotation without downtime)

### Reload Triggers

| Deployment | Signal | Behavior |
|------------|--------|----------|
| Standalone | `SIGHUP` | Re-read the TOML file specified at startup, re-apply env overrides, diff against running config |
| Embedded | `Kamino::reload_config(new_config) -> Result<ReloadReport>` | Caller supplies the new `Config` explicitly |

In both paths, the diff between old and new is computed field-by-field:

- `#[reloadable]` fields that changed are applied atomically.
- `#[bootstrap_only]` fields that changed cause the reload to **fail** with `ErrBootstrapImmutable { field }` — no partial apply.

```rust
pub struct ReloadReport {
    pub changed: Vec<&'static str>,
    pub rejected: Vec<RejectedChange>,  // bootstrap_only fields the caller tried to change
}
```

The reload is all-or-nothing on the bootstrap dimension. If `partition_count` differs, the entire reload fails before any other field is touched. This eliminates "partial reload that applied half the changes then refused" — the worst kind of config bug.

Every reload event logs `who, when, which fields changed` at INFO level.

## Validation

`Config::build()` runs cross-field invariants. None are deferred to first-use:

| Check | Failure |
|-------|---------|
| `partition_count` is prime | `ErrInvalidPartitionCount` |
| `write_quorum ≤ replica_count` | `ErrQuorumExceedsReplicas` |
| `read_quorum ≤ replica_count` | `ErrQuorumExceedsReplicas` |
| `member_count_quorum ≥ 1` | `ErrInvalidQuorum` |
| `network.bind_port ≠ discovery.bind_port` | `ErrPortCollision` |
| `Mode::EmbeddedSolo` ⟹ `replica_count == 1` | `ErrSoloReplication` |
| `Profile::Production` ⟹ `replica_count ≥ 2` | `ErrProductionReplicaCount` (hard fail) |
| `Profile::Production` ⟹ `member_count_quorum ≥ majority` | `ErrProductionQuorum` (hard fail) |
| TOML section irrelevant to the chosen mode | `ErrIrrelevantSection` |

`Profile::Production` enforces hard limits, not nudges. If production allowed `replica_count = 1` with just a warning, the warning would be ignored, the incident would happen, and the profile would have provided false comfort.

## Embedded Ergonomics

90% of embedded users want a one-liner. The builder collapses the common case:

```rust
let node = Kamino::embedded()
    .with_dmap("sessions", |d| d.ttl_secs(3600).max_keys(100_000))
    .start().await?;

let cache = node.client().new_dmap("sessions", Default::default())?;
cache.put("user:1", b"data", Default::default()).await?;
```

`Kamino::embedded()` is sugar for `Config::builder().mode(Mode::EmbeddedSolo).profile(Profile::Development)`. The full `Config` is still available via `.config()` on the builder for inspection or test fixtures.

For embedded-clustered:

```rust
let node = Kamino::embedded_clustered()
    .profile(Profile::Production)
    .with_peers(&["app-1.svc:3322", "app-2.svc:3322"])
    .with_dmap("sessions", |d| d.ttl_secs(3600))
    .start().await?;
```

The server case stays TOML-first:

```rust
let cfg = Config::load_toml("/etc/kamino.toml")?
    .profile(Profile::Production)        // enforces production invariants
    .with_env_overrides("KAMINO_")?
    .build()?;
Kamino::serve(cfg).await
```

The profile is applied **before** the TOML so the TOML overrides what it intends to override; the profile catches anything the TOML left as default.

## Worked Examples

### Embedded, Single Process

```rust
use kamino::Kamino;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let node = Kamino::embedded()
        .with_dmap("sessions", |d| d.ttl_secs(3600).max_keys(1_000_000))
        .with_dmap("rate_limits", |d| d.ttl_secs(60).max_keys(100_000))
        .start().await?;

    // ... use node.client() ...

    node.shutdown().await?;
    Ok(())
}
```

No TOML, no env vars, no listener — just the cache.

### Embedded Clustered (Service Fleet)

```rust
let node = Kamino::embedded_clustered()
    .profile(Profile::Production)
    .with_peers_from_env("APP_PEERS")            // comma-separated host:port
    .with_cluster_secret_from_env("KAMINO_CLUSTER_SECRET")
    .with_dmap("session_cache", |d| d.ttl_secs(1800))
    .start().await?;
```

Each app process is also a Kamino node. No separate server tier to operate.

### Standalone (Production)

```toml
# /etc/kamino.toml
mode = "standalone"

[core]
partition_count = 271
replica_count = 2
write_quorum = 2
member_count_quorum = 2
read_repair = true

[network]
bind_addr = "0.0.0.0"
bind_port = 3320

[discovery]
plugin = "kubernetes"
bind_port = 3322

[auth]
# password and cluster_secret are loaded from env:
# KAMINO_AUTH__PASSWORD, KAMINO_AUTH__CLUSTER_SECRET
# Never commit secrets to this file.

[observability]
prometheus_addr = "0.0.0.0:9128"
otlp_endpoint = "http://otel-collector:4317"

[[dmaps]]
name = "sessions"
ttl = "24h"
max_inuse = "512MB"
eviction_policy = "lru"
```

```rust
let cfg = Config::load_toml("/etc/kamino.toml")?
    .profile(Profile::Production)
    .with_env_overrides("KAMINO_")?
    .build()?;
Kamino::serve(cfg).await
```

## What This Architecture Does Not Solve

- **Secret management.** Passwords and the cluster secret load from env vars (never from the TOML in production), but the secret store itself is the operator's concern. Vault, AWS Secrets Manager, K8s Secrets — all fine; Kamino does not embed a secret backend.
- **Config distribution to a fleet.** The TOML file at `/etc/kamino.toml` is the unit; distribute it however your deploy tooling distributes files.
- **Configuration audit trail.** Reload events log `who, when, what changed` at INFO, but the durable audit record belongs in your log aggregator, not in Kamino.
