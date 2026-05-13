# Configuration Reference

## Configuration File Format

Kamino uses TOML for configuration:

```toml
# kamino.toml
```

## Complete Configuration Reference

### Core Settings

```toml
[core]
# Total number of hash ring partitions (should be prime).
# IMMUTABLE after first write — changing this value invalidates every existing key.
# Pick once at bootstrap. See docs/02-consistent-hashing.md#immutability-of-partition_count.
partition_count = 271

# Number of data replicas (1 = primary only, 2 = primary + 1 backup)
replica_count = 1

# Minimum successful writes before acknowledging
# Set to replica_count for strongest consistency
write_quorum = 1

# Minimum reads before returning a value
# Set > 1 to enable read-from-replicas
read_quorum = 1

# Minimum cluster members to accept operations
# Set to majority (N/2 + 1) for split-brain protection
member_count_quorum = 1

# Replication mode: "sync" or "async"
replication_mode = "sync"

# Enable read-repair (compare replicas on read, fix stale copies)
read_repair = false

# Consistent hashing load factor (>= 1.0)
load_factor = 1.25
```

### Network Settings

```toml
[network]
# Address to bind the RESP server
bind_addr = "0.0.0.0"

# Port for the RESP server (client + inter-node)
bind_port = 3320

# TCP keep-alive period
keep_alive_period = "300s"

# Close idle connections after this duration (0 = disabled)
idle_close = "0s"
```

### Authentication

```toml
[auth]
# Password for client authentication (empty = no auth)
password = ""

# Shared secret for inter-node (server-to-server) authentication.
# Prevents arbitrary RESP clients from issuing INTERNAL.NODE.* commands.
# Must be identical across all cluster members.
cluster_secret = ""
```

### Discovery Settings

```toml
[discovery]
# SWIM protocol bind address
bind_addr = "0.0.0.0"

# SWIM protocol port
bind_port = 3322

# Static peer list for cluster joining
peers = ["10.0.1.1:3322", "10.0.1.2:3322"]

# Maximum number of join attempts
max_join_attempts = 10

# Delay between join attempts
join_retry_interval = "1s"

# Maximum time to wait for initial cluster formation
bootstrap_timeout = "10s"

# Graceful leave timeout
leave_timeout = "5s"

# Discovery plugin (optional)
# plugin = "consul"
# plugin = "kubernetes"
# plugin = "dns"
```

### SWIM Protocol Tuning

```toml
[swim]
# How often each node probes a random peer
probe_interval = "1s"

# How long to wait for a direct probe response
probe_timeout = "500ms"

# Number of proxy nodes for indirect probing (higher = fewer false positives)
indirect_probes = 3

# Multiplier for suspicion timeout before declaring a suspected node dead.
# Actual timeout = suspicion_multiplier * log(N) * probe_interval
suspicion_multiplier = 5
```

### Storage Engine Settings

```toml
[storage]
# Default storage engine
engine = "ramblock"

# Pre-allocated table size
table_size = "1MB"

# Maximum garbage ratio before compaction
max_garbage_ratio = 0.40

# Compaction check interval
trigger_compaction_interval = "10m"
```

### Eviction Settings (Global Defaults)

```toml
[eviction]
# Number of background eviction worker threads
num_eviction_workers = 1

# Default eviction policy: "none" or "lru"
policy = "none"

# Keys sampled for LRU eviction
lru_samples = 5
```

### Balancer Settings

```toml
[balancer]
# How often the balancer checks for data to migrate
trigger_interval = "15s"
```

### Routing Table Settings

```toml
[routing]
# How often the coordinator pushes the routing table
push_interval = "60s"

# Check and clean empty fragments interval
check_empty_fragments_interval = "60s"
```

### Cluster Events

```toml
[events]
# Publish cluster events to "cluster.events" pub/sub channel
enable_cluster_events_channel = false
```

### Per-DMap Configuration

Override global defaults for specific DMaps:

```toml
[[dmaps]]
name = "sessions"
max_idle_duration = "30m"
ttl = "24h"
max_keys = 1000000
max_inuse = "512MB"
lru_samples = 10
eviction_policy = "lru"

[[dmaps]]
name = "rate_limits"
ttl = "1m"
max_keys = 100000
eviction_policy = "lru"
lru_samples = 3

[[dmaps]]
name = "feature_flags"
# No TTL, no eviction - permanent storage
ttl = "0s"
eviction_policy = "none"
```

### Hash Function

```toml
[hash]
# Hash function: "xxhash" (default), or custom via code
function = "xxhash"
```

## Programmatic Configuration (Rust)

```rust
use kamino::Config;
use std::time::Duration;

let config = Config {
    partition_count: 271,
    replica_count: 2,
    write_quorum: 2,
    read_quorum: 1,
    member_count_quorum: 2,
    replication_mode: ReplicationMode::Sync,
    read_repair: true,
    load_factor: 1.25,

    network: NetworkConfig {
        bind_addr: "0.0.0.0".parse()?,
        bind_port: 3320,
        keep_alive_period: Duration::from_secs(300),
        idle_close: None,
    },

    discovery: DiscoveryConfig {
        bind_addr: "0.0.0.0".parse()?,
        bind_port: 3322,
        peers: vec!["10.0.1.1:3322".parse()?],
        max_join_attempts: 10,
        join_retry_interval: Duration::from_secs(1),
        bootstrap_timeout: Duration::from_secs(10),
        leave_timeout: Duration::from_secs(5),
        plugin: None,
    },

    storage: StorageConfig {
        engine: "ramblock".to_string(),
        table_size: 1024 * 1024, // 1 MB
        max_garbage_ratio: 0.40,
        trigger_compaction_interval: Duration::from_secs(600),
    },

    eviction: EvictionConfig {
        num_workers: 1,
        policy: EvictionPolicy::None,
        lru_samples: 5,
    },

    dmaps: HashMap::from([
        ("sessions".to_string(), DMapConfig {
            max_idle_duration: Some(Duration::from_secs(1800)),
            ttl: Some(Duration::from_secs(86400)),
            max_keys: Some(1_000_000),
            max_inuse: Some(512 * 1024 * 1024),
            lru_samples: 10,
            eviction_policy: EvictionPolicy::LRU,
        }),
    ]),

    ..Default::default()
};
```

## Default Values Summary

| Parameter | Default |
|-----------|---------|
| `partition_count` | 271 |
| `replica_count` | 1 |
| `write_quorum` | 1 |
| `read_quorum` | 1 |
| `member_count_quorum` | 1 |
| `replication_mode` | Sync |
| `read_repair` | false |
| `load_factor` | 1.25 |
| `bind_port` | 3320 |
| `discovery_port` | 3322 |
| `keep_alive_period` | 300s |
| `max_join_attempts` | 10 |
| `join_retry_interval` | 1s |
| `bootstrap_timeout` | 10s |
| `leave_timeout` | 5s |
| `routing_push_interval` | 60s |
| `balancer_trigger_interval` | 15s |
| `compaction_interval` | 10m |
| `empty_fragments_check` | 60s |
| `cluster_secret` | "" (no auth) |
| `swim_probe_interval` | 1s |
| `swim_probe_timeout` | 500ms |
| `swim_indirect_probes` | 3 |
| `swim_suspicion_multiplier` | 5 |
| `storage_engine` | ramblock |
| `table_size` | 1 MB |
| `max_garbage_ratio` | 0.40 |
| `num_eviction_workers` | 1 |
| `lru_samples` | 5 |
| `eviction_policy` | None |

## Production-Recommended Defaults

The shipped defaults prioritize developer ergonomics (single-node quickstart, no replication overhead). **These are not safe for production.** For any deployment that must survive a node crash without data loss, override the following:

```toml
[core]
# Was: 1. Production: at least 2 (primary + 1 backup).
replica_count = 2

# Was: 1. Production: match replica_count for strong write durability,
# or replica_count - 1 to tolerate one slow backup.
write_quorum = 2

# Was: 1. Production: majority of expected cluster size.
# Example: N=3 → 2, N=5 → 3, N=7 → 4. Prevents minority-partition writes
# AND ensures routing table correctness during network partitions
# (the scalar signature mechanism alone is not sufficient — see docs/03-cluster-management.md).
member_count_quorum = 2   # for a 3-node cluster

# Was: false. Recommended ON if cross-DC or replica drift is a concern.
read_repair = true
```

### Sizing Quorums

For a cluster of N nodes:

| N | `replica_count` | `write_quorum` | `member_count_quorum` |
|---|-----------------|----------------|------------------------|
| 3 | 2 or 3          | 2              | 2                      |
| 5 | 3               | 2 or 3         | 3                      |
| 7 | 3               | 2 or 3         | 4                      |

### What These Defaults Lose

- **Latency**: Sync replication adds one network round-trip per write to the slowest backup in the quorum.
- **Availability under partition**: With `member_count_quorum = majority`, the minority side stops serving writes.
- **Memory**: Each additional replica multiplies cluster-wide memory usage by ~1× per replica (replica_count=2 → 2× total memory across the cluster).

These are conscious trade-offs against the **silent data-loss** mode of the shipped defaults. Pick the trade-off you can defend in a post-incident review.

### NTP Requirement

LWW conflict resolution uses primaries' wall-clock timestamps. Run NTP on every node and alert if drift exceeds 50ms. Without this, cross-primary LWW under partition may resolve in favor of the wrong write. See [Replication](04-replication.md#failure-mode-cross-primary-clock-skew).
