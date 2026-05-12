# Replication Strategy

## Model: Primary-Backup

Kamino uses **primary-backup replication**. Each partition has exactly one primary owner and zero or more backup owners. All writes go to the primary; backups receive replicated data.

```
Write Request
     │
     v
[Primary Owner]  ──replicate──>  [Backup Owner #1]
                 ──replicate──>  [Backup Owner #2]
```

> **⚠️ Default `replica_count = 1` is NOT fault-tolerant.** With the shipped default, each partition has only one copy and no backups. If the owning node fails, all data in that partition is **permanently lost**. For any production deployment, set `replica_count >= 2` and `write_quorum >= 2`. See [Production-Recommended Defaults](09-configuration.md#production-recommended-defaults).

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `replica_count` | 1 | Total copies (1 = primary only, 2 = primary + 1 backup) |
| `replication_mode` | `Sync` | `Sync` (0) or `Async` (1) |
| `write_quorum` | 1 | Minimum successful writes before acknowledging |
| `read_quorum` | 1 | Minimum reads before returning a value |
| `member_count_quorum` | 1 | Minimum cluster members to accept operations |

## Backup Owner Selection

Backup owners are determined by `get_closest_n_for_partition()` on the consistent hash ring, which returns the N closest nodes **excluding** the primary owner:

```rust
fn backup_owners(&self, partition_id: u32) -> Vec<Member> {
    self.ring
        .get_closest_n_for_partition(partition_id, self.config.replica_count)
        .into_iter()
        .skip(1) // skip primary
        .collect()
}
```

## Replication Modes

### Synchronous Replication (Default)

The write blocks until all backup owners acknowledge the replication:

```
Client ──PUT──> Primary
                  │
                  ├──PUT──> Backup-1  ──ACK──┐
                  ├──PUT──> Backup-2  ──ACK──┤
                  │                          │
                  │<── quorum met ───────────┘
                  │
Client <──OK───┘
```

The operation succeeds when `write_quorum` nodes (including the primary) have acknowledged the write. If the quorum is not met, the operation returns an error.

**Trade-off**: Higher latency, stronger consistency guarantees.

### Asynchronous Replication

The primary writes locally and sends replication requests without waiting:

```
Client ──PUT──> Primary
                  │
                  ├──PUT(fire-and-forget)──> Backup-1
                  ├──PUT(fire-and-forget)──> Backup-2
                  │
Client <──OK───┘
```

**Trade-off**: Lower latency, risk of data loss if primary fails before replication completes.

## Write Quorum

`write_quorum` defines the minimum number of successful writes (across primary + backups) required before returning success to the client.

| `write_quorum` | `replica_count` | Behavior |
|-----------------|-----------------|----------|
| 1 | 2 | Primary write sufficient (default) |
| 2 | 2 | Primary + 1 backup must succeed |
| 2 | 3 | Primary + 1 backup must succeed |
| 3 | 3 | All replicas must succeed |

Setting `write_quorum = replica_count` gives the strongest consistency but highest latency.

## Read Quorum

`read_quorum` defines how many copies are read and compared before returning a value.

| `read_quorum` | Behavior |
|---------------|----------|
| 1 | Read from primary only (default, fastest) |
| 2+ | Read from primary + backups, compare versions |

When `read_quorum > 1`, the system reads from multiple replicas and selects the value with the highest timestamp (LWW).

## Read Repair

When `read_repair` is enabled:

1. **Get** reads from primary, all previous owners, and all backup owners
2. Compares all versions by timestamp
3. Selects the version with the highest timestamp (LWW)
4. Propagates the winning version to all stale replicas
5. Returns the winning version to the client

```rust
fn get_with_read_repair(&self, key: &str) -> Result<Entry> {
    let versions = vec![];
    versions.push(self.get_local(key)?);          // primary
    versions.extend(self.get_from_backups(key)?);  // backups
    versions.extend(self.get_from_previous(key)?); // previous owners

    let winner = versions.into_iter()
        .max_by_key(|v| v.timestamp)
        .ok_or(Error::KeyNotFound)?;

    // Propagate winner to stale replicas
    self.repair_stale_replicas(&winner)?;

    Ok(winner)
}
```

**Use case**: Eventual consistency convergence after network partitions or node failures.

## Member Count Quorum (Split-Brain Protection)

`member_count_quorum` sets the minimum number of cluster members required for the node to accept operations. Setting this to a majority value prevents split-brain scenarios:

```
Cluster: 5 nodes
member_count_quorum = 3 (majority)

Network partition:
  Partition A: [Node-1, Node-2, Node-3]  → 3 >= 3, continues operating
  Partition B: [Node-4, Node-5]          → 2 < 3, rejects all operations
```

When quorum is not met, all DMap operations return `ErrClusterQuorum`.

## Conflict Resolution: Last-Write-Wins (LWW)

All entries carry a `timestamp` field used for conflict resolution. Conflicts arise during read-repair, merge after partition heal, ownership transfer between primaries, or fragmented-partition resolution. The entry with the **highest timestamp** wins; the loser is discarded.

```rust
fn merge(local: &Entry, remote: &Entry) -> &Entry {
    if remote.timestamp > local.timestamp { remote } else { local }
}
```

### Timestamp Source

- **Default**: Server-assigned by the **partition primary** at write acceptance, using the local monotonic clock anchored to wall-clock time. This means a single primary's writes are totally ordered by timestamp.
- **Optional client override**: The `PutOptions.timestamp: Option<i64>` field lets callers supply a timestamp explicitly (for replication tools, replay, or external HLC integration). Use with care — a client that writes a far-future timestamp will block all subsequent writes for that key until the timestamp is exceeded.

### Failure Mode: Cross-Primary Clock Skew

Under a network partition, both sides may accept writes through their own primaries. On heal, LWW compares timestamps from the two primaries' clocks. If the clocks are skewed (e.g., NTP failure), the side with the faster clock wins **regardless of which write happened later in real time**. Mitigations:

- Require NTP on all nodes; alert on drift > 50ms.
- For partition-safety, set `member_count_quorum = majority` — the minority side rejects writes, eliminating cross-primary conflicts entirely.
- For application-critical counters, see the warning in [INCR/DECR Lost-Update](#incrdecr-lost-update-warning) below.

### LWW Silently Drops Concurrent Writes

LWW is a **lost-write** strategy by design: if two clients write to the same key at the same nanosecond (or under clock skew, in any order), one write disappears with no error returned to the loser. This is acceptable for cache workloads (most-recent value usually wins) but is **not** suitable for ledger-style accounting, financial state, or any workload where lost writes are a correctness violation.

### INCR/DECR Lost-Update Warning

`INCR`, `DECR`, and `INCRBYFLOAT` are serialized at the partition primary via key-level locks — atomic on a single primary. They are **not atomic across the cluster** under partition. With `member_count_quorum < majority`, two partitioned primaries may each accept increments; on heal, LWW keeps only the entry with the higher timestamp, so the other partition's increments are **lost** (not added, simply discarded).

For partition-safe counters, set `member_count_quorum` to a majority value, accepting the trade-off that minority partitions reject all writes. A CRDT PN-Counter is not yet implemented and would be the proper solution for fully partition-tolerant counters.
