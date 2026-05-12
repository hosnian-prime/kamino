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

All entries carry a client-attached timestamp. When conflicts arise (e.g., during read-repair, merge after partition heal, or ownership transfer), the entry with the **highest timestamp** wins:

```rust
fn merge(&self, local: &Entry, remote: &Entry) -> &Entry {
    if remote.timestamp > local.timestamp {
        remote
    } else {
        local
    }
}
```

This is a simple, partition-tolerant conflict resolution strategy suitable for caching workloads where "most recent value" is typically the correct value.
