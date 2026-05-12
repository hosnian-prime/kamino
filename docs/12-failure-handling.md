# Failure Handling and Recovery

## Overview

Kamino is designed to handle node failures, network partitions, and cluster topology changes gracefully. This document covers the mechanisms for detecting, recovering from, and preventing data loss during failures.

## Node Failure Detection

### SWIM Protocol

The gossip-based SWIM protocol detects failures in three phases:

```
Phase 1: Direct Probe (every T interval)
  Node-A picks a random member Node-B
  Node-A ──ping──> Node-B
  If ACK received within timeout → Node-B is alive

Phase 2: Indirect Probe (if direct fails)
  Node-A picks K random members as proxies
  Node-A ──ping-req──> Node-C ──ping──> Node-B
  If any proxy receives ACK → Node-B is alive

Phase 3: Suspect → Dead
  If both direct and indirect probes fail:
  Node-B enters "suspect" state
  Suspicion disseminated via gossip
  After suspicion timeout → Node-B declared "dead"
  Dead status gossipped to all members
```

### Tunable Parameters

| Parameter | Effect |
|-----------|--------|
| Probe interval | How often each node probes a random peer |
| Probe timeout | How long to wait for a direct probe response |
| Indirect probes | Number of proxy nodes for indirect probing |
| Suspicion multiplier | How long a node stays in "suspect" before "dead" |

## Recovery After Node Failure

### Step 1: Membership Update

When a node is declared dead:
1. All surviving nodes remove the dead node from their member list
2. The coordinator (oldest surviving node) detects the topology change

### Step 2: Routing Table Rebuild

```
Coordinator:
  1. Remove dead node from consistent hash ring
  2. Recalculate partition ownership for all partitions
  3. Push new routing table to all surviving members (parallel)
```

### Step 3: Data Recovery from Backups

If `replica_count > 1`:

```
Before failure:
  Partition 42: Primary=Node-B(dead), Backup=Node-C

After routing table rebuild:
  Partition 42: Primary=Node-C (promoted from backup)

Node-C already has the data from backup replication.
No data migration needed for this partition.
```

If `replica_count == 1` (the **shipped default**):
- There are no backups. Data on the dead node is **permanently lost** the moment SWIM declares it dead.
- All affected partitions return `ErrKeyNotFound` for previously stored keys.
- **This is the failure mode of the default configuration.** Any production deployment that values its cached data must override to `replica_count >= 2`. See [Production-Recommended Defaults](09-configuration.md#production-recommended-defaults).

### Step 4: Balancer Migration

The balancer runs every 15 seconds and:
1. Scans all local partitions
2. Identifies fragments that should now be owned by a different node
3. Migrates fragments to their new owners
4. Removes local copies after successful migration

## Fragmented Partitions

During topology transitions, a partition may temporarily have **multiple owners**:

```
Partition 42:
  owners: [Node-C (new primary), Node-A (previous owner, has stale data)]
```

### Behavior During Fragmentation

| Operation | Behavior |
|-----------|----------|
| **Write** | Goes only to the new primary (Node-C) |
| **Read** | Checks new primary first, then falls back to previous owners |
| **Migration** | Balancer moves data from Node-A to Node-C with LWW merge |

### Resolution

The balancer eventually:
1. Transfers all data from previous owners to the new primary
2. Resolves conflicts using LWW (highest timestamp wins)
3. Removes the previous owner from the partition's owner list

## Ownership Transfer Protocol

When the balancer migrates a fragment:

```rust
fn move_fragment(partition_id: u32, dmap_name: &str, from: &Member, to: &Member) {
    // 1. Export: serialize all entries from the source fragment
    let payload = source_fragment.storage.export()?;

    // 2. Pack: create a FragmentPack with metadata
    let pack = FragmentPack {
        partition_id,
        partition_type: PartitionType::Primary,
        dmap_name: dmap_name.to_string(),
        payload,
    };

    // 3. Transfer: send to the new owner via RESP
    client.send(to, "INTERNAL.NODE.MOVEFRAGMENT", &pack)?;

    // 4. Receive: new owner validates ownership
    //    - Verifies it actually owns this partition (per routing table)
    //    - Creates or loads the destination fragment

    // 5. Merge: import with LWW conflict resolution
    destination_fragment.import_with_merge(&pack.payload)?;

    // 6. Cleanup: remove source fragment
    source_fragment.clear()?;

    // 7. Event: publish FragmentReceivedEvent (if events enabled)
}
```

### Merge Function

During import, entries are merged using LWW:

```rust
fn merge_entry(local: Option<&Entry>, remote: &Entry) -> MergeAction {
    match local {
        None => MergeAction::Insert(remote),
        Some(local) if remote.timestamp > local.timestamp => MergeAction::Replace(remote),
        Some(_) => MergeAction::Keep, // local is newer, ignore remote
    }
}
```

## Split-Brain Protection

### The Problem

A network partition can split the cluster into two (or more) groups, each believing the other is dead:

```
┌─────────────────┐     PARTITION     ┌─────────────────┐
│  Group A        │  ═══════════════  │  Group B        │
│  [Node-1, 2, 3] │                   │  [Node-4, 5]    │
│  Coordinator: 1 │                   │  Coordinator: 4 │
└─────────────────┘                   └─────────────────┘
```

Both groups elect a coordinator and continue accepting writes, leading to divergent state.

### The Solution: Member Count Quorum

Set `member_count_quorum` to a majority value:

```toml
# 5-node cluster: majority = 3
member_count_quorum = 3
```

**Behavior during partition:**
- Group A (3 nodes): `3 >= 3` → continues operating
- Group B (2 nodes): `2 < 3` → rejects all operations with `ErrClusterQuorum`

**After partition heals:**
- All nodes rejoin the cluster
- The coordinator rebuilds the routing table
- The balancer reconciles any divergent data (LWW)
- All nodes resume normal operation

### Quorum Check

```rust
fn check_quorum(&self) -> Result<()> {
    let member_count = self.members.len();
    if member_count < self.config.member_count_quorum {
        return Err(Error::ClusterQuorum);
    }
    Ok(())
}
```

This check runs before every DMap operation.

## Anti-Entropy Mechanisms

### 1. Read Repair

When enabled (`read_repair = true`):
- Every `GET` reads from primary + backups + previous owners
- Compares all versions by timestamp
- Propagates the winning version to stale replicas

**Cost**: Higher read latency (multiple network round-trips per read).
**Benefit**: Gradual consistency convergence without background processes.

### 2. Periodic Routing Table Push

The coordinator pushes the routing table to all members every `push_interval` (default: 60s), ensuring consistency even if gossip messages were lost.

### 3. Left-Over Data Reports

When the coordinator pushes the routing table, each member responds with a report of non-empty partitions it holds:

```rust
struct LeftOverDataReport {
    /// Partitions this node holds but should not own
    orphaned: Vec<(u32, String)>, // (partition_id, dmap_name)
}
```

The coordinator processes these reports and directs members to migrate orphaned data to the correct owners.

### 4. Balancer

The balancer runs every `trigger_interval` (default: 15s):

```
For each local partition:
  current_owner = routing_table.owner(partition_id)
  if current_owner != self:
    migrate_fragment(partition_id, self, current_owner)
```

This catches any data that should have been migrated but wasn't (e.g., due to transient failures during a previous migration).

## Lost-Update Under Partition: INCR/DECR

`INCR`, `DECR`, and `INCRBYFLOAT` are serialized at the partition primary but are not cluster-wide atomic during a network partition. Concrete scenario:

```
Initial state: counter:requests = 100

Network partition splits cluster:
  Group A: client_a issues INCR counter:requests 5   → stored as (105, ts=T1)
  Group B: client_b issues INCR counter:requests 3   → stored as (103, ts=T2)

Partition heals. LWW compares timestamps:
  If T2 > T1: stored value becomes 103 (Group A's +5 is silently lost)
  If T1 > T2: stored value becomes 105 (Group B's +3 is silently lost)
```

Either way, one client's increment vanishes. To prevent this:

- Set `member_count_quorum = ceil((N+1)/2)` so the minority side rejects all writes.
- Accept the availability trade-off: a partition that splits the cluster evenly leaves at least one side unable to write.
- For counters that absolutely must not lose updates and must remain available on both sides of a partition, a CRDT PN-Counter is required (not currently provided by Kamino).

## Error Handling Summary

| Scenario | Detection | Recovery | Data Impact |
|----------|-----------|----------|-------------|
| Node crash | SWIM (seconds) | Routing rebuild + backup promotion | None if replica_count > 1 |
| Network partition | SWIM + quorum check | Minority stops; majority continues | None with quorum |
| Coordinator failure | SWIM | Next-oldest becomes coordinator | None |
| Slow node | SWIM suspect state | Eventual removal if truly failed | None if replica_count > 1 |
| Transient network blip | Operation timeout + retry | Client retries | None |
| Storage corruption | N/A (in-memory only) | Node restart, data from backups | None if replica_count > 1 |
