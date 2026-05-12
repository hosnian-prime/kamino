# Consistent Hashing and Data Partitioning

## Overview

Kamino uses **consistent hashing with bounded loads** to distribute data across cluster nodes. This algorithm, based on research by Mirrokni et al., ensures that no single node receives disproportionately more data than others, while maintaining the stability properties of traditional consistent hashing.

## Algorithm

### Bounded-Load Consistent Hashing

Traditional consistent hashing can lead to uneven load distribution. Bounded-load consistent hashing adds a constraint: each node's load cannot exceed `average_load * load_factor`. When a node would exceed this threshold, the key is assigned to the next node on the ring that has capacity.

**Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `partition_count` | 271 | Total number of partitions (should be prime) |
| `replication_factor` | 20 | Virtual nodes per physical member on the hash ring |
| `load_factor` | 1.25 | Maximum load ratio relative to average |

### Why 271 Partitions?

- Prime number for better hash distribution
- Small enough for low overhead in routing table
- Large enough for reasonable distribution across typical cluster sizes (3-50 nodes)
- Configurable for larger deployments

## Hash Function

**Default: xxHash (XXH64)** - a non-cryptographic, extremely fast 64-bit hash function.

```rust
pub trait Hasher: Send + Sync {
    fn sum64(&self, data: &[u8]) -> u64;
}

pub struct XxHasher;

impl Hasher for XxHasher {
    fn sum64(&self, data: &[u8]) -> u64 {
        xxhash::xxh64(data, 0)
    }
}
```

The hasher is pluggable - any implementation of the `Hasher` trait can be provided via configuration.

## Key-to-Partition Mapping

```
partition_id = hash(dmap_name + key) % partition_count
```

The DMap name and key are concatenated before hashing. This ensures keys with the same name in different DMaps are independently distributed.

### Example

```
dmap_name = "sessions"
key = "user:12345"
hash_input = "sessionsuser:12345"
hkey = xxh64(hash_input) = 0x7A3F...
partition_id = hkey % 271 = 142
```

## Partition-to-Owner Mapping

The consistent hash ring maps partition IDs to physical nodes:

```
Ring:
  Node-A: virtual nodes at positions [v0, v1, ..., v19]
  Node-B: virtual nodes at positions [v0, v1, ..., v19]
  Node-C: virtual nodes at positions [v0, v1, ..., v19]

Partition 142 → closest virtual node → Node-B (primary owner)
                next closest node    → Node-C (backup owner #1)
                next closest node    → Node-A (backup owner #2)
```

## Rebalancing on Topology Change

When a node joins or leaves:

1. **Coordinator detects topology change** (via gossip membership event)
2. **Rebuild hash ring** with new member set
3. **Recalculate partition ownership** for all partitions
4. **Push new routing table** to all members (parallel, bounded by CPU count)
5. **Balancer migrates data** from old owners to new owners

### Minimal Disruption

With consistent hashing, only `K/N` partitions need to move on average when a node is added/removed (where K = partition count, N = node count). This is near-optimal.

### Fragmented Partitions

During rebalancing, a partition may temporarily have **multiple owners** (previous + new). This is called a "fragmented partition":

```
Partition 42:
  owners: [Node-C (new primary), Node-A (previous owner)]
```

- Writes go only to the new primary
- Reads check the new primary first, then fall back to previous owners
- The balancer eventually migrates all data to the new primary and removes previous owners

## Routing Table Structure

```rust
pub struct RoutingTable {
    /// Mapping from partition ID to list of owners (newest first).
    /// During topology transitions, a partition may have multiple owners (fragmented).
    primary: HashMap<u32, Vec<Member>>,
    /// Mapping from partition ID to list of backup owners (closest N on the ring, excluding the primary).
    backup: HashMap<u32, Vec<Member>>,
    /// Current cluster members, sorted by (birthdate ASC, id ASC). Index 0 is the coordinator.
    members: Vec<Member>,
    /// Monotonic version assigned by the coordinator. Incremented on every topology change.
    /// Nodes and clients compare signatures to detect stale routing tables: a received
    /// routing table is accepted only if its signature is strictly greater than the locally
    /// stored one. This is the conflict resolution mechanism when multiple nodes briefly
    /// believe themselves to be the coordinator during SWIM convergence.
    signature: u64,
}
```

The routing table is serialized with MessagePack for efficient wire transfer. The `signature` field is the single source of truth for table freshness — any handler that receives a `CLUSTER.ROUTINGTABLE` or `INTERNAL.NODE.UPDATEROUTING` message ignores it if its signature is ≤ the local signature, which keeps stale broadcasts from corrupting the local view during a coordinator transition.

## Client-Side Routing

Clients cache the routing table locally and route requests directly to the correct node:

```rust
fn route(&self, dmap: &str, key: &str) -> &Member {
    let hkey = self.hasher.sum64(format!("{}{}", dmap, key).as_bytes());
    let part_id = hkey % self.partition_count;
    &self.routing_table.primary[&part_id][0] // first = current primary
}
```

The client refreshes its routing table periodically (default: 60 seconds) or when it receives a `MOVED` error.
