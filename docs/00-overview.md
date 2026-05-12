# Kamino - Distributed In-Memory Cache

## Overview

Kamino is a distributed, in-memory key/value store and cache library written in Rust. It is designed to be embedded directly into applications or run as a standalone server, providing a horizontally scalable caching layer with single-primary write serialization per partition and Last-Write-Wins conflict resolution (see [Consistency Model](#consistency-model) for the full guarantee).

## Design Goals

- **High Performance**: Concurrent reader paths via per-fragment RwLock, pre-allocated memory blocks, minimal allocations on the hot path
- **Horizontal Scalability**: Automatic data partitioning and rebalancing as nodes join/leave
- **Optional Fault Tolerance**: Primary-backup replication with configurable quorum. **Note**: Defaults ship with `replica_count = 1` (no replicas) for development simplicity — production deployments must override these (see [Configuration](09-configuration.md#production-recommended-defaults))
- **Operational Simplicity**: Zero external dependencies for coordination (no ZooKeeper, no etcd)
- **Dual Mode**: Embeddable library or standalone server
- **Rust Safety**: Leverage Rust's ownership model for memory safety without GC overhead

## Core Pillars

Kamino is built on three foundational subsystems:

1. **Gossip-based Cluster Membership** - SWIM protocol for decentralized failure detection and membership dissemination
2. **Bounded-Load Consistent Hashing** - Fair data partitioning with load balancing guarantees
3. **RESP Wire Protocol** - Redis-compatible protocol for client and inter-node communication

## Consistency Model

Kamino is a **primary-routed, eventually consistent** distributed cache:

- **Single-primary serialization**: Each partition has exactly one primary owner; all writes for a partition are serialized through that primary.
- **Replication**: Optional synchronous or asynchronous replication to backup owners (when `replica_count > 1`).
- **Conflict resolution**: Last-Write-Wins (LWW) by server-assigned timestamp. Concurrent writes to different primaries (during a network partition) resolve via LWW after the partition heals — **the loser is silently dropped**.
- **PACELC classification**: **PA/EL** — under Partition, prefers Availability; Else, prefers Latency (tunable via `replication_mode` and quorum settings). Strong consistency (linearizability) is **not** provided even with quorum settings, because LWW resolves conflicts non-deterministically with respect to client wall-clock order.
- **Split-brain protection**: Set `member_count_quorum` to a majority value to prevent minority partitions from accepting writes.

## Architecture at a Glance

```
+------------------------------------------------------------------+
|                          Kamino Node                              |
|                                                                   |
|  +-------------------+  +-------------------+  +---------------+  |
|  |   RESP Server     |  |  Gossip (SWIM)    |  |   Balancer    |  |
|  |   (TCP:3320)      |  |  (UDP/TCP:3322)   |  |   (periodic)  |  |
|  +--------+----------+  +--------+----------+  +-------+-------+  |
|           |                      |                      |          |
|  +--------v----------------------v----------------------v-------+  |
|  |                     Routing Table                            |  |
|  |              (Partition -> Owner mapping)                    |  |
|  +------+---------------------------------------------------+--+  |
|         |                                                   |      |
|  +------v--------------+                  +-----------------v--+   |
|  |  Primary Partitions |                  |  Backup Partitions |   |
|  |  [0..270]           |                  |  [0..270]          |   |
|  +------+--------------+                  +---------+----------+   |
|         |                                           |              |
|  +------v-------------------------------------------v-----------+  |
|  |                    Storage Engine (RamBlock)                  |  |
|  |          Append-only tables, pre-allocated buffers            |  |
|  +--------------------------------------------------------------+  |
+------------------------------------------------------------------+
```

## Key Data Structures

| Structure | Description |
|-----------|-------------|
| **DMap** (Distributed Map) | Primary key/value store with TTL, eviction, locking support |
| **Pub/Sub Channels** | Cluster-wide publish/subscribe messaging |
| **Distributed Locks** | Approximate key-level locks for coordination |

## Documentation Index

| Document | Description |
|----------|-------------|
| [Architecture](01-architecture.md) | Detailed system architecture and component interaction |
| [Consistent Hashing](02-consistent-hashing.md) | Partitioning strategy and data distribution |
| [Cluster Management](03-cluster-management.md) | Membership protocol, discovery, failure detection |
| [Replication](04-replication.md) | Primary-backup replication and quorum controls |
| [Storage Engine](05-storage-engine.md) | In-memory storage engine design |
| [Network Protocol](06-network-protocol.md) | Wire protocol and command set |
| [Concurrency Model](07-concurrency.md) | Locking strategies and concurrent access |
| [API Design](08-api-design.md) | Client API surface and usage patterns |
| [Configuration](09-configuration.md) | Configuration reference |
| [Distributed Locking](10-distributed-locking.md) | Lock algorithm and semantics |
| [Pub/Sub](11-pubsub.md) | Publish/subscribe system |
| [Failure Handling](12-failure-handling.md) | Partition recovery, split-brain, anti-entropy |
| [Kubernetes](13-kubernetes.md) | StatefulSet, headless Service, DNS-based discovery |
