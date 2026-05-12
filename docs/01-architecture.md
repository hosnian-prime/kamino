# Architecture

## System Components

Kamino consists of the following core components:

```
KaminoNode
├── Server          -- RESP TCP server for client and inter-node communication
├── Gossip          -- SWIM-based membership and failure detection
├── RoutingTable    -- Partition-to-owner mapping, maintained by coordinator
├── Balancer        -- Periodic data migration for rebalancing
├── DMapService     -- Distributed map operations (get, put, delete, lock, etc.)
├── PubSubService   -- Publish/subscribe message routing
├── Partitions
│   ├── Primary[0..N]  -- Primary data partitions
│   └── Backup[0..N]   -- Backup/replica partitions
└── StorageEngine   -- Pluggable storage backend (default: RamBlock)
```

## Component Responsibilities

### Server (RESP)
- Listens on TCP port (default: 3320)
- Parses RESP commands from clients and peer nodes
- Dispatches commands to appropriate service handlers
- Tracks connection metrics (bytes read/written, active connections, command counts)
- Supports password-based authentication

### Gossip (SWIM Protocol)
- Manages cluster membership via UDP/TCP (default port: 3322)
- Detects node failures through ping/indirect-ping/suspect mechanism
- Disseminates membership changes via gossip protocol
- Provides sorted member list (by join time / birthdate)

### Routing Table
- Maps each partition ID to its primary owner and backup owners
- Built and distributed by the **coordinator** (the oldest node in the cluster)
- Uses consistent hashing with bounded loads for partition assignment
- Pushed to all members periodically (default: 60 seconds) and on topology changes

### Balancer
- Runs periodically (default: every 15 seconds)
- Detects fragments that no longer belong on the current node
- Migrates data to the correct owner based on the current routing table
- Handles ownership transfer with conflict resolution (LWW merge)

### DMap Service
- Manages the lifecycle of distributed maps
- Routes operations to the correct partition owner
- Executes local operations or forwards to remote nodes
- Manages per-DMap configuration (TTL, eviction, etc.)

### Pub/Sub Service
- Maintains subscription registry (channel -> connections)
- Supports exact and pattern-based (glob) subscriptions
- Propagates published messages across all cluster nodes

## Coordinator Election

There is **no Raft or Paxos consensus**. The coordinator is simply the oldest member in the cluster, determined by birthdate (join timestamp). When the coordinator leaves or fails, the second-oldest member automatically assumes the role.

**Eventual coordinator convergence**: Because membership is eventually consistent via SWIM, two nodes may briefly consider themselves the coordinator during a membership transition. Routing tables carry a monotonic `signature` field; nodes accept the table with the higher signature and reject older versions. Coordinator tiebreaker is `(birthdate ASC, member_id ASC)` to ensure deterministic agreement when birthdates collide. See [Cluster Management](03-cluster-management.md#coordinator).

**Coordinator responsibilities:**
1. Build the routing table using consistent hashing
2. Push the routing table to all cluster members
3. Collect and process left-over data reports from members
4. Trigger partition redistribution on topology changes

## Request Flow

### Write Path (Put)

```
Client
  │
  ├─ 1. Hash(dmap_name, key) → partition_id
  ├─ 2. Lookup partition_id → primary_owner (from routing table)
  ├─ 3. Send PUT to primary_owner
  │
Primary Owner
  ├─ 4. Acquire fragment write lock
  ├─ 5. Serialize entry → storage engine
  ├─ 6. Replicate to backup owners (sync or async)
  ├─ 7. Check write quorum
  └─ 8. Return success/failure
```

### Read Path (Get)

```
Client
  │
  ├─ 1. Hash(dmap_name, key) → partition_id
  ├─ 2. Lookup partition_id → primary_owner
  ├─ 3. Send GET to primary_owner
  │
Primary Owner
  ├─ 4. Acquire fragment read lock
  ├─ 5. Lookup key in storage engine
  ├─ 6. (Optional) Read-repair: compare with backups, resolve via LWW
  └─ 7. Return value or ErrKeyNotFound
```

## Partition Layout

Each node maintains two partition sets:

- **Primary Partitions**: Hold the authoritative copy of data
- **Backup Partitions**: Hold replica copies from other nodes' primary partitions

A partition contains zero or more **fragments**. Each fragment corresponds to a single DMap's data within that partition:

```
Partition[42]
├── Fragment("dmap.sessions")    → StorageEngine instance
├── Fragment("dmap.user_cache")  → StorageEngine instance
└── Fragment("dmap.rate_limits") → StorageEngine instance
```

## Embedded vs Standalone

### Embedded Mode
```rust
let node = Kamino::new(config).await?;
node.start().await?;
let client = node.embedded_client();
// Operations execute in-process, no network I/O
```

### Standalone Mode
```
$ kamino-server --config /etc/kamino.toml
```
Clients connect via TCP using any RESP-compatible client library.

Both modes expose the identical `Client` trait interface.
