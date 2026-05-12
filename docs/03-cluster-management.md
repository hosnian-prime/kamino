# Cluster Management

## Membership Protocol: SWIM

Kamino uses the **SWIM** (Scalable Weakly-consistent Infection-style process group Membership) protocol for decentralized cluster membership (Das, Gupta, Motivala, 2002). SWIM provides:

- **O(1) failure detection** — a node failure is first detected in an expected constant number of protocol periods (~1.58), independent of cluster size
- **O(log N) dissemination** — once detected, a membership change propagates to all members in O(log N) protocol periods via epidemic-style piggybacking
- **Configurable false-positive rate** — tunable via indirect probe count (`k`) and suspicion timeout; not a fixed guarantee but a configurable bound
- **No single point of failure** — fully decentralized

## Protocol Mechanics

### Failure Detection

SWIM uses a three-phase failure detection mechanism:

```
Phase 1: Direct Ping
  Node-A ──ping──> Node-B
  Node-A <──ack─── Node-B  ✓ (alive)

Phase 2: Indirect Ping (if direct fails)
  Node-A ──ping-req──> Node-C ──ping──> Node-B
  Node-A <────ack────── Node-C <──ack── Node-B  ✓ (alive via proxy)

Phase 3: Suspect (if indirect fails)
  Node-B enters "suspect" state
  After suspicion timeout → declared "dead"
  Membership update gossipped to all nodes
```

### Gossip Dissemination

Membership changes (join, leave, suspect, dead) are piggybacked on SWIM protocol messages. Each node maintains a list of recent events and attaches them to periodic protocol messages, ensuring **epidemic-style** dissemination.

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 3320 | TCP | RESP server (client + inter-node commands) |
| 3322 | TCP + UDP | SWIM protocol (membership gossip) |

## Node Discovery

Kamino supports multiple discovery mechanisms:

### 1. Static Peer List
```toml
[discovery]
peers = ["10.0.1.1:3322", "10.0.1.2:3322", "10.0.1.3:3322"]
```

### 2. DNS-Based Discovery
```toml
[discovery]
dns = "kamino.service.consul"
```

### 3. Plugin-Based Discovery
Pluggable discovery backends for:
- Consul
- Kubernetes (headless service)
- Cloud provider APIs (AWS, GCP)
- NATS
- Custom implementations via trait

```rust
#[async_trait]
pub trait DiscoveryPlugin: Send + Sync {
    /// Initialize the plugin
    async fn init(&mut self) -> Result<()>;
    /// Register this node
    async fn register(&self) -> Result<()>;
    /// Deregister this node
    async fn deregister(&self) -> Result<()>;
    /// Discover peer addresses
    async fn discover(&self) -> Result<Vec<SocketAddr>>;
    /// Shutdown the plugin
    async fn shutdown(&self) -> Result<()>;
}
```

Methods are async to support plugins that perform network I/O (e.g., Kubernetes Endpoints API, Consul HTTP API).

## Join Process

```
1. Node starts, loads configuration
2. Initializes SWIM protocol listener on discovery port
3. Contacts configured peers (or queries discovery plugin)
4. Retries up to max_join_attempts (default: 10) with join_retry_interval (default: 1s)
5. bootstrap_timeout (default: 10s) limits total join time
6. Once joined, receives routing table from coordinator
7. Node is ready to serve requests
```

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_join_attempts` | 10 | Maximum number of join retries |
| `join_retry_interval` | 1s | Delay between join attempts |
| `bootstrap_timeout` | 10s | Maximum time to wait for cluster formation |

## Graceful Leave

```
1. Node broadcasts leave intention via SWIM
2. Coordinator recalculates routing table (excluding leaving node)
3. Balancer migrates data from leaving node to new owners
4. leave_timeout (default: 5s) limits graceful shutdown time
5. SWIM listener shuts down
6. TCP server closes connections
```

## Coordinator

The **coordinator** is the oldest node in the cluster. The member list is sorted by `(birthdate ASC, id ASC)`; the first member is the coordinator.

### Tiebreaker

Two nodes can be assigned identical birthdates (clock resolution, simultaneous join). The deterministic tiebreaker is `Member.id` (u64, unique per node — generated from a random seed at boot, persisted in process memory):

```rust
fn coordinator(&self) -> &Member {
    // Members sorted by (birthdate, id), ascending
    &self.members[0]
}

fn sort_members(members: &mut Vec<Member>) {
    members.sort_by(|a, b| (a.birthdate, a.id).cmp(&(b.birthdate, b.id)));
}
```

### Election (Eventually Consistent)

There is no election protocol. Every node sorts its local member list and picks index 0. All nodes converge on the same answer **eventually**, but during SWIM convergence (e.g., immediately after a coordinator failure) different nodes may briefly disagree.

**During disagreement, two nodes may both attempt to push routing tables.** The resolution mechanism is the `RoutingTable.signature` field:

1. The coordinator increments `signature` on every topology change.
2. Receivers reject any routing table whose signature is ≤ their current signature.
3. After SWIM convergence (typically within a few probe intervals), only one node has the correct, latest member set and therefore produces the highest-signature table. The wrong coordinator stops seeing topology changes, so its signature stops advancing.

**Limitation — transient disagreement only.** The signature mechanism is a scalar clock (equivalent to a Lamport scalar). It is sufficient for brief SWIM convergence windows (sub-second) but **not for sustained network partitions**:

- During a partition, the minority-side coordinator may fall behind in signature even if it holds the "correct" pre-partition member list.
- The majority-side coordinator accumulates higher signatures (it sees more topology changes) and its routing table wins after heal — regardless of which side's member list is more complete.
- A scalar clock cannot distinguish concurrent coordinators (Fidge 1988, Mattern 1988). A `(coordinator_id, sequence)` epoch scheme would be more robust but is not currently implemented.

**Mitigation**: Set `member_count_quorum` to a majority value. This prevents the minority side from accepting writes **and** ensures the majority-side coordinator's routing table is authoritative (it has the quorum). Without `member_count_quorum`, the signature mechanism alone does not guarantee routing table correctness during sustained partitions.

This is **not Paxos/Raft** and provides no liveness guarantee during sustained network instability.

### Coordinator Responsibilities

1. **Build routing table**: Using consistent hashing, compute partition-to-owner mapping
2. **Push routing table**: Send to all members in parallel (concurrency bounded by CPU cores)
3. **Collect left-over data reports**: When pushing, members respond with partitions they hold
4. **Handle orphaned data**: Direct members to migrate data they should no longer own

### Coordinator Failover

When the coordinator fails:
1. SWIM detects the failure and removes the node from the member list
2. The second-oldest member becomes the new coordinator automatically
3. The new coordinator rebuilds and pushes the routing table
4. Normal operation resumes

## Member Representation

```rust
pub struct Member {
    /// Unique identifier, used as the tiebreaker when birthdates collide.
    /// Generated at boot from a random seed.
    pub id: u64,
    /// Display name
    pub name: String,
    /// RESP server address
    pub addr: SocketAddr,
    /// SWIM protocol address  
    pub discovery_addr: SocketAddr,
    /// Monotonic join timestamp
    pub birthdate: u64,
    /// Is this member the coordinator?
    pub is_coordinator: bool,
}
```

## Cluster Events

When enabled via `enable_cluster_events_channel`, the following events are published to a dedicated pub/sub channel (`cluster.events`):

| Event | Description |
|-------|-------------|
| `node-join` | A new node joined the cluster |
| `node-left` | A node left the cluster (graceful or failure) |
| `fragment-migration` | Data is being migrated between nodes |
| `fragment-received` | A node received migrated data |
