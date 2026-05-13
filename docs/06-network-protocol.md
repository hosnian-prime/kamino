# Network Protocol

## Wire Protocol: RESP

Kamino uses the **RESP (Redis Serialization Protocol)** for all communication - both client-to-server and server-to-server. This provides:

- **RESP transport compatibility**: Any RESP client library can communicate with Kamino using its raw/generic command API (e.g., `execute_command` in redis-py, `sendCommand` in node-redis). **Standard Redis commands (SET, GET, DEL) are not supported** — use the DM.* command set instead. Pub/Sub commands (SUBSCRIBE, PUBLISH, etc.) use standard Redis syntax.
- **Simplicity**: Text-based protocol, easy to debug with standard tools
- **Performance**: Efficient binary-safe encoding with minimal overhead
- **Proven at scale**: Battle-tested protocol used by millions of Redis deployments

### Protocol Version

Kamino implements **RESP2** by default. Clients may upgrade to **RESP3** by sending `HELLO 3` immediately after `AUTH`; this enables RESP3 push frames for pub/sub delivery (cleaner separation from request/response). All command-level encoding remains RESP2-compatible.

## Server Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bind_addr` | `0.0.0.0` | Address to bind the RESP server |
| `bind_port` | `3320` | Port for the RESP server |
| `keep_alive_period` | `300s` | TCP keep-alive period |
| `idle_close` | configurable | Close idle connections after this duration |

## Authentication

### Client Authentication

Simple password-based authentication (equivalent to Redis `requirepass`):

```
Client: AUTH mypassword
Server: +OK
```

Configured via `auth.password` in the config file.

### Inter-Node Authentication

Server-to-server messages carry a separate `cluster_secret`. This prevents an arbitrary RESP client (which only has the client password) from issuing `INTERNAL.NODE.*` commands. The internal client attaches the cluster secret to every inter-node connection at handshake time.

### Wire Security: No Built-in TLS

There is no built-in TLS for either client or inter-node traffic. **Both the client AUTH password and the inter-node cluster_secret are sent in plaintext over the network.** For any deployment outside a trusted private network, terminate TLS at a proxy (stunnel, envoy, AWS NLB with TLS, Kubernetes service mesh with mTLS) and treat the Kamino port as accessible only from `127.0.0.1` of the proxy host.

## Command Set

### DMap Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `DM.PUT` | `DM.PUT dmap key value [EX s] [PX ms] [EXAT ts] [PXAT ts] [NX\|XX] [TS unix-nanos]` | Store a key-value pair (TS overrides server-assigned timestamp; see Put Options) |
| `DM.GET` | `DM.GET dmap key` | Retrieve a value by key |
| `DM.DEL` | `DM.DEL dmap key [key ...]` | Delete one or more keys. Cross-partition keys are fanned out server-side; see [Multi-Key Operations](#multi-key-operations) for semantics. |
| `DM.EXPIRE` | `DM.EXPIRE dmap key seconds` | Set TTL in seconds |
| `DM.PEXPIRE` | `DM.PEXPIRE dmap key milliseconds` | Set TTL in milliseconds |
| `DM.INCR` | `DM.INCR dmap key delta` | Increment integer value (serialized at partition primary; lost-update under partition — see [Replication](04-replication.md#incrdecr-lost-update-warning)) |
| `DM.DECR` | `DM.DECR dmap key delta` | Decrement integer value (same caveat as DM.INCR) |
| `DM.GETPUT` | `DM.GETPUT dmap key value` | Set value and return previous |
| `DM.INCRBYFLOAT` | `DM.INCRBYFLOAT dmap key delta` | Increment float value (same caveat as DM.INCR) |
| `DM.DESTROY` | `DM.DESTROY dmap` | Delete entire DMap |
| `DM.SCAN` | `DM.SCAN partID dmap cursor [MATCH pat] [COUNT n]` | Cursor-based iteration **scoped to a single partition**. Clients iterate the whole DMap by scanning each partition ID (0..partition_count). Cursors are opaque server-assigned tokens; a cursor returned by partition P is invalidated if partition P migrates between scan calls (server returns `ErrInvalidCursor` — restart the scan for that partition). |
| `DM.LOCK` | `DM.LOCK dmap key deadline [timeout]` | Acquire distributed lock (the `timeout` form is the safe variant; see [Distributed Locking](10-distributed-locking.md#lock-api-safety)) |
| `DM.UNLOCK` | `DM.UNLOCK dmap key token` | Release distributed lock |
| `DM.LOCKLEASE` | `DM.LOCKLEASE dmap key token seconds` | Extend lock lease |
| `DM.PLOCKLEASE` | `DM.PLOCKLEASE dmap key token milliseconds` | Extend lock lease (ms) |

### Put Options

| Flag | Description |
|------|-------------|
| `EX seconds` | Set expiry in seconds |
| `PX milliseconds` | Set expiry in milliseconds |
| `EXAT unix-seconds` | Set absolute expiry (Unix timestamp) |
| `PXAT unix-milliseconds` | Set absolute expiry (Unix timestamp ms) |
| `NX` | Only set if key does **not** exist |
| `XX` | Only set if key **already** exists |
| `TS unix-nanos` | Override the server-assigned LWW timestamp. Use with care — see [LWW](04-replication.md#timestamp-source) |

### Multi-Key Operations

`DM.DEL` is the only command in the DMap set that accepts multiple keys. Unlike Redis Cluster — which rejects multi-key commands that cross slots with `CROSSSLOT` and forces clients to use hash tags or split requests — Kamino fans out internally:

1. The receiving node groups the input keys by partition primary using the local routing table.
2. For each distinct primary, the node issues a single `DM.DEL` to that primary covering all keys mapped there. Local-owned keys are deleted in-process.
3. Per-primary RPCs run in parallel, bounded by the per-peer inflight limit (see [Inter-Node Communication](#inter-node-communication)).
4. The reply is the **sum of successfully deleted live keys** across all primaries.

**Semantics:**

- **Not atomic across partitions.** A partial failure (one primary unreachable, quorum lost on one shard) is reported as a `+PARTIAL` reply carrying `(deleted_count, first_error)`. Successful deletions on reachable primaries are **not** rolled back.
- **Quorum check is per-primary.** If `member_count_quorum` is not satisfied on the receiving node, the entire request fails before any fan-out. If it is satisfied on the receiver but a downstream primary's view differs, that primary's deletes return `ErrClusterQuorum`.
- **Reordering.** The order in which individual deletes hit each primary is unspecified. Callers that need ordering must serialize at the application level.
- **No `DM.MGET`/`DM.MPUT`.** Read and write multi-key forms are deliberately not provided. A `pipeline` over single-key operations is the supported pattern; it makes the per-key error visible and avoids hiding cross-partition cost behind a single command.

If you want Redis Cluster semantics (single-slot guarantee, fail loudly on cross-slot), set `multi_key_strict = true` under `[network]` — the server then rejects multi-key requests that span partitions with `ErrCrossPartition`.

### Pub/Sub Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `SUBSCRIBE` | `SUBSCRIBE channel [channel ...]` | Subscribe to channels |
| `PSUBSCRIBE` | `PSUBSCRIBE pattern [pattern ...]` | Subscribe to glob patterns |
| `PUBLISH` | `PUBLISH channel message` | Publish message to channel |
| `UNSUBSCRIBE` | `UNSUBSCRIBE [channel ...]` | Unsubscribe from channels |
| `PUNSUBSCRIBE` | `PUNSUBSCRIBE [pattern ...]` | Unsubscribe from patterns |
| `PUBSUB CHANNELS` | `PUBSUB CHANNELS [pattern]` | List active channels |
| `PUBSUB NUMSUB` | `PUBSUB NUMSUB [channel ...]` | Count subscribers per channel |
| `PUBSUB NUMPAT` | `PUBSUB NUMPAT` | Count pattern subscriptions |

### Cluster Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `CLUSTER.ROUTINGTABLE` | `CLUSTER.ROUTINGTABLE` | Get current routing table |
| `CLUSTER.MEMBERS` | `CLUSTER.MEMBERS` | List cluster members |
| `CLUSTER.READY` | `CLUSTER.READY` | Returns `+OK` only if this node has: (1) joined the SWIM cluster, (2) received a routing table with `signature > 0`, (3) `member_count >= member_count_quorum`. Otherwise returns an error. Suitable for Kubernetes readiness probes. |

### Internal Commands (Server-to-Server)

| Command | Description |
|---------|-------------|
| `INTERNAL.NODE.MOVEFRAGMENT` | Migrate a fragment to this node |
| `INTERNAL.NODE.UPDATEROUTING` | Push routing table update |
| `INTERNAL.NODE.LENGTHOFPART` | Query partition size |

### Utility Commands

| Command | Description |
|---------|-------------|
| `PING` | Health check |
| `AUTH` | Authenticate connection |
| `HELLO` | Protocol version handshake. `HELLO 3` upgrades the connection to RESP3 (enables push frames for pub/sub). |
| `QUIT` | Close the connection gracefully |
| `STATS` | Node statistics and metrics |

## Inter-Node Communication

Server-to-server communication uses the same RESP protocol. When a node receives a command for a key it doesn't own, it forwards the command to the correct node using an internal RESP client.

```
Client ──DM.PUT──> Node-A (not owner)
                     │
                     ├── Route lookup: partition 42 → Node-B
                     │
                     └──DM.PUT (internal)──> Node-B (owner)
                                               │
                     <──────response────────────┘
                     │
Client <──OK───────┘
```

### Connection Pool

Each node maintains a small **per-peer connection pool**. Connections are pipelined: a single TCP connection carries many in-flight RESP requests, demuxed by response order. This matches Cassandra's multiplexed driver model (per-node pool, many concurrent streams per connection) rather than the one-request-per-connection RDBMS pattern.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `internode_pool_size` | 4 | TCP connections per peer |
| `internode_inflight_per_conn` | 256 | Maximum in-flight requests per connection before queueing |
| `internode_connect_timeout` | 500ms | New-connection establishment timeout |
| `internode_request_timeout` | 2s | Per-request deadline (forwarded RPC) |
| `internode_reconnect_backoff` | 100ms..5s | Exponential reconnect on failure |

Connections are evicted from the pool when the peer leaves SWIM membership; in-flight requests on those connections receive `ErrServerGone` and the caller is expected to retry against the new owner.

### Backpressure

The forward path can become a bottleneck if a downstream peer is slow. Three mechanisms cap memory growth:

1. **TCP socket buffers.** When the peer's kernel inbound buffer fills, the receiver's TCP advertised window shrinks. Tokio's reader stops pulling from the socket, which propagates to the writer on the sending side. This is the same mechanism Cassandra uses via Netty's `autoread=false`.
2. **Per-peer inflight cap.** Each peer connection has a bounded queue (`internode_inflight_per_conn`). When the queue is full, the forwarder applies internal pushback: the next forward call awaits a permit. The caller's RESP server keeps reading from the client, but commands queue at the forward stage rather than blocking the per-connection read loop. This avoids head-of-line blocking *between* clients of the same node, at the cost of bounded latency growth for the slow-peer destination.
3. **Per-peer request timeout.** Any forwarded RPC older than `internode_request_timeout` is cancelled and returns `ErrTimeout` to the originating client. Timed-out RPCs free their permit immediately.

Bounded queues are mandatory in any peer-to-peer forwarding system; an unbounded queue turns a transient slow peer into an OOM. The defaults above are conservative for in-DC traffic; cross-DC deployments should raise `internode_request_timeout` to at least `2 × RTT_p99`.

### Failure Handling on the Forward Path

| Failure | Behavior |
|---------|----------|
| Peer connection error mid-flight | All in-flight requests on that connection return `ErrServerGone`. Pool reopens the connection in the background with `internode_reconnect_backoff`. |
| Peer marked dead by SWIM | Pool drained, connections closed, queued requests returned with `ErrServerGone`. Routing table refresh routes future requests to the new owner. |
| `internode_request_timeout` exceeded | Request returns `ErrTimeout`. The originating client decides whether to retry (typically yes for idempotent ops, with caller-supplied jitter). |
| Routing-table staleness | If the forwarded RPC returns `ErrMoved` (peer no longer owns the partition), the local node refreshes the routing table and retries once. Repeated `ErrMoved` after refresh returns the error to the client. |

## Metrics

The server tracks the following metrics:

| Metric | Description |
|--------|-------------|
| `commands_total` | Total commands processed (by command name, status) |
| `connections_total` | Total connections established |
| `current_connections` | Currently active connections |
| `written_bytes_total` | Total bytes written to clients |
| `read_bytes_total` | Total bytes read from clients |

## Serialization Formats

| Context | Format |
|---------|--------|
| Wire protocol | RESP (Redis Serialization Protocol) |
| Entry storage | Custom binary (29-byte header + key + value) |
| Routing table transfer | MessagePack |
| Fragment migration | Custom binary (storage engine export format) |
