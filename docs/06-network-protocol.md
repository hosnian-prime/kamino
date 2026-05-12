# Network Protocol

## Wire Protocol: RESP

Kamino uses the **RESP (Redis Serialization Protocol)** for all communication - both client-to-server and server-to-server. This provides:

- **Redis ecosystem compatibility**: Any RESP client library can communicate with Kamino
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
| `DM.DEL` | `DM.DEL dmap key [key ...]` | Delete one or more keys |
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
