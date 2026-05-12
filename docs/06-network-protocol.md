# Network Protocol

## Wire Protocol: RESP

Kamino uses the **RESP (Redis Serialization Protocol)** for all communication - both client-to-server and server-to-server. This provides:

- **Redis ecosystem compatibility**: Any RESP client library can communicate with Kamino
- **Simplicity**: Text-based protocol, easy to debug with standard tools
- **Performance**: Efficient binary-safe encoding with minimal overhead
- **Proven at scale**: Battle-tested protocol used by millions of Redis deployments

## Server Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bind_addr` | `0.0.0.0` | Address to bind the RESP server |
| `bind_port` | `3320` | Port for the RESP server |
| `keep_alive_period` | `300s` | TCP keep-alive period |
| `idle_close` | configurable | Close idle connections after this duration |

## Authentication

Simple password-based authentication (equivalent to Redis `requirepass`):

```
Client: AUTH mypassword
Server: +OK
```

No TLS built-in. For encrypted transport, use a TLS proxy (e.g., stunnel, envoy) or wrap at the network layer.

## Command Set

### DMap Commands

| Command | Syntax | Description |
|---------|--------|-------------|
| `DM.PUT` | `DM.PUT dmap key value [EX s] [PX ms] [EXAT ts] [PXAT ts] [NX\|XX]` | Store a key-value pair |
| `DM.GET` | `DM.GET dmap key` | Retrieve a value by key |
| `DM.DEL` | `DM.DEL dmap key [key ...]` | Delete one or more keys |
| `DM.EXPIRE` | `DM.EXPIRE dmap key seconds` | Set TTL in seconds |
| `DM.PEXPIRE` | `DM.PEXPIRE dmap key milliseconds` | Set TTL in milliseconds |
| `DM.INCR` | `DM.INCR dmap key delta` | Atomically increment integer value |
| `DM.DECR` | `DM.DECR dmap key delta` | Atomically decrement integer value |
| `DM.GETPUT` | `DM.GETPUT dmap key value` | Set value and return previous |
| `DM.INCRBYFLOAT` | `DM.INCRBYFLOAT dmap key delta` | Atomically increment float value |
| `DM.DESTROY` | `DM.DESTROY dmap` | Delete entire DMap |
| `DM.SCAN` | `DM.SCAN partID dmap cursor [MATCH pat] [COUNT n]` | Cursor-based iteration |
| `DM.LOCK` | `DM.LOCK dmap key deadline [timeout]` | Acquire distributed lock |
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
