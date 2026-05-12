# Pub/Sub System

## Overview

Kamino provides a cluster-wide publish/subscribe messaging system. Messages published on any node are automatically propagated to subscribers on all nodes in the cluster.

## Architecture

```
Publisher (any node)
     │
     ├── PUBLISH "events" "payload"
     │
     v
[Local Node]
     ├── Deliver to local subscribers of "events"
     └── Forward to all other cluster nodes
           │
           ├──> [Node-B] → deliver to local subscribers
           ├──> [Node-C] → deliver to local subscribers
           └──> [Node-D] → deliver to local subscribers
```

### Subscription Registry

Each node maintains a B-tree indexed by `(is_pattern, channel_name, connection_id)`:

```rust
struct PubSubRegistry {
    /// B-tree of subscriptions: (pattern_flag, channel, conn_id) → Subscriber
    subscriptions: BTreeMap<(bool, String, u64), Subscriber>,
}
```

This indexing allows efficient lookup for both exact channel matches and pattern-based subscriptions.

## Subscription Types

### Exact Channel Subscription

```
SUBSCRIBE channel1 channel2 channel3
```

Subscribes to exact channel names. Messages published to `channel1` are delivered only to subscribers of `channel1`.

### Pattern Subscription

```
PSUBSCRIBE events.* user.login.* *.critical
```

Subscribes to glob patterns. A message published to `events.order.created` would be delivered to subscribers of `events.*`.

**Supported glob syntax:**
- `*` - match any sequence of characters
- `?` - match any single character
- `[abc]` - match any character in the set
- `[^abc]` - match any character NOT in the set

## Commands

### SUBSCRIBE
```
SUBSCRIBE channel [channel ...]
```
Subscribe to one or more channels. The connection enters pub/sub mode.

### PSUBSCRIBE
```
PSUBSCRIBE pattern [pattern ...]
```
Subscribe to one or more glob patterns.

### PUBLISH
```
PUBLISH channel message
```
Publish a message to a channel. Returns the number of subscribers that received the message.

### UNSUBSCRIBE
```
UNSUBSCRIBE [channel ...]
```
Unsubscribe from channels. If no channels specified, unsubscribe from all.

### PUNSUBSCRIBE
```
PUNSUBSCRIBE [pattern ...]
```
Unsubscribe from patterns. If no patterns specified, unsubscribe from all.

### PUBSUB CHANNELS
```
PUBSUB CHANNELS [pattern]
```
List active channels, optionally filtered by glob pattern.

### PUBSUB NUMSUB
```
PUBSUB NUMSUB [channel ...]
```
Get subscriber count for specific channels.

### PUBSUB NUMPAT
```
PUBSUB NUMPAT
```
Get the total number of active pattern subscriptions.

## Pub/Sub Mode Restrictions

Once a connection enters pub/sub mode (by issuing `SUBSCRIBE` or `PSUBSCRIBE`), only the following commands are allowed:

- `SUBSCRIBE`
- `PSUBSCRIBE`
- `UNSUBSCRIBE`
- `PUNSUBSCRIBE`
- `PING`
- `QUIT`

All other commands return an error.

## Message Format

Messages delivered to subscribers follow the RESP array format:

### Regular message:
```
*3
$7
message
$6
events
$13
{"key":"value"}
```

### Pattern message:
```
*4
$8
pmessage
$8
events.*
$14
events.created
$13
{"key":"value"}
```

## Rust API

```rust
// Publisher
let client = kamino.embedded_client();
let pubsub = client.new_pubsub(Default::default())?;
pubsub.publish("events.order", b"order_created").await?;

// Subscriber
let subscription = pubsub.subscribe(&["events.order", "events.payment"]).await?;

loop {
    match subscription.recv().await {
        Ok(message) => {
            println!("Channel: {}, Payload: {:?}", message.channel, message.payload);
        }
        Err(e) => break,
    }
}
```

## Cluster Event Channel

When `enable_cluster_events_channel = true`, Kamino publishes internal cluster events to the `cluster.events` channel:

```rust
let sub = pubsub.subscribe(&["cluster.events"]).await?;
// Events like:
// { "type": "node-join",          "member": "node-3", "addr": "10.0.1.3:3320" }
// { "type": "node-left",          "member": "node-2", "addr": "10.0.1.2:3320" }
// { "type": "fragment-migration", "partition": 42, "from": "node-1", "to": "node-3" }
```

### Reliability Caveat

Cluster events are delivered over the same pub/sub channel as application messages, which means **at-most-once delivery**: an event is lost if the subscriber is disconnected, slow to drain, or temporarily partitioned at the moment of publish.

For consumers that need to track cluster state precisely (operational dashboards, control-plane integrations), **do not rely on the event stream alone**. Pair it with periodic reconciliation:

```rust
// Drain events; on every tick (e.g., 30s) also poll for ground truth.
let routing = client.routing_table().await?;
let members = client.members().await?;
// Diff the snapshot against what events implied; correct any drift.
```

The event stream is the fast path for cache-friendly notifications. The poll is the slow path that guarantees eventual correctness.

## Delivery Guarantees

- **At-most-once**: Messages are delivered at most once. If a subscriber is disconnected, slow to drain its buffer, or partitioned at the moment of publish, the message is **silently lost** — no error is surfaced to publisher or subscriber. Subscribers that depend on receiving every message must pair the subscription with a reconciliation mechanism (see [Cluster Event Channel](#cluster-event-channel) for an example).
- **No persistence**: Messages are not stored. There is no message history or replay.
- **No ordering guarantee across nodes**: Messages published on different nodes may arrive in different orders.
- **Ordering within a connection**: Messages from the same publisher to the same channel arrive in order.

## Performance Considerations

- Pub/Sub is independent of DMap partitioning - messages are broadcast, not routed by hash
- High-volume publishing can create significant inter-node traffic (every message is forwarded to every node)
- Pattern matching (`PSUBSCRIBE`) has slightly higher CPU cost than exact matching
- Subscriber backpressure: slow consumers may cause message buffering in memory
