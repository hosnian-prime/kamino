# Kamino

Distributed in-memory cache written in Rust. Designed to scale horizontally across nodes with automatic data partitioning, replication, and failure recovery.

## What it does

- Key/value storage distributed across a cluster using consistent hashing
- Primary-backup replication with configurable quorum
- Gossip-based cluster membership (SWIM protocol) — no external coordination service needed
- TTL, LRU, and idle-based eviction
- Distributed locking
- Pub/sub messaging
- Redis-compatible wire protocol (RESP)
- Runs embedded in your application or as a standalone server

## Quick start

```rust
use kamino::{Kamino, Config};

let node = Kamino::new(Config::default()).await?;
node.start().await?;

let client = node.embedded_client();
let cache = client.new_dmap("sessions", Default::default())?;

cache.put("user:1", b"session_data", PutOptions { ex: Some(3600), ..Default::default() }).await?;
let val = cache.get("user:1").await?;
```

## Running as a server

```
kamino-server --config kamino.toml
```

Connect using any Redis client's raw command API on port 3320. Standard Redis commands (SET, GET, DEL) are not supported — use the `DM.*` command set. Pub/Sub uses standard Redis syntax.

## Cluster

Nodes find each other through static peers, DNS, or Kubernetes service discovery. Once connected, the gossip protocol handles the rest — failure detection, membership, routing table distribution.

```toml
[discovery]
peers = ["10.0.1.1:3322", "10.0.1.2:3322"]
```

On Kubernetes, point it at a headless Service and it figures out the topology from the Endpoints API.

## Docs

Technical documentation is in [`docs/`](docs/00-overview.md).

## Status

Early development. Not production-ready.

## License

MIT
