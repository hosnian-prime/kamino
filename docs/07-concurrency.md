# Concurrency Model

## Overview

Kamino leverages Rust's ownership model and type system to ensure thread safety at compile time. The concurrency model uses a hierarchy of locks at different granularities to maximize throughput while preventing data races.

## Lock Hierarchy

```
Level 0: Node-wide
  └── RoutingTable RwLock (protects partition-to-owner mapping)

Level 1: Service-wide
  └── DMapService RwLock (protects DMap registry)

Level 2: Fragment-level
  └── Fragment RwLock (protects per-DMap-per-partition data)

Level 3: Key-level
  └── NamedLock (fine-grained per-key locks for atomic operations)
```

## Routing Table Concurrency

The routing table is protected by an `RwLock`:

- **Reads** (majority of operations): Concurrent, lock-free among readers. Every DMap operation reads the routing table to determine the partition owner.
- **Writes** (rare, topology changes only): Exclusive lock. Only the coordinator thread updates the routing table.

Additionally, a dedicated `update_routing_mutex` serializes routing table updates to prevent concurrent recalculations:

```rust
pub struct RoutingTable {
    inner: RwLock<RoutingTableInner>,
    update_mutex: Mutex<()>, // serializes routing updates
}
```

## DMap Registry Concurrency

The DMap service maintains a registry of active DMaps:

```rust
pub struct DMapService {
    dmaps: RwLock<HashMap<String, Arc<DMap>>>,
}
```

- **Lookup** (common path): Read lock, concurrent
- **Create** (rare): Write lock, exclusive

Once a DMap reference is obtained (via `Arc`), it can be used concurrently without holding the registry lock.

## Fragment-Level Locking

Each fragment (a DMap's data within a partition) has its own `RwLock`:

```rust
pub struct Fragment {
    storage: RwLock<Box<dyn StorageEngine>>,
    config: FragmentConfig,
}
```

- **Read operations** (`GET`): Acquire read lock, concurrent within the same fragment
- **Write operations** (`PUT`, `DELETE`): Acquire write lock, exclusive within the fragment
- **Different fragments**: Completely independent, no contention

This allows concurrent reads on the same DMap partition while serializing writes.

## Key-Level Locking (Named Locks)

For atomic operations (`INCR`, `DECR`, `GETPUT`, `INCRBYFLOAT`), a fine-grained named lock system prevents races on individual keys:

```rust
pub struct Locker {
    locks: Mutex<HashMap<String, Arc<LockEntry>>>,
}

struct LockEntry {
    mutex: Mutex<()>,
    waiters: AtomicI32,
}
```

### Semantics

```rust
impl Locker {
    fn lock(&self, key: &str) -> LockGuard {
        // 1. Get or create lock entry for this key
        // 2. Increment waiter count atomically
        // 3. Acquire the per-key mutex
        // 4. Return guard that decrements waiters on drop
    }
}
```

**Automatic cleanup**: When a lock guard is dropped and the waiter count reaches zero, the lock entry is removed from the map. This prevents unbounded memory growth.

### Usage in Atomic Operations

```rust
fn incr(&self, key: &str, delta: i64) -> Result<i64> {
    let _guard = self.locker.lock(key); // key-level lock
    let current = self.get(key)?;
    let new_value = current + delta;
    self.put(key, new_value)?;
    Ok(new_value)
}
```

Without key-level locking, concurrent `INCR` operations on the same key would have a TOCTOU race.

## Routing Table Push Concurrency

When the coordinator pushes the routing table to all members, it does so concurrently but with bounded parallelism:

```rust
async fn push_routing_table(&self) {
    let semaphore = Semaphore::new(num_cpus::get());

    let mut tasks = Vec::new();
    for member in &self.members {
        let permit = semaphore.acquire().await;
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            self.send_routing_table(member).await
        }));
    }

    join_all(tasks).await;
}
```

## Async Runtime

Kamino uses `tokio` as its async runtime:

- **Network I/O**: All TCP operations are async
- **Timer-based tasks**: Balancer, eviction workers, compaction run on tokio timers
- **CPU-bound work**: Hashing and serialization run on blocking thread pool via `spawn_blocking`

## Thread Safety Guarantees

Rust's type system enforces:

- All shared state implements `Send + Sync`
- No data races possible (checked at compile time)
- Lock ordering is enforced by the borrow checker
- `Arc<RwLock<T>>` for shared mutable state across tasks

## Deadlock Prevention

1. **Consistent lock ordering**: Always acquire locks in the order: routing table -> service -> fragment -> key
2. **No nested locks**: Each operation acquires at most one lock at each level
3. **Lock timeouts**: Internal operations have bounded timeouts to prevent indefinite blocking
4. **Try-lock fallback**: Where possible, use `try_lock()` with retry logic instead of blocking `lock()`
