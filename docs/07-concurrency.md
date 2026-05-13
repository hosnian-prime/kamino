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

- **Reads** (majority of operations): Multiple concurrent readers allowed via `RwLock` shared mode (not lock-free — readers and a pending writer are coordinated by the lock implementation). Every DMap operation reads the routing table to determine the partition owner.
- **Writes** (rare, topology changes only): Exclusive lock. Only the coordinator thread updates the routing table.

Additionally, a dedicated `update_routing_mutex` serializes routing table updates to prevent concurrent recalculations:

```rust
pub struct RoutingTable {
    inner: tokio::sync::RwLock<RoutingTableInner>,
    update_mutex: tokio::sync::Mutex<()>, // serializes routing updates across async tasks
}
```

## DMap Registry Concurrency

The DMap service maintains a registry of active DMaps:

```rust
pub struct DMapService {
    dmaps: tokio::sync::RwLock<HashMap<String, Arc<DMap>>>,
}
```

- **Lookup** (common path): Read lock, concurrent
- **Create** (rare): Write lock, exclusive

Once a DMap reference is obtained (via `Arc`), it can be used concurrently without holding the registry lock.

## Fragment-Level Locking

Each fragment (a DMap's data within a partition) has its own `RwLock`:

```rust
pub struct Fragment {
    storage: tokio::sync::RwLock<Box<dyn StorageEngine>>,
    config: FragmentConfig,
}
```

- **Read operations** (`GET`): Acquire read lock, concurrent within the same fragment
- **Write operations** (`PUT`, `DELETE`): Acquire write lock, exclusive within the fragment
- **Different fragments**: Completely independent, no contention

This allows concurrent reads on the same DMap partition while serializing writes.

**Async semantics**: The fragment lock is `tokio::sync::RwLock` (async). The storage engine trait itself is `async` (see [Storage Engine](05-storage-engine.md#storage-engine-trait)), so all storage operations yield cooperatively. CPU-bound work (e.g., serialization of large entries) is dispatched to a blocking pool via `spawn_blocking` when measured to exceed a few hundred microseconds.

## Key-Level Locking (Named Locks)

For atomic operations (`INCR`, `DECR`, `GETPUT`, `INCRBYFLOAT`), a fine-grained named lock system prevents races on individual keys:

```rust
pub struct Locker {
    /// Sync mutex: held only briefly for map lookup/insert, never across `.await`.
    /// `parking_lot::Mutex` chosen for its faster uncontended path and no poisoning.
    locks: parking_lot::Mutex<HashMap<String, Arc<LockEntry>>>,
}

struct LockEntry {
    /// Async mutex: the holder yields while inside the critical section
    /// (which may `await` storage I/O). Must be tokio-aware.
    mutex: tokio::sync::Mutex<()>,
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

**Automatic cleanup**: Removal of a `LockEntry` from the map happens under the same map mutex that any new waiter must acquire for lookup. When the last guard drops and the waiter count reaches zero, the cleanup path locks the map and removes the entry; a concurrent lookup either runs to completion before the lock is acquired (and obtains a valid `Arc<LockEntry>`) or runs after removal (and inserts a fresh entry). This prevents both unbounded growth and the use-after-free of removed entries.

### Usage in Atomic Operations

```rust
async fn incr(&self, key: &str, delta: i64) -> Result<i64> {
    // Per-key lock prevents the TOCTOU race on the read-modify-write below.
    // The guard is held across `.await` points — `LockEntry.mutex` is `tokio::sync::Mutex`
    // for exactly this reason. A `std::sync::Mutex` here would block the runtime thread.
    let _guard = self.locker.lock(key).await;
    let current = self.get(key).await?;
    let new_value = current + delta;
    self.put(key, new_value).await?;
    Ok(new_value)
}
```

Without key-level locking, concurrent `INCR` operations on the same key would have a TOCTOU race.

### Sync vs Async Mutex

Kamino uses three lock primitives, deliberately:

| Primitive | Where used | Held across `.await`? |
|-----------|------------|------------------------|
| `tokio::sync::RwLock` | Routing table, DMap registry, fragment storage | Yes |
| `tokio::sync::Mutex` | Per-key `LockEntry`, routing-table update serialization | Yes |
| `parking_lot::Mutex` | `Locker.locks` map, in-memory counters, gauges | **Never** |

**The rule is non-negotiable.** Holding a `std::sync::Mutex` or `parking_lot::Mutex` across an `.await` blocks the executor thread, can deadlock on a single-thread runtime, and silently starves other tasks on a multi-thread one. Tokio's own tutorial flags this as the primary footgun of mixing sync and async lock types. Code review enforces the rule; the borrow checker does not catch it because `parking_lot::MutexGuard` is `Send`.

Quick test when reading a diff: trace from every sync-mutex acquisition to its `Drop`. If any `.await` sits between them, the lock is wrong.

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
- `Arc<RwLock<T>>` for shared mutable state across tasks

**Note**: Data race freedom is guaranteed by the compiler. Lock **ordering** (routing table -> service -> fragment -> key) is a convention enforced by code review — the borrow checker does not prevent deadlocks caused by acquiring locks in the wrong order.

## Deadlock Prevention

1. **Consistent lock ordering**: Always acquire locks in the order: routing table -> service -> fragment -> key
2. **No nested locks**: Each operation acquires at most one lock at each level
3. **Lock timeouts**: Internal operations have bounded timeouts to prevent indefinite blocking
4. **Try-lock fallback**: Where possible, use `try_lock()` with retry logic instead of blocking `lock()`
