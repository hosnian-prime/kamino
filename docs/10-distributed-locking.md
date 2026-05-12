# Distributed Locking

## Overview

Kamino provides **approximate distributed locks** for coordinating access to shared resources across cluster nodes. These locks are implemented as DMap entries with special semantics.

> **Important**: These locks are approximate and should only be used for non-critical coordination purposes (e.g., deduplication, rate limiting, cache stampede prevention). They are **not** suitable for mission-critical distributed synchronization where absolute mutual exclusion is required.

## Lock API Safety

Kamino exposes two lock variants. **Use `lock_with_timeout` by default.**

| Variant | Auto-expire | Use case |
|---------|-------------|----------|
| `lock_with_timeout(key, lease, deadline)` | ✓ — lease expires after `lease` duration | **Recommended**. The lease is the safety net against caller crashes. |
| `lock(key, deadline)` | ✗ — held forever until explicit unlock | Footgun. Only correct when you can guarantee orderly unlock (i.e., never). |

The `lock` (no-timeout) variant exists for completeness but in practice almost always reflects a bug. If the holder crashes between `lock` and `unlock`, the lock remains held until the partition owner is restarted or the entry is manually deleted. Treat it as `unsafe`.

## Algorithm

### Lock Acquisition

```
1. Generate a random 16-byte token
2. Attempt to store the token as a DMap entry using NX semantics
   (NX = only-set-if-not-exists)
3. If the key already exists:
   a. Wait 10 milliseconds
   b. Retry from step 2
4. Repeat until:
   a. Lock is acquired (NX succeeds) → return LockContext with token
   b. Deadline expires → return ErrLockNotAcquired
```

### Lock Release (Unlock)

```
1. Read the current value for the lock key
2. Compare the stored token with the provided token (byte comparison)
3. If tokens match → delete the entry → lock released
4. If tokens don't match → return ErrNoSuchLock
```

The token comparison prevents unauthorized unlocks - only the lock holder can release it.

### Lease Extension

```
1. Validate the provided token matches the stored token
2. Update the entry's TTL to the new lease duration
3. Return success
```

This allows long-running operations to extend their lock before it expires.

## Partition Routing

Lock operations are routed via the same consistent hashing as regular DMap operations:

```
lock_key → hash → partition_id → partition_owner
```

If the current node owns the partition, the operation executes locally. Otherwise, it is forwarded to the partition owner via the RESP protocol.

## Concurrency Safety

Lock operations use the fine-grained key-level `Locker` to prevent race conditions during the read-modify-write cycle:

```rust
async fn lock(&self, key: &str, deadline: Duration) -> Result<LockContext> {
    let token: Vec<u8> = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .collect();

    let deadline_at = Instant::now() + deadline;

    loop {
        // Key-level lock prevents TOCTOU race on NX check
        let result = self.put(key, &token, PutOptions { nx: true, ..default() }).await;

        match result {
            Ok(()) => {
                return Ok(LockContext { token, dmap: self.name.clone(), key: key.to_string() });
            }
            Err(Error::KeyAlreadyExists) => {
                if Instant::now() >= deadline_at {
                    return Err(Error::LockNotAcquired);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

## The Two Lock Variants in Detail

### `lock_with_timeout(key, lease, deadline)` — Recommended

```
acquire_loop:
  attempt PUT(key, token) with NX and TTL=lease
  on success → return LockContext { token, lease_expires_at }
  on already-exists:
    if Instant::now() >= deadline_at → return ErrLockNotAcquired
    sleep 10ms and retry
```

The lease is the safety net. If the holder crashes, the entry expires after `lease` and another caller can acquire. The holder can call `LockContext::lease()` to extend before expiry; if it forgets, the lock is recoverable.

```rust
// Lease auto-expires after 30s. Give up acquisition after 5s.
let lock = cache.lock_with_timeout(
    "resource:mutex",
    Duration::from_secs(30),  // lease (auto-expiry)
    Duration::from_secs(5),   // acquisition deadline
).await?;
```

### `lock(key, deadline)` — Footgun, Avoid

Same acquisition loop but the PUT has no TTL. If the holder crashes, the lock entry remains until manual deletion or partition owner restart. There is no good reason to call this in application code; it is provided only as a primitive for the implementation of `lock_with_timeout` itself.

## Wire Protocol

### LOCK Command
```
DM.LOCK <dmap> <key> <deadline_ms> [timeout_ms]
→ Returns: token (bulk string) on success
→ Returns: error on failure
```

### UNLOCK Command
```
DM.UNLOCK <dmap> <key> <token>
→ Returns: +OK on success
→ Returns: error if token doesn't match
```

### LEASE Commands
```
DM.LOCKLEASE <dmap> <key> <token> <seconds>
DM.PLOCKLEASE <dmap> <key> <token> <milliseconds>
→ Returns: +OK on success
```

## Failure Scenarios

### Lock Holder Crashes
- If `lock_with_timeout` was used: lock auto-expires after timeout
- If `lock` was used (no timeout): lock is held until the node holding the partition restarts or the entry is manually deleted

### Partition Owner Fails
- Lock data is replicated to backup owners (if `replica_count > 1`)
- New partition owner has the lock entry
- Lock holder can still unlock using the original token

### Network Partition

Lock operations route to the partition owner. Behavior depends on `member_count_quorum`:

- `member_count_quorum >= majority`: minority partitions reject lock operations with `ErrClusterQuorum`. **Recommended** for any lock used to gate writes — both sides cannot believe they hold the lock.
- `member_count_quorum = 1` (default): both partitions accept locks for keys whose primaries they each contain. After heal, LWW resolves — one side's lock entry survives, the other's is silently dropped, and both holders may have already entered their critical sections. **This violates mutual exclusion.**

If a lock matters, set the quorum.

## Best Practices

1. **Always use `lock_with_timeout`**: The no-lease `lock` variant has no safety net — a crashed holder permanently wedges the key. Treat `lock` as `unsafe`.
2. **Keep critical sections short**: Minimize time between lock and unlock
3. **Use lease extension for long operations**: Call `lease()` periodically for operations that may take longer than the initial timeout
4. **Don't use for financial transactions**: These locks are approximate; use a proper consensus system (Raft, etc.) for critical mutual exclusion
5. **Handle `ErrLockNotAcquired` gracefully**: Implement backoff or fallback logic
