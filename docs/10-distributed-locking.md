# Distributed Locking

## Overview

Kamino provides **approximate distributed locks** for coordinating access to shared resources across cluster nodes. These locks are implemented as DMap entries with special semantics.

> **Important**: These locks are approximate and should only be used for non-critical coordination purposes (e.g., deduplication, rate limiting, cache stampede prevention). They are **not** suitable for mission-critical distributed synchronization where absolute mutual exclusion is required.

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

## Lock with Timeout

Two variants are provided:

### `lock(key, deadline)`
- Tries to acquire the lock
- Retries every 10ms until `deadline` expires
- The lock has no automatic expiry (held until explicitly unlocked)

### `lock_with_timeout(key, timeout, deadline)`
- Same retry behavior as `lock`
- The lock automatically expires after `timeout` duration
- Acts as a safety net against lock holder crashes

```rust
// Lock that auto-expires after 30 seconds
// Give up trying after 5 seconds
let lock = cache.lock_with_timeout(
    "resource:mutex",
    Duration::from_secs(30),  // lock timeout (auto-expire)
    Duration::from_secs(5),   // acquisition deadline
).await?;
```

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
- Lock operations route to the partition owner
- If the partition owner is unreachable, the operation fails
- With `member_count_quorum` set, minority partitions reject lock operations

## Best Practices

1. **Always use timeouts**: Use `lock_with_timeout` to prevent indefinite lock holding
2. **Keep critical sections short**: Minimize time between lock and unlock
3. **Use lease extension for long operations**: Call `lease()` periodically for operations that may take longer than the initial timeout
4. **Don't use for financial transactions**: These locks are approximate; use a proper consensus system (Raft, etc.) for critical mutual exclusion
5. **Handle `ErrLockNotAcquired` gracefully**: Implement backoff or fallback logic
