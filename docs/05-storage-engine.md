# Storage Engine

## Overview

Kamino's default storage engine is **RamBlock** — a GC-free (Rust has no GC, but the design avoids excessive allocations), append-only, in-memory storage engine optimized for cache workloads. The design follows the Bitcask model (Basho/Riak, 2010): append-only writes with an in-memory hash index mapping keys to byte offsets, and periodic compaction to reclaim garbage space.

## Design Principles

1. **Append-only writes**: New entries are appended sequentially, maximizing write throughput
2. **Pre-allocated buffers**: Memory is allocated in large blocks to reduce allocation overhead
3. **O(1) lookups**: Hash-key-to-offset mapping for constant-time reads
4. **Lazy deletion**: Entries are marked as garbage; space is reclaimed during compaction
5. **Pluggable**: The storage engine is behind a trait interface for custom implementations

## Architecture

```
Fragment (per DMap per Partition)
└── StorageEngine
    ├── Table[0] (read-only, oldest)
    ├── Table[1] (read-only)
    ├── ...
    └── Table[N] (read-write, current)
```

### Table Structure

Each table is a contiguous memory block:

```rust
pub struct Table {
    /// Pre-allocated byte buffer (default: 1 MB)
    memory: Vec<u8>,
    /// Hash key → byte offset mapping
    hkeys: HashMap<u64, usize>,
    /// Roaring bitmap tracking active entry positions
    offset_index: RoaringBitmap,
    /// Current write offset
    offset: usize,
    /// Number of garbage (deleted) entries
    garbage_count: usize,
    /// State: ReadWrite, ReadOnly, Recycled
    state: TableState,
}
```

**States:**
- `ReadWrite`: The current table accepting new writes
- `ReadOnly`: Full table, only serves reads
- `Recycled`: Compacted and reusable

### Write Path

```
1. Serialize entry into binary format
2. Check if current table has space
3. If full → seal current table as ReadOnly, allocate new ReadWrite table
4. Append serialized bytes to table.memory at current offset
5. Record hkey → offset in hash map
6. Update offset_index bitmap
7. Advance write offset
```

### Read Path

```
1. Compute hkey = hash(dmap_name + key)
2. Scan tables in reverse order (newest first)
3. Lookup hkey in table.hkeys → byte offset
4. Deserialize entry from table.memory[offset..]
5. Return entry (or continue to next table if not found)
```

## Entry Binary Format

Each entry is serialized as a compact binary structure:

```
┌───────────────┬──────────┬─────────┬─────────────┬──────────────┬──────────────┬──────────────┐
│ KEY_LENGTH(1B)│ KEY(var) │ TTL(8B) │ TIMESTAMP(8B)│ LAST_ACCESS(8B)│ VAL_LEN(4B)│ VALUE(var)  │
└───────────────┴──────────┴─────────┴─────────────┴──────────────┴──────────────┴──────────────┘
```

| Field | Size | Type | Description |
|-------|------|------|-------------|
| `key_length` | 1 byte | u8 | Key length (max 255 bytes) |
| `key` | variable | [u8] | Raw key bytes |
| `ttl` | 8 bytes | i64 | Time-to-live in nanoseconds (0 = no expiry) |
| `timestamp` | 8 bytes | i64 | Entry creation/update timestamp (for LWW). **Assigned by the partition primary at write acceptance** using a monotonized wall clock (`max(prev_ts + 1, wall_time)` — a simplified HLC). May be overridden by the client via `PutOptions.timestamp` for replay or external HLC integration. |
| `last_access` | 8 bytes | i64 | Last access timestamp (for LRU eviction) |
| `value_length` | 4 bytes | u32 | Value length |
| `value` | variable | [u8] | Raw value bytes |

**Fixed overhead**: 29 bytes per entry.
**Maximum key size**: 255 bytes (u8 length field).
**Maximum value size**: ~4 GB (u32 length field).

## Eviction Policies

### 1. TTL-Based Eviction

Background workers run continuously, evicting expired entries:

```
Algorithm (inspired by probabilistic expiry):
  loop:
    1. Sample 20 random keys with TTL set
    2. Delete all expired keys in the sample
    3. If > 25% of sampled keys were expired → repeat
    4. If <= 25% → sleep and retry later
```

This adaptive algorithm concentrates effort when many keys are expiring simultaneously.

**Configuration:**
- `num_eviction_workers`: Number of background eviction threads (default: 1)

### 2. Max Idle Duration

Entries not accessed within `max_idle_duration` are evicted:

```rust
fn is_idle(&self, entry: &Entry, max_idle: Duration) -> bool {
    let idle_deadline = entry.last_access + max_idle.as_nanos() as i64;
    let now = timestamp_now();
    now > idle_deadline
}
```

Every `Get`, `Put`, `Expire`, or `Lock` operation updates `last_access`.

### 3. LRU (Least Recently Used)

Approximated LRU, triggered during writes when capacity limits are reached:

```
Algorithm:
  1. Sample `lru_samples` random keys (default: 5)
  2. Sort samples by last_access timestamp
  3. Evict the entry with the oldest last_access
```

This approximation avoids the overhead of maintaining a full LRU linked list while providing good eviction behavior in practice.

**Configuration:**
- `lru_samples`: Keys sampled per eviction (default: 5)
- `max_keys`: Maximum keys per DMap (triggers LRU when exceeded)
- `max_inuse`: Maximum memory per DMap (triggers LRU when exceeded)
- `eviction_policy`: `LRU` or `None`

## Compaction

Deleted entries leave garbage in tables. Compaction reclaims this space:

```
Trigger: garbage_ratio > 40% (max_garbage_ratio)
Interval: Every 10 minutes (trigger_compaction_interval)

Process:
  1. Scan all ReadOnly tables
  2. For tables with garbage_ratio > threshold:
     a. Create new table
     b. Copy only live (non-deleted) entries
     c. Replace old table with compacted table
     d. Mark old table as Recycled
```

## Storage Engine Trait

```rust
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Store an entry.
    async fn put(&mut self, hkey: u64, entry: &Entry) -> Result<()>;

    /// Retrieve an entry by hash key.
    async fn get(&self, hkey: u64) -> Result<Entry>;

    /// Delete an entry. Returns true if a live entry was deleted.
    async fn delete(&mut self, hkey: u64) -> Result<bool>;

    /// Scan all entries. The callback returns `false` to stop iteration.
    async fn scan<F>(&self, f: F) -> Result<()>
    where F: FnMut(u64, &Entry) -> bool + Send;

    /// Scan entries whose keys match a regex pattern.
    async fn scan_regex_match<F>(&self, pattern: &str, f: F) -> Result<()>
    where F: FnMut(u64, &Entry) -> bool + Send;

    /// Number of stored entries.
    fn len(&self) -> usize;

    /// Total bytes in use.
    fn inuse(&self) -> usize;

    /// Export all data for migration.
    async fn export(&self) -> Result<Vec<u8>>;

    /// Import data from migration (with LWW merge if entries collide).
    async fn import(&mut self, data: &[u8]) -> Result<()>;
}
```

Custom storage engines can be implemented by providing this trait. The default `RamBlock` is in-memory and trivially async (all futures complete in the same poll). Disk-backed engines (LMDB, RocksDB, on-disk B-trees) use real async I/O via their respective async-capable bindings or `spawn_blocking` internally.

The `len()` and `inuse()` accessors remain synchronous because they are O(1) atomic reads that must be available to size-based eviction without yielding.

## Per-DMap Storage Configuration

Each DMap can override global storage settings:

```toml
[[dmaps]]
name = "sessions"
max_idle_duration = "30m"
ttl = "24h"
max_keys = 1000000
max_inuse = "512MB"
lru_samples = 10
eviction_policy = "LRU"
storage_engine = "ramblock"
```

## Limitations

### Key Size: 255 bytes

The entry header's `key_length` field is `u8`, so keys are limited to **255 bytes**. This is significantly more restrictive than Redis (512 MB). It is suitable for typical cache keys (user IDs, session tokens, short composite keys) but **not** for long URLs, full file paths, or large composite keys.

If your workload requires longer keys, hash the application-level key client-side (e.g., to a 32-byte SHA-256 hex) and store the original key inside the value if you need to recover it.

### Value Size and Table Sizing

A value is bounded by `u32` length (~4 GiB), but the practical limit is the `table_size` configuration (default: 1 MiB):

- If a value plus its 29-byte overhead fits in the current `ReadWrite` table → appended directly.
- If it does not fit → the current table is sealed `ReadOnly` and a fresh table is allocated to hold the new entry.
- If a single value exceeds `table_size`, the storage engine allocates a one-shot table sized to fit the value plus overhead. This is supported but inefficient — keep `table_size` ≥ your P99 value size.

### Max Key/Value Cardinality per DMap

`max_keys` and `max_inuse` are soft limits enforced by LRU eviction. They are **not** hard rejections — under bursty writes the limits may be transiently exceeded before eviction catches up.

### No Persistence

RamBlock is purely in-memory. There is no WAL, no snapshot to disk, no recovery on restart. A node restart loses all data on that node. Cluster-level durability comes from `replica_count` only.

### Compaction Pause

During compaction of a ReadOnly table, the fragment write lock is **not** held — reads continue from the old table while the new compacted table is built. The atomic swap at the end is short (single pointer update). However, the compaction itself consumes CPU and memory proportional to table size; tune `trigger_compaction_interval` for your workload.
