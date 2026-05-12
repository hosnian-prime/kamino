# API Design

## Client Trait

The core client interface is shared between embedded and remote modes:

```rust
#[async_trait]
pub trait Client: Send + Sync {
    /// Create or access a distributed map
    fn new_dmap(&self, name: &str, options: DMapOptions) -> Result<Arc<dyn DMap>>;

    /// Create a pub/sub handle
    fn new_pubsub(&self, options: PubSubOptions) -> Result<Arc<dyn PubSub>>;

    /// Get node statistics
    async fn stats(&self, options: StatsOptions) -> Result<Stats>;

    /// Ping a specific node
    async fn ping(&self, addr: &str) -> Result<()>;

    /// Get the current routing table
    async fn routing_table(&self) -> Result<RoutingTable>;

    /// Get current cluster members
    async fn members(&self) -> Result<Vec<Member>>;

    /// Force refresh the cached routing table metadata
    async fn refresh_metadata(&self) -> Result<()>;

    /// Total number of hash-ring partitions (from the cluster config).
    /// Stable for the lifetime of the cluster; used by `DMap::scan` to iterate
    /// all partitions of a DMap.
    fn partition_count(&self) -> u32;

    /// Gracefully close the client
    async fn close(&self) -> Result<()>;
}
```

## DMap Trait

The distributed map interface:

```rust
#[async_trait]
pub trait DMap: Send + Sync {
    /// Store a key-value pair with optional settings
    async fn put(&self, key: &str, value: &[u8], options: PutOptions) -> Result<()>;

    /// Retrieve a value by key
    async fn get(&self, key: &str) -> Result<GetResponse>;

    /// Delete one or more keys, returns count of deleted keys
    async fn delete(&self, keys: &[&str]) -> Result<usize>;

    /// Increment an integer value, serialized at the partition primary.
    /// NOT atomic across the cluster under partition — see [Replication](04-replication.md#incrdecr-lost-update-warning).
    async fn incr(&self, key: &str, delta: i64) -> Result<i64>;

    /// Decrement an integer value, serialized at the partition primary.
    /// Same partition caveat as `incr`.
    async fn decr(&self, key: &str, delta: i64) -> Result<i64>;

    /// Set value and return previous value
    async fn get_put(&self, key: &str, value: &[u8]) -> Result<Option<GetResponse>>;

    /// Increment a float value, serialized at the partition primary.
    /// Same partition caveat as `incr`.
    async fn incr_by_float(&self, key: &str, delta: f64) -> Result<f64>;

    /// Set or update TTL for a key
    async fn expire(&self, key: &str, duration: Duration) -> Result<()>;

    /// Acquire a distributed lock on a key.
    ///
    /// **⚠ SAFETY**: This variant holds the lock with NO automatic expiry. If the caller
    /// crashes or loses connection, the lock remains held until the partition owner
    /// restarts or someone manually deletes the entry. **Prefer `lock_with_timeout` for
    /// any lock that may be released by something other than orderly shutdown.**
    async fn lock(&self, key: &str, deadline: Duration) -> Result<LockContext>;

    /// Acquire a distributed lock that auto-expires after `lease`. Recommended variant.
    ///
    /// `deadline` is how long to keep retrying acquisition; `lease` is how long the lock
    /// stays held once acquired. The lock holder can call `LockContext::lease` to extend.
    async fn lock_with_timeout(
        &self,
        key: &str,
        lease: Duration,
        deadline: Duration,
    ) -> Result<LockContext>;

    /// Cursor-based iteration over a single partition.
    /// Clients iterate the whole DMap by calling scan for each partition ID (0..partition_count).
    /// See `Client::partition_count()`.
    async fn scan(
        &self,
        partition_id: u32,
        options: ScanOptions,
    ) -> Result<Box<dyn ScanCursor>>;

    /// Delete the entire DMap across all partitions
    async fn destroy(&self) -> Result<()>;

    /// Create a pipeline for batched operations
    fn pipeline(&self, options: PipelineOptions) -> Pipeline;
}
```

## Put Options

```rust
pub struct PutOptions {
    /// Set expiry in seconds.
    pub ex: Option<u64>,
    /// Set expiry in milliseconds.
    pub px: Option<u64>,
    /// Set absolute expiry (Unix timestamp seconds).
    pub exat: Option<u64>,
    /// Set absolute expiry (Unix timestamp milliseconds).
    pub pxat: Option<u64>,
    /// Only set if key does NOT exist.
    pub nx: bool,
    /// Only set if key ALREADY exists.
    pub xx: bool,
    /// Override the server-assigned LWW timestamp (Unix nanoseconds).
    /// Leave as `None` to let the partition primary stamp the entry on acceptance.
    /// Provide a value only for replication tools, replay, or external HLC integration.
    pub timestamp: Option<i64>,
}
```

## Get Response

```rust
pub struct GetResponse {
    /// The stored value
    pub value: Vec<u8>,
    /// Entry timestamp (for LWW conflict resolution)
    pub timestamp: i64,
    /// Remaining TTL (if set)
    pub ttl: Option<Duration>,
}

impl GetResponse {
    /// Deserialize value as UTF-8 string
    pub fn as_str(&self) -> Result<&str>;

    /// Deserialize value as integer
    pub fn as_i64(&self) -> Result<i64>;

    /// Deserialize value as float
    pub fn as_f64(&self) -> Result<f64>;
}
```

## Lock Context

```rust
pub struct LockContext {
    /// Opaque lock token (16 random bytes)
    token: Vec<u8>,
    /// DMap name
    dmap: String,
    /// Locked key
    key: String,
    /// Client reference for unlock/lease operations
    client: Arc<dyn Client>,
}

impl LockContext {
    /// Release the lock
    pub async fn unlock(&self) -> Result<()>;

    /// Extend the lock lease
    pub async fn lease(&self, duration: Duration) -> Result<()>;
}
```

## Scan Cursor

```rust
#[async_trait]
pub trait ScanCursor: Send {
    /// Advance and return the next entry, or `None` when the partition is exhausted.
    ///
    /// Returns `Err(InvalidCursor)` if the partition migrated to a different owner
    /// between calls. On `InvalidCursor`, restart the scan for that partition from
    /// cursor 0; the data is not lost, only the iteration state.
    async fn next(&mut self) -> Result<Option<(String, Vec<u8>)>>;

    /// Release server-side cursor state.
    async fn close(&mut self) -> Result<()>;
}
```

### Scan Options

```rust
pub struct ScanOptions {
    /// Approximate number of keys per server round-trip.
    pub count: Option<usize>,
    /// Glob pattern (e.g., `user:*`) matched against keys.
    pub match_pattern: Option<String>,
}
```

### Iterating the Whole DMap

```rust
let partition_count = client.partition_count();
for part_id in 0..partition_count {
    let mut cursor = dmap.scan(part_id, ScanOptions::default()).await?;
    while let Some((key, value)) = cursor.next().await? {
        // process (key, value)
    }
    cursor.close().await?;
}
```

Scan is intentionally partition-scoped. Iterating partitions in parallel is safe but increases server load proportionally; for admin-grade full scans, bound the parallelism (e.g., 4–8 concurrent partitions).

## Pipeline

```rust
pub struct PipelineOptions {
    /// Maximum number of concurrent operations when executing the pipeline.
    /// Commands targeting different nodes run in parallel up to this limit.
    pub concurrency: usize,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self { concurrency: 4 }
    }
}
```

```rust
pub struct Pipeline {
    operations: Vec<PipelineOp>,
    concurrency: usize,
}

impl Pipeline {
    /// Add a PUT operation
    pub fn put(&mut self, key: &str, value: &[u8], options: PutOptions) -> &mut Self;

    /// Add a GET operation
    pub fn get(&mut self, key: &str) -> &mut Self;

    /// Add a DELETE operation
    pub fn delete(&mut self, key: &str) -> &mut Self;

    /// Execute all operations, returns results in order
    pub async fn execute(&self) -> Vec<Result<PipelineResult>>;
}
```

## Pub/Sub

```rust
#[async_trait]
pub trait PubSub: Send + Sync {
    /// Subscribe to exact channel names
    async fn subscribe(&self, channels: &[&str]) -> Result<Subscription>;

    /// Subscribe to glob patterns
    async fn psubscribe(&self, patterns: &[&str]) -> Result<Subscription>;

    /// Publish a message to a channel
    async fn publish(&self, channel: &str, message: &[u8]) -> Result<usize>;
}

pub struct Subscription {
    receiver: tokio::sync::mpsc::Receiver<Message>,
}

pub struct Message {
    pub channel: String,
    pub pattern: Option<String>,
    pub payload: Vec<u8>,
}
```

## Error Types

```rust
pub enum Error {
    /// Key not found in the DMap
    KeyNotFound,
    /// DMap not found
    DMapNotFound,
    /// Key already exists (NX semantics)
    KeyAlreadyExists,
    /// Key does not exist (XX semantics)
    KeyNotExists,
    /// Lock not acquired within deadline
    LockNotAcquired,
    /// Invalid lock token on unlock
    NoSuchLock,
    /// Scan cursor invalidated (partition migrated between scan calls).
    /// Restart the scan for that partition from cursor 0.
    InvalidCursor,
    /// Cluster quorum not met
    ClusterQuorum,
    /// Server is shutting down
    ServerGone,
    /// Network/IO error
    Io(std::io::Error),
    /// Operation timeout
    Timeout,
    /// Serialization error
    Serialization(String),
}
```

## Usage Examples

### Embedded Mode

```rust
use kamino::{Kamino, Config};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::default();
    let node = Kamino::new(config).await?;
    node.start().await?;

    let client = node.embedded_client();
    let cache = client.new_dmap("sessions", Default::default())?;

    // Put with TTL
    cache.put("user:123", b"session_data", PutOptions {
        ex: Some(3600), // 1 hour TTL
        ..Default::default()
    }).await?;

    // Get
    let response = cache.get("user:123").await?;
    println!("Value: {}", response.as_str()?);

    // Atomic increment
    let counter = cache.incr("requests:total", 1).await?;

    // Distributed lock
    let lock = cache.lock("resource:mutex", Duration::from_secs(10)).await?;
    // ... critical section ...
    lock.unlock().await?;

    node.shutdown().await?;
    Ok(())
}
```

### Client-Server Mode

```rust
use kamino::ClusterClient;

#[tokio::main]
async fn main() -> Result<()> {
    let client = ClusterClient::new(
        vec!["10.0.1.1:3320", "10.0.1.2:3320"],
        Default::default(),
    ).await?;

    let cache = client.new_dmap("products", Default::default())?;
    cache.put("sku:ABC", b"product_json", Default::default()).await?;

    client.close().await?;
    Ok(())
}
```
