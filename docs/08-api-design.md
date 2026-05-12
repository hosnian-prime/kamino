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

    /// Atomically increment an integer value
    async fn incr(&self, key: &str, delta: i64) -> Result<i64>;

    /// Atomically decrement an integer value
    async fn decr(&self, key: &str, delta: i64) -> Result<i64>;

    /// Set value and return previous value
    async fn get_put(&self, key: &str, value: &[u8]) -> Result<Option<GetResponse>>;

    /// Atomically increment a float value
    async fn incr_by_float(&self, key: &str, delta: f64) -> Result<f64>;

    /// Set or update TTL for a key
    async fn expire(&self, key: &str, duration: Duration) -> Result<()>;

    /// Acquire a distributed lock on a key
    async fn lock(&self, key: &str, deadline: Duration) -> Result<LockContext>;

    /// Acquire a distributed lock with timeout
    async fn lock_with_timeout(
        &self,
        key: &str,
        timeout: Duration,
        deadline: Duration,
    ) -> Result<LockContext>;

    /// Cursor-based key iteration
    async fn scan(&self, options: ScanOptions) -> Result<Box<dyn Iterator>>;

    /// Delete the entire DMap across all partitions
    async fn destroy(&self) -> Result<()>;

    /// Create a pipeline for batched operations
    fn pipeline(&self, options: PipelineOptions) -> Pipeline;
}
```

## Put Options

```rust
pub struct PutOptions {
    /// Set expiry in seconds
    pub ex: Option<u64>,
    /// Set expiry in milliseconds
    pub px: Option<u64>,
    /// Set absolute expiry (Unix timestamp seconds)
    pub exat: Option<u64>,
    /// Set absolute expiry (Unix timestamp milliseconds)
    pub pxat: Option<u64>,
    /// Only set if key does NOT exist
    pub nx: bool,
    /// Only set if key ALREADY exists
    pub xx: bool,
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

## Iterator

```rust
pub trait Iterator: Send {
    /// Advance to the next key
    fn next(&mut self) -> bool;

    /// Get the current key
    fn key(&self) -> &str;

    /// Close the iterator and release resources
    fn close(&mut self);
}
```

### Scan Options

```rust
pub struct ScanOptions {
    /// Approximate number of keys per batch
    pub count: Option<usize>,
    /// Glob pattern to match keys
    pub match_pattern: Option<String>,
}
```

**Important**: Scan operates per-partition. The client must iterate through all `partition_count` partitions to scan the entire DMap.

## Pipeline

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
