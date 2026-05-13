//! `MultiNodeRemoteClient` — cluster-aware RESP client with MOVED retry.
//!
//! Per `docs/02-consistent-hashing.md` / `docs/06-network-protocol.md`:
//!
//! - On every `DM.*` call, the client picks a [`RemoteClient`] handle from
//!   its per-address pool. The initial pick is round-robin so a routing-
//!   table-less client can warm up without a primary lookup.
//! - If the server replies with `-MOVED <part> <addr>`, the client opens a
//!   handle to `addr` (cache miss = fresh connect) and retries the same
//!   command **once**. A second MOVED bubbles to the caller as
//!   [`Error::Moved`].
//! - Caller-supplied `auth` is reused for every new connection.
//!
//! This is a thin wrapper for now; once a `ClusteredClient` lands with a
//! routing-table mirror, it will pre-route by `partition_for` and skip the
//! retry hop for the steady state.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::cursor::{ScanCursor, ScanOptions};
use crate::dmap::DMap;
use crate::error::{Error, Result};
use crate::lock::LockContext;
use crate::remote::RemoteClient;
use crate::stats::{Stats, StatsOptions};
use crate::traits::Client;
use crate::types::{DMapOptions, GetResponse, PutOptions};

/// RESP client that fronts a pool of [`RemoteClient`]s keyed by
/// `<host:port>` strings.
#[allow(missing_debug_implementations)]
pub struct MultiNodeRemoteClient {
    inner: Arc<Inner>,
}

struct Inner {
    auth: Option<String>,
    seeds: Vec<String>,
    next: AtomicUsize,
    /// `addr -> RemoteClient`. Cloned on hit; new entries are connected
    /// on miss (under lock).
    pool: Mutex<std::collections::HashMap<String, Arc<RemoteClient>>>,
}

impl MultiNodeRemoteClient {
    /// Build the client and eagerly connect to one of `seeds`.
    pub async fn connect(seeds: Vec<String>, auth: Option<&str>) -> Result<Self> {
        if seeds.is_empty() {
            return Err(Error::InvalidArgument(
                "MultiNodeRemoteClient requires at least one seed address".into(),
            ));
        }
        let inner = Arc::new(Inner {
            auth: auth.map(str::to_owned),
            seeds: seeds.clone(),
            next: AtomicUsize::new(0),
            pool: Mutex::new(std::collections::HashMap::new()),
        });
        // Warm one connection so the caller sees handshake failures early.
        let warm_addr = seeds[0].clone();
        let warm = RemoteClient::connect(&warm_addr, inner.auth.as_deref()).await?;
        inner.pool.lock().insert(warm_addr, Arc::new(warm));
        Ok(Self { inner })
    }

    /// Return a remote handle for `addr`, opening a fresh connection if the
    /// pool doesn't have one cached. Caller-supplied `auth` is reused.
    async fn handle(&self, addr: &str) -> Result<Arc<RemoteClient>> {
        let cached = self.inner.pool.lock().get(addr).cloned();
        if let Some(existing) = cached {
            return Ok(existing);
        }
        let client = RemoteClient::connect(addr, self.inner.auth.as_deref()).await?;
        let arc = Arc::new(client);
        self.inner
            .pool
            .lock()
            .insert(addr.to_string(), Arc::clone(&arc));
        Ok(arc)
    }

    /// Pick the next seed in round-robin order.
    fn pick_seed(&self) -> String {
        let idx = self.inner.next.fetch_add(1, Ordering::Relaxed) % self.inner.seeds.len();
        self.inner.seeds[idx].clone()
    }

    /// Drop the cached connection for `addr` (e.g. on persistent errors).
    pub fn evict(&self, addr: &str) {
        self.inner.pool.lock().remove(addr);
    }
}

impl std::fmt::Debug for MultiNodeRemoteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiNodeRemoteClient")
            .field("seeds", &self.inner.seeds)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Client for MultiNodeRemoteClient {
    async fn new_dmap(&self, name: &str, _options: DMapOptions) -> Result<Arc<dyn DMap>> {
        let dmap: Arc<dyn DMap> = Arc::new(MultiNodeDMap {
            client: Arc::clone(&self.inner),
            owner: Self {
                inner: Arc::clone(&self.inner),
            },
            name: name.to_string(),
        });
        Ok(dmap)
    }

    async fn stats(&self, options: StatsOptions) -> Result<Stats> {
        let seed = self.pick_seed();
        let handle = self.handle(&seed).await?;
        handle.stats(options).await
    }

    fn partition_count(&self) -> u32 {
        271
    }

    async fn close(&self) -> Result<()> {
        let pool = std::mem::take(&mut *self.inner.pool.lock());
        for (_addr, client) in pool {
            let _ = client.close().await;
        }
        Ok(())
    }

    async fn ping(&self, addr: &str) -> Result<()> {
        let handle = self.handle(addr).await?;
        handle.ping_once(None).await
    }

    async fn refresh_metadata(&self) -> Result<()> {
        // Phase 4: caller uses `CLUSTER.ROUTINGTABLE` directly if it wants
        // the table — no implicit refresh.
        Ok(())
    }
}

/// `DMap` handle that goes through `MultiNodeRemoteClient` and honours
/// `Error::Moved` by reopening on the new owner.
pub struct MultiNodeDMap {
    client: Arc<Inner>,
    owner: MultiNodeRemoteClient,
    name: String,
}

impl std::fmt::Debug for MultiNodeDMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiNodeDMap")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl MultiNodeDMap {
    async fn first_handle(&self) -> Result<Arc<RemoteClient>> {
        // The caller's seed list is already shuffled into round-robin
        // pick_seed order; reuse the owner's helper to honour that.
        self.owner.handle(&self.owner.pick_seed()).await
    }

    /// Look up an existing handle by address (no implicit connect).
    fn cached_handle(&self, addr: &str) -> Option<Arc<RemoteClient>> {
        self.client.pool.lock().get(addr).cloned()
    }
}

#[async_trait]
impl DMap for MultiNodeDMap {
    fn name(&self) -> &str {
        &self.name
    }

    async fn put(&self, key: &str, value: &[u8], options: PutOptions) -> Result<()> {
        with_moved_retry(self, |handle| {
            let name = self.name.clone();
            let key = key.to_string();
            let value = value.to_vec();
            let options = options.clone();
            async move {
                let d = handle.new_dmap(&name, DMapOptions::default()).await?;
                d.put(&key, &value, options).await
            }
        })
        .await
    }

    async fn get(&self, key: &str) -> Result<GetResponse> {
        with_moved_retry(self, |handle| {
            let name = self.name.clone();
            let key = key.to_string();
            async move {
                let d = handle.new_dmap(&name, DMapOptions::default()).await?;
                d.get(&key).await
            }
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        with_moved_retry(self, |handle| {
            let name = self.name.clone();
            let key = key.to_string();
            async move {
                let d = handle.new_dmap(&name, DMapOptions::default()).await?;
                d.delete(&key).await
            }
        })
        .await
    }

    async fn incr(&self, key: &str, delta: i64) -> Result<i64> {
        with_moved_retry(self, |handle| {
            let name = self.name.clone();
            let key = key.to_string();
            async move {
                let d = handle.new_dmap(&name, DMapOptions::default()).await?;
                d.incr(&key, delta).await
            }
        })
        .await
    }

    async fn decr(&self, key: &str, delta: i64) -> Result<i64> {
        with_moved_retry(self, |handle| {
            let name = self.name.clone();
            let key = key.to_string();
            async move {
                let d = handle.new_dmap(&name, DMapOptions::default()).await?;
                d.decr(&key, delta).await
            }
        })
        .await
    }

    async fn incr_by_float(&self, key: &str, delta: f64) -> Result<f64> {
        with_moved_retry(self, |handle| {
            let name = self.name.clone();
            let key = key.to_string();
            async move {
                let d = handle.new_dmap(&name, DMapOptions::default()).await?;
                d.incr_by_float(&key, delta).await
            }
        })
        .await
    }

    async fn get_put(&self, key: &str, value: &[u8]) -> Result<Option<GetResponse>> {
        with_moved_retry(self, |handle| {
            let name = self.name.clone();
            let key = key.to_string();
            let value = value.to_vec();
            async move {
                let d = handle.new_dmap(&name, DMapOptions::default()).await?;
                d.get_put(&key, &value).await
            }
        })
        .await
    }

    async fn expire(&self, key: &str, duration: Duration) -> Result<()> {
        with_moved_retry(self, |handle| {
            let name = self.name.clone();
            let key = key.to_string();
            async move {
                let d = handle.new_dmap(&name, DMapOptions::default()).await?;
                d.expire(&key, duration).await
            }
        })
        .await
    }

    async fn lock(self: Arc<Self>, _key: &str, _deadline: Duration) -> Result<LockContext> {
        Err(Error::Unsupported("DM.LOCK (Phase 7)"))
    }

    async fn lock_with_timeout(
        self: Arc<Self>,
        _key: &str,
        _lease: Duration,
        _deadline: Duration,
    ) -> Result<LockContext> {
        Err(Error::Unsupported("DM.LOCK (Phase 7)"))
    }

    async fn scan(&self, partition_id: u32, options: ScanOptions) -> Result<Box<dyn ScanCursor>> {
        // `DM.SCAN partID` is partition-pinned; the receiving node MUST
        // serve it (no MOVED in this path). Use any handle.
        let handle = self.first_handle().await?;
        let d = handle.new_dmap(&self.name, DMapOptions::default()).await?;
        d.scan(partition_id, options).await
    }

    async fn destroy(&self) -> Result<()> {
        // `DM.DESTROY` fans out to every owner — for Phase 4 we hit the
        // first handle and let the server-side fan-out machinery do its
        // job (Phase 5+ will wire the cluster-aware path).
        let handle = self.first_handle().await?;
        let d = handle.new_dmap(&self.name, DMapOptions::default()).await?;
        d.destroy().await
    }

    async fn unlock_internal(&self, _key: &str, _token: &[u8]) -> Result<()> {
        Err(Error::Unsupported("DM.LOCK (Phase 7)"))
    }

    async fn lease_internal(&self, _key: &str, _token: &[u8], _duration: Duration) -> Result<()> {
        Err(Error::Unsupported("DM.LOCK (Phase 7)"))
    }
}

/// Run `op` against the first handle; if it returns `Error::Moved { addr,
/// .. }`, open a handle to `addr` and run `op` again exactly once.
async fn with_moved_retry<T, F, Fut>(dmap: &MultiNodeDMap, mut op: F) -> Result<T>
where
    F: FnMut(Arc<RemoteClient>) -> Fut,
    Fut: std::future::Future<Output = Result<T>> + Send,
{
    let first = dmap.first_handle().await?;
    match op(first).await {
        Ok(v) => Ok(v),
        Err(Error::Moved { addr, .. }) => {
            // Cache the redirected handle so subsequent ops to the same
            // partition skip the bounce.
            let handle = match dmap.cached_handle(&addr) {
                Some(h) => h,
                None => dmap.owner.handle(&addr).await?,
            };
            op(handle).await
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error as ClientError;

    #[tokio::test]
    async fn empty_seeds_rejected() {
        let err = MultiNodeRemoteClient::connect(Vec::new(), None)
            .await
            .expect_err("empty seeds");
        assert!(matches!(err, ClientError::InvalidArgument(_)));
    }
}
