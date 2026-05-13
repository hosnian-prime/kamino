//! `EmbeddedClient` + `EmbeddedDMap` — in-process implementations of the
//! `Client` / `DMap` traits.
//!
//! In Phase 1 (embedded solo) each DMap owns exactly one [`Fragment`] — no
//! partitioning yet. The shapes already match the cluster case so Phase 4 can
//! generalise without rewriting the call sites.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kamino_core::{Clock, Hasher};
use kamino_storage::{Entry, Fragment, Locker, LruSampler};
use parking_lot::RwLock;
use rand::RngCore;
use tracing::debug;

use crate::cursor::{BufferedCursor, ScanCursor, ScanOptions, glob_to_regex};
use crate::dmap::DMap;
use crate::error::{Error, Result};
use crate::lock::LockContext;
use crate::migration::{decode_fragment_payload, encode_fragment_payload};
use crate::stats::{DMapStats, Stats, StatsOptions};
use crate::traits::Client;
use crate::types::{DMapOptions, GetResponse, PutOptions, micros_to_nanos_i64};

/// Factory the umbrella crate uses to produce a fresh engine per DMap.
///
/// The umbrella crate owns the choice of engine (RamBlock in prod, mock in
/// tests). The client only needs an opaque `Box<dyn StorageEngine>`.
pub type EngineFactory = Arc<dyn Fn() -> Box<dyn kamino_storage::StorageEngine> + Send + Sync>;

/// Bundle of dependencies passed in from the umbrella `Kamino` builder.
#[derive(Clone)]
pub struct EmbeddedDeps {
    pub clock: Arc<dyn Clock>,
    pub hasher: Arc<dyn Hasher>,
    pub locker: Arc<Locker>,
    pub engine_factory: EngineFactory,
    pub partition_count: u32,
}

impl std::fmt::Debug for EmbeddedDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedDeps")
            .field("partition_count", &self.partition_count)
            .finish_non_exhaustive()
    }
}

/// In-process `Client` implementation.
#[derive(Debug)]
pub struct EmbeddedClient {
    deps: EmbeddedDeps,
    dmaps: RwLock<BTreeMap<String, Arc<EmbeddedDMap>>>,
}

impl EmbeddedClient {
    /// Build a fresh client from the supplied dependencies.
    #[must_use]
    pub fn new(deps: EmbeddedDeps) -> Arc<Self> {
        Arc::new(Self {
            deps,
            dmaps: RwLock::new(BTreeMap::new()),
        })
    }

    /// Get-or-create a DMap. Used by both the `Client` trait and the umbrella
    /// `Kamino::embedded` setup path (which pre-creates DMaps from config).
    pub fn get_or_create(&self, name: &str, options: DMapOptions) -> Arc<EmbeddedDMap> {
        if let Some(existing) = self.dmaps.read().get(name) {
            return Arc::clone(existing);
        }
        let mut guard = self.dmaps.write();
        // Re-check after upgrade.
        if let Some(existing) = guard.get(name) {
            return Arc::clone(existing);
        }
        let dmap = EmbeddedDMap::new(name.to_string(), options, self.deps.clone());
        guard.insert(name.to_string(), Arc::clone(&dmap));
        dmap
    }

    /// Snapshot the list of currently-registered DMaps (used by stats + shutdown).
    pub fn registered_dmaps(&self) -> Vec<Arc<EmbeddedDMap>> {
        self.dmaps.read().values().cloned().collect()
    }

    /// Compute the routing-level partition id for `(dmap, key)`. Mirrors
    /// `kamino_cluster::partition_for` so the migration path uses exactly
    /// the same partition function as the routing table.
    fn partition_for(&self, dmap: &[u8], key: &[u8]) -> u32 {
        let mut buf = Vec::with_capacity(dmap.len() + key.len());
        buf.extend_from_slice(dmap);
        buf.extend_from_slice(key);
        let h = self.deps.hasher.hash64(&buf);
        u32::try_from(h % u64::from(self.deps.partition_count)).expect("modulo of u32 fits in u32")
    }
}

#[async_trait]
impl Client for EmbeddedClient {
    async fn new_dmap(&self, name: &str, options: DMapOptions) -> Result<Arc<dyn DMap>> {
        let dmap = self.get_or_create(name, options);
        Ok(dmap as Arc<dyn DMap>)
    }

    async fn stats(&self, _options: StatsOptions) -> Result<Stats> {
        let mut out = Stats::default();
        for dmap in self.registered_dmaps() {
            let frag = dmap.fragment.clone();
            let len = frag.len().await;
            let inuse = frag.inuse().await;
            out.dmaps
                .insert(dmap.name.clone(), DMapStats { len, inuse });
        }
        Ok(out)
    }

    fn partition_count(&self) -> u32 {
        self.deps.partition_count
    }

    async fn close(&self) -> Result<()> {
        // The umbrella owns the eviction workers; the client itself only owns
        // the DMap registry. Drop our references; if the umbrella also drops
        // its references the fragments will be reclaimed.
        self.dmaps.write().clear();
        Ok(())
    }

    async fn local_partitions(&self) -> Result<Vec<(String, u32)>> {
        let mut out: Vec<(String, u32)> = Vec::new();
        for dmap in self.registered_dmaps() {
            let mut seen: std::collections::HashSet<u32> = std::collections::HashSet::new();
            let dmap_name = dmap.name.as_bytes().to_vec();
            let mut cb = |_hk: u64, e: &Entry| -> bool {
                seen.insert(self.partition_for(&dmap_name, &e.key));
                true
            };
            dmap.fragment.scan(&mut cb).await?;
            let mut ids: Vec<u32> = seen.into_iter().collect();
            ids.sort_unstable();
            for id in ids {
                out.push((dmap.name.clone(), id));
            }
        }
        Ok(out)
    }

    async fn export_partition(&self, dmap: &str, partition_id: u32) -> Result<Vec<u8>> {
        let Some(handle) = self.dmaps.read().get(dmap).cloned() else {
            // Exporting a DMap that doesn't exist locally yields an empty
            // payload — caller treats this as a no-op migration.
            return Ok(encode_fragment_payload(partition_id, &[]));
        };
        let dmap_bytes = dmap.as_bytes().to_vec();
        let mut entries: Vec<Entry> = Vec::new();
        let now = handle.now_nanos();
        let mut cb = |_hk: u64, e: &Entry| -> bool {
            // Skip expired entries — they would be lazily purged on read
            // anyway, and shipping them across the wire wastes bandwidth.
            if e.is_expired(now) {
                return true;
            }
            if self.partition_for(&dmap_bytes, &e.key) == partition_id {
                entries.push(e.clone());
            }
            true
        };
        handle.fragment.scan(&mut cb).await?;
        Ok(encode_fragment_payload(partition_id, &entries))
    }

    async fn import_partition(&self, dmap: &str, partition_id: u32, payload: &[u8]) -> Result<u32> {
        let (embedded_part, entries) = decode_fragment_payload(payload)?;
        if embedded_part != partition_id {
            return Err(Error::InvalidArgument(format!(
                "fragment payload partition_id {embedded_part} != verb partition_id {partition_id}"
            )));
        }
        // Get-or-create the destination DMap with default options. The
        // receiving node has no per-DMap TOML overrides for an arbitrary
        // peer's DMap; defaults are the correct fallback. If the operator
        // pre-configured the DMap in TOML, that registration ran during
        // boot and `get_or_create` returns the existing handle.
        let handle = self.get_or_create(dmap, DMapOptions::default());
        let mut applied = 0_u32;
        for entry in entries {
            // Translate the storage hkey from the in-memory hasher (the same
            // one the receiver uses for puts/gets).
            let hkey = handle.hkey_bytes(&entry.key);
            let won = handle.fragment.put_lww(hkey, &entry).await?;
            if won {
                applied += 1;
                // Advance the receiver's LWW floor so subsequent local writes
                // never silently regress under the just-imported peer stamp.
                handle.observe_lww_timestamp(entry.timestamp_nanos);
            }
        }
        Ok(applied)
    }

    async fn clear_partition(&self, dmap: &str, partition_id: u32) -> Result<u32> {
        let Some(handle) = self.dmaps.read().get(dmap).cloned() else {
            return Ok(0);
        };
        let dmap_bytes = dmap.as_bytes().to_vec();
        let mut victims: Vec<u64> = Vec::new();
        let mut cb = |hk: u64, e: &Entry| -> bool {
            if self.partition_for(&dmap_bytes, &e.key) == partition_id {
                victims.push(hk);
            }
            true
        };
        handle.fragment.scan(&mut cb).await?;
        let mut deleted = 0_u32;
        for hk in victims {
            if handle.fragment.delete(hk).await? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}

/// One DMap instance in embedded solo mode.
pub struct EmbeddedDMap {
    name: String,
    options: DMapOptions,
    deps: EmbeddedDeps,
    /// Single fragment for Phase 1 (no partitioning yet).
    fragment: Arc<Fragment>,
    /// Simplified-HLC clock for LWW timestamps. Each accepted write claims
    /// `max(prev + 1, wall_time_nanos)`; client-supplied overrides advance
    /// the clock so a future-dated `TS` never lets a later local write
    /// silently regress. Phase 5 — `docs/04-replication.md` "Timestamp Source".
    monotonic_ts: AtomicI64,
}

impl std::fmt::Debug for EmbeddedDMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedDMap")
            .field("name", &self.name)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl EmbeddedDMap {
    fn new(name: String, options: DMapOptions, deps: EmbeddedDeps) -> Arc<Self> {
        let engine = (deps.engine_factory)();
        let fragment = Fragment::new(engine);
        Arc::new(Self {
            name,
            options,
            deps,
            fragment,
            monotonic_ts: AtomicI64::new(0),
        })
    }

    /// Claim the next LWW timestamp:
    /// `next = max(monotonic_ts + 1, wall_time_nanos)` and store it back.
    /// CAS loop matches `docs/04-replication.md` simplified-HLC contract: a
    /// single primary's writes are totally ordered, and a future wall-clock
    /// jump never decreases the clock.
    fn next_lww_timestamp(&self) -> i64 {
        let now = self.now_nanos();
        loop {
            let prev = self.monotonic_ts.load(Ordering::Acquire);
            let candidate = prev.saturating_add(1).max(now);
            if self
                .monotonic_ts
                .compare_exchange_weak(prev, candidate, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return candidate;
            }
        }
    }

    /// Advance the monotonic clock so it tracks `observed`. Used when an
    /// override `TS` arrives (client-supplied or peer-supplied replication)
    /// so subsequent default writes keep the strictly-increasing property.
    fn observe_lww_timestamp(&self, observed: i64) {
        // `fetch_max` is the perfect primitive here — only advances on a
        // strictly larger value. Relaxed memory ordering would be fine since
        // we don't synchronise other memory through this slot, but use
        // AcqRel to match the CAS loop's ordering for readability.
        self.monotonic_ts.fetch_max(observed, Ordering::AcqRel);
    }

    /// Read-only access to the fragment — the umbrella crate consumes this
    /// for its background eviction workers.
    #[must_use]
    pub fn fragment(&self) -> Arc<Fragment> {
        Arc::clone(&self.fragment)
    }

    /// Read-only access to the per-DMap options.
    #[must_use]
    pub const fn options(&self) -> &DMapOptions {
        &self.options
    }

    fn hkey(&self, key: &str) -> u64 {
        self.deps.hasher.hash64(key.as_bytes())
    }

    /// Same as [`Self::hkey`] but for already-decoded byte slices. Used by
    /// the Phase 6 migration import path so the receiver hashes the entry's
    /// raw key bytes with the same hasher instance the sender's writes used.
    pub(crate) fn hkey_bytes(&self, key: &[u8]) -> u64 {
        self.deps.hasher.hash64(key)
    }

    fn now_nanos(&self) -> i64 {
        micros_to_nanos_i64(self.deps.clock.now_micros())
    }

    fn to_get_response(&self, e: &Entry) -> GetResponse {
        let now = self.now_nanos();
        let ttl = if e.ttl_nanos == 0 {
            None
        } else {
            let remaining = e.ttl_nanos.saturating_sub(now);
            if remaining <= 0 {
                Some(Duration::ZERO)
            } else {
                Some(Duration::from_nanos(u64::try_from(remaining).unwrap_or(0)))
            }
        };
        GetResponse {
            value: e.value.clone(),
            timestamp: e.timestamp_nanos,
            ttl,
        }
    }

    async fn fetch_live(&self, key: &str) -> Result<Option<Entry>> {
        let hkey = self.hkey(key);
        let entry = self.fragment.get(hkey).await?;
        let Some(entry) = entry else { return Ok(None) };
        let now = self.now_nanos();
        if entry.is_expired(now) {
            // Lazy expiry on read.
            let _ = self.fragment.delete(hkey).await;
            Ok(None)
        } else {
            Ok(Some(entry))
        }
    }

    async fn evict_if_needed(&self) {
        let policy = self.options.eviction_policy;
        if !matches!(policy, Some(kamino_core::EvictionPolicy::Lru)) {
            return;
        }
        let samples = self.options.lru_samples.unwrap_or(5) as usize;
        loop {
            let len = self.fragment.len().await as u64;
            let inuse = self.fragment.inuse().await as u64;
            let over_keys = self.options.max_keys.is_some_and(|m| len > m);
            let over_bytes = self.options.max_inuse.is_some_and(|m| inuse > m);
            if !over_keys && !over_bytes {
                return;
            }
            if !LruSampler::evict_one(&self.fragment, samples).await {
                return;
            }
        }
    }

    async fn put_internal(&self, key: &str, value: &[u8], options: PutOptions) -> Result<()> {
        options.validate()?;
        let hkey = self.hkey(key);
        let now = self.now_nanos();

        let existing_live = self.fetch_live(key).await?;
        if options.nx && existing_live.is_some() {
            return Err(Error::KeyAlreadyExists);
        }
        if options.xx && existing_live.is_none() {
            return Err(Error::KeyNotExists);
        }

        let ttl_nanos = options.resolve_ttl_nanos(self.deps.clock.as_ref(), self.options.ttl)?;
        // Client-supplied TS overrides; otherwise claim a strictly-increasing
        // monotonic stamp so consecutive writes are totally ordered even when
        // the wall clock doesn't tick.
        let timestamp = options.timestamp.map_or_else(
            || self.next_lww_timestamp(),
            |ts| {
                self.observe_lww_timestamp(ts);
                ts
            },
        );

        let entry = Entry {
            key: key.as_bytes().to_vec(),
            ttl_nanos,
            timestamp_nanos: timestamp,
            last_access_nanos: now,
            value: value.to_vec(),
        };
        self.fragment.put(hkey, &entry).await?;
        self.evict_if_needed().await;
        Ok(())
    }

    /// LWW-merge variant used by Phase 5 backup replication.
    ///
    /// Unlike [`Self::put_internal`] this:
    /// - requires `options.timestamp` (primary stamped the write already);
    /// - bypasses `nx` / `xx` (the primary already enforced these);
    /// - returns `false` when the existing entry's `timestamp_nanos` already
    ///   meets-or-exceeds the incoming TS (replication arrived out of order).
    async fn put_lww_internal(&self, key: &str, value: &[u8], options: PutOptions) -> Result<bool> {
        options.validate()?;
        let Some(timestamp) = options.timestamp else {
            return Err(Error::InvalidArgument(
                "put_lww requires an explicit timestamp (primary's LWW stamp)".into(),
            ));
        };
        self.observe_lww_timestamp(timestamp);
        let hkey = self.hkey(key);
        let now = self.now_nanos();
        let ttl_nanos = options.resolve_ttl_nanos(self.deps.clock.as_ref(), self.options.ttl)?;
        let entry = Entry {
            key: key.as_bytes().to_vec(),
            ttl_nanos,
            timestamp_nanos: timestamp,
            last_access_nanos: now,
            value: value.to_vec(),
        };
        let applied = self.fragment.put_lww(hkey, &entry).await?;
        if applied {
            self.evict_if_needed().await;
        }
        Ok(applied)
    }

    async fn touch_last_access(&self, key: &str, entry: &Entry) {
        let hkey = self.hkey(key);
        let mut updated = entry.clone();
        updated.last_access_nanos = self.now_nanos();
        if let Err(err) = self.fragment.put(hkey, &updated).await {
            debug!(?err, "best-effort last_access update failed");
        }
    }

    async fn acquire_lock(
        self: Arc<Self>,
        key: &str,
        lease: Option<Duration>,
        deadline: Duration,
    ) -> Result<LockContext> {
        let token = random_token();
        let start = self.deps.clock.now_monotonic();
        // Sleep budget: 10ms per docs/10.
        let retry = Duration::from_millis(10);
        loop {
            let mut opts = PutOptions {
                nx: true,
                ..PutOptions::default()
            };
            if let Some(l) = lease {
                opts.px = Some(u64::try_from(l.as_millis()).unwrap_or(u64::MAX));
            }
            match self.put_internal(key, &token, opts).await {
                Ok(()) => {
                    let dmap_handle: Arc<dyn DMap> = Arc::<Self>::clone(&self);
                    return Ok(LockContext::new(
                        token,
                        &dmap_handle,
                        self.name.clone(),
                        key.to_string(),
                    ));
                }
                Err(Error::KeyAlreadyExists) => {
                    if start.elapsed() >= deadline {
                        return Err(Error::LockNotAcquired);
                    }
                    let remaining = deadline.saturating_sub(start.elapsed());
                    tokio::time::sleep(retry.min(remaining)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

fn random_token() -> Vec<u8> {
    let mut buf = vec![0_u8; 16];
    rand::thread_rng().fill_bytes(&mut buf);
    buf
}

#[async_trait]
impl DMap for EmbeddedDMap {
    fn name(&self) -> &str {
        &self.name
    }

    async fn put(&self, key: &str, value: &[u8], options: PutOptions) -> Result<()> {
        self.put_internal(key, value, options).await
    }

    async fn put_lww(&self, key: &str, value: &[u8], options: PutOptions) -> Result<bool> {
        self.put_lww_internal(key, value, options).await
    }

    async fn get(&self, key: &str) -> Result<GetResponse> {
        let entry = self.fetch_live(key).await?.ok_or(Error::KeyNotFound)?;
        let resp = self.to_get_response(&entry);
        // Best-effort idle bump — needed so IdleSweeper works as docs/05
        // describes ("every get/put/expire/lock updates last_access").
        self.touch_last_access(key, &entry).await;
        Ok(resp)
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let hkey = self.hkey(key);
        Ok(self.fragment.delete(hkey).await?)
    }

    async fn incr(&self, key: &str, delta: i64) -> Result<i64> {
        let _guard = self
            .deps
            .locker
            .acquire(&self.name, key.as_bytes(), Duration::from_secs(5))
            .await
            .map_err(|_| Error::Timeout)?;
        let current = match self.fetch_live(key).await? {
            None => 0_i64,
            Some(e) => std::str::from_utf8(&e.value)
                .map_err(|err| Error::Serialization(err.to_string()))?
                .parse::<i64>()
                .map_err(|err| Error::NotANumber {
                    expected: "integer",
                    got: err.to_string(),
                })?,
        };
        let new = current.saturating_add(delta);
        self.put_internal(key, new.to_string().as_bytes(), PutOptions::default())
            .await?;
        Ok(new)
    }

    async fn decr(&self, key: &str, delta: i64) -> Result<i64> {
        self.incr(key, delta.wrapping_neg()).await
    }

    async fn incr_by_float(&self, key: &str, delta: f64) -> Result<f64> {
        let _guard = self
            .deps
            .locker
            .acquire(&self.name, key.as_bytes(), Duration::from_secs(5))
            .await
            .map_err(|_| Error::Timeout)?;
        let current = match self.fetch_live(key).await? {
            None => 0.0_f64,
            Some(e) => std::str::from_utf8(&e.value)
                .map_err(|err| Error::Serialization(err.to_string()))?
                .parse::<f64>()
                .map_err(|err| Error::NotANumber {
                    expected: "float",
                    got: err.to_string(),
                })?,
        };
        let new = current + delta;
        self.put_internal(key, new.to_string().as_bytes(), PutOptions::default())
            .await?;
        Ok(new)
    }

    async fn get_put(&self, key: &str, value: &[u8]) -> Result<Option<GetResponse>> {
        let _guard = self
            .deps
            .locker
            .acquire(&self.name, key.as_bytes(), Duration::from_secs(5))
            .await
            .map_err(|_| Error::Timeout)?;
        let previous = self
            .fetch_live(key)
            .await?
            .map(|e| self.to_get_response(&e));
        self.put_internal(key, value, PutOptions::default()).await?;
        Ok(previous)
    }

    async fn expire(&self, key: &str, duration: Duration) -> Result<()> {
        let _guard = self
            .deps
            .locker
            .acquire(&self.name, key.as_bytes(), Duration::from_secs(5))
            .await
            .map_err(|_| Error::Timeout)?;
        let entry = self.fetch_live(key).await?.ok_or(Error::KeyNotFound)?;
        let now = self.now_nanos();
        let dur_nanos = i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX);
        let new_ttl = if duration.is_zero() {
            0
        } else {
            now.saturating_add(dur_nanos)
        };
        let updated = Entry {
            ttl_nanos: new_ttl,
            last_access_nanos: now,
            ..entry
        };
        let hkey = self.hkey(key);
        self.fragment.put(hkey, &updated).await?;
        Ok(())
    }

    async fn lock(self: Arc<Self>, key: &str, deadline: Duration) -> Result<LockContext> {
        self.acquire_lock(key, None, deadline).await
    }

    async fn lock_with_timeout(
        self: Arc<Self>,
        key: &str,
        lease: Duration,
        deadline: Duration,
    ) -> Result<LockContext> {
        self.acquire_lock(key, Some(lease), deadline).await
    }

    async fn scan(&self, partition_id: u32, options: ScanOptions) -> Result<Box<dyn ScanCursor>> {
        if partition_id >= self.deps.partition_count {
            return Err(Error::InvalidArgument(format!(
                "partition_id {} >= partition_count {}",
                partition_id, self.deps.partition_count
            )));
        }
        // Embedded solo: all data lives in the single fragment regardless of
        // partition_id. We accept the id only to keep the API shape stable
        // for Phase 4. Only partition 0 yields data; other partitions return
        // an empty cursor.
        if partition_id != 0 {
            return Ok(Box::new(BufferedCursor::new(Vec::new())));
        }
        let now = self.now_nanos();
        let mut collected: Vec<(String, Vec<u8>)> = Vec::new();
        let push = |k: &[u8], v: &[u8], out: &mut Vec<(String, Vec<u8>)>| {
            if let Ok(s) = std::str::from_utf8(k) {
                out.push((s.to_string(), v.to_vec()));
            }
        };
        match options.match_pattern {
            None => {
                self.fragment
                    .scan(|_, e| {
                        if !e.is_expired(now) {
                            push(&e.key, &e.value, &mut collected);
                        }
                        true
                    })
                    .await?;
            }
            Some(pattern) => {
                let regex = glob_to_regex(&pattern);
                self.fragment
                    .scan_regex_match(&regex, |_, e| {
                        if !e.is_expired(now) {
                            push(&e.key, &e.value, &mut collected);
                        }
                        true
                    })
                    .await?;
            }
        }
        Ok(Box::new(BufferedCursor::new(collected)))
    }

    async fn destroy(&self) -> Result<()> {
        // Drop every entry; the fragment + engine stay around until the DMap
        // itself is dropped from the registry (which is the orchestrator's
        // job, not the DMap's).
        let mut victims: Vec<u64> = Vec::new();
        self.fragment
            .scan(|h, _| {
                victims.push(h);
                true
            })
            .await?;
        for h in victims {
            let _ = self.fragment.delete(h).await;
        }
        Ok(())
    }

    async fn unlock_internal(&self, key: &str, token: &[u8]) -> Result<()> {
        let _guard = self
            .deps
            .locker
            .acquire(&self.name, key.as_bytes(), Duration::from_secs(5))
            .await
            .map_err(|_| Error::Timeout)?;
        let Some(entry) = self.fetch_live(key).await? else {
            return Err(Error::NoSuchLock);
        };
        if entry.value != token {
            return Err(Error::NoSuchLock);
        }
        let hkey = self.hkey(key);
        self.fragment.delete(hkey).await?;
        Ok(())
    }

    async fn lease_internal(&self, key: &str, token: &[u8], duration: Duration) -> Result<()> {
        let _guard = self
            .deps
            .locker
            .acquire(&self.name, key.as_bytes(), Duration::from_secs(5))
            .await
            .map_err(|_| Error::Timeout)?;
        let Some(entry) = self.fetch_live(key).await? else {
            return Err(Error::NoSuchLock);
        };
        if entry.value != token {
            return Err(Error::NoSuchLock);
        }
        let now = self.now_nanos();
        let dur_nanos = i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX);
        let new_ttl = if duration.is_zero() {
            0
        } else {
            now.saturating_add(dur_nanos)
        };
        let updated = Entry {
            ttl_nanos: new_ttl,
            last_access_nanos: now,
            ..entry
        };
        let hkey = self.hkey(key);
        self.fragment.put(hkey, &updated).await?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::significant_drop_tightening, clippy::manual_let_else)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use kamino_core::{SystemClock, XxHasher};
    use kamino_storage::{Entry, ScanCallback, StorageEngine};

    use super::*;

    #[derive(Debug, Default)]
    struct MockEngine {
        inner: Mutex<HashMap<u64, Entry>>,
        bytes: Mutex<usize>,
    }

    #[async_trait]
    impl StorageEngine for MockEngine {
        async fn put(&mut self, hkey: u64, entry: &Entry) -> kamino_storage::Result<()> {
            let mut map = self.inner.lock().unwrap();
            if let Some(prev) = map.insert(hkey, entry.clone()) {
                *self.bytes.lock().unwrap() -= prev.encoded_len();
            }
            *self.bytes.lock().unwrap() += entry.encoded_len();
            Ok(())
        }
        async fn get(&self, hkey: u64) -> kamino_storage::Result<Option<Entry>> {
            Ok(self.inner.lock().unwrap().get(&hkey).cloned())
        }
        async fn delete(&mut self, hkey: u64) -> kamino_storage::Result<bool> {
            let mut map = self.inner.lock().unwrap();
            if let Some(prev) = map.remove(&hkey) {
                *self.bytes.lock().unwrap() -= prev.encoded_len();
                Ok(true)
            } else {
                Ok(false)
            }
        }
        async fn scan(&self, callback: ScanCallback<'_>) -> kamino_storage::Result<()> {
            let snapshot: Vec<(u64, Entry)> = self
                .inner
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect();
            for (k, v) in &snapshot {
                if !callback(*k, v) {
                    break;
                }
            }
            Ok(())
        }
        async fn scan_regex_match(
            &self,
            pattern: &str,
            callback: ScanCallback<'_>,
        ) -> kamino_storage::Result<()> {
            let re = regex::Regex::new(pattern)
                .map_err(|e| kamino_storage::Error::Regex(e.to_string()))?;
            let snapshot: Vec<(u64, Entry)> = self
                .inner
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect();
            for (k, v) in &snapshot {
                if let Ok(s) = std::str::from_utf8(&v.key) {
                    if re.is_match(s) && !callback(*k, v) {
                        break;
                    }
                }
            }
            Ok(())
        }
        fn len(&self) -> usize {
            self.inner.lock().unwrap().len()
        }
        fn inuse(&self) -> usize {
            *self.bytes.lock().unwrap()
        }
        async fn export(&self) -> kamino_storage::Result<Vec<u8>> {
            Ok(Vec::new())
        }
        async fn import(&mut self, _data: &[u8]) -> kamino_storage::Result<()> {
            Ok(())
        }
    }

    fn deps() -> EmbeddedDeps {
        EmbeddedDeps {
            clock: Arc::new(SystemClock),
            hasher: Arc::new(XxHasher),
            locker: Locker::new(),
            engine_factory: Arc::new(|| Box::new(MockEngine::default())),
            partition_count: 1,
        }
    }

    fn client() -> Arc<EmbeddedClient> {
        EmbeddedClient::new(deps())
    }

    async fn dmap() -> Arc<dyn DMap> {
        client()
            .new_dmap("d", DMapOptions::default())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn put_get_delete_roundtrip() {
        let d = dmap().await;
        d.put("k", b"v", PutOptions::default()).await.unwrap();
        let r = d.get("k").await.unwrap();
        assert_eq!(r.as_str().unwrap(), "v");
        assert!(d.delete("k").await.unwrap());
        assert!(matches!(d.get("k").await, Err(Error::KeyNotFound)));
    }

    #[tokio::test]
    async fn put_nx_rejects_existing() {
        let d = dmap().await;
        d.put("k", b"a", PutOptions::default()).await.unwrap();
        let r = d
            .put(
                "k",
                b"b",
                PutOptions {
                    nx: true,
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(r, Err(Error::KeyAlreadyExists)));
    }

    #[tokio::test]
    async fn put_xx_requires_existing() {
        let d = dmap().await;
        let r = d
            .put(
                "k",
                b"a",
                PutOptions {
                    xx: true,
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(r, Err(Error::KeyNotExists)));
    }

    #[tokio::test]
    async fn expire_clears_ttl_on_zero_duration() {
        let d = dmap().await;
        d.put(
            "k",
            b"v",
            PutOptions {
                ex: Some(60),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let r = d.get("k").await.unwrap();
        assert!(r.ttl.is_some());
        d.expire("k", Duration::ZERO).await.unwrap();
        let r = d.get("k").await.unwrap();
        assert!(r.ttl.is_none());
    }

    #[tokio::test]
    async fn expire_then_get_misses() {
        let d = dmap().await;
        d.put("k", b"v", PutOptions::default()).await.unwrap();
        d.expire("k", Duration::from_millis(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(matches!(d.get("k").await, Err(Error::KeyNotFound)));
    }

    #[tokio::test]
    async fn expire_on_missing_key_errors() {
        let d = dmap().await;
        let err = d
            .expire("missing", Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::KeyNotFound));
    }

    #[tokio::test]
    async fn incr_creates_then_increments() {
        let d = dmap().await;
        assert_eq!(d.incr("c", 1).await.unwrap(), 1);
        assert_eq!(d.incr("c", 5).await.unwrap(), 6);
        assert_eq!(d.decr("c", 2).await.unwrap(), 4);
    }

    #[tokio::test]
    async fn incr_by_float_works() {
        let d = dmap().await;
        let v = d.incr_by_float("f", 1.5).await.unwrap();
        assert!((v - 1.5).abs() < 1e-9);
        let v = d.incr_by_float("f", 0.5).await.unwrap();
        assert!((v - 2.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn get_put_returns_previous() {
        let d = dmap().await;
        assert!(d.get_put("k", b"a").await.unwrap().is_none());
        let prev = d.get_put("k", b"b").await.unwrap().unwrap();
        assert_eq!(prev.as_str().unwrap(), "a");
        let now = d.get("k").await.unwrap();
        assert_eq!(now.as_str().unwrap(), "b");
    }

    #[tokio::test]
    async fn scan_returns_all_keys_for_partition_zero() {
        let d = dmap().await;
        d.put("a", b"1", PutOptions::default()).await.unwrap();
        d.put("b", b"2", PutOptions::default()).await.unwrap();
        let mut cur = d.scan(0, ScanOptions::default()).await.unwrap();
        let mut got: Vec<String> = Vec::new();
        while let Some((k, _)) = cur.next().await.unwrap() {
            got.push(k);
        }
        got.sort();
        assert_eq!(got, vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test]
    async fn scan_match_pattern_filters() {
        let d = dmap().await;
        d.put("user:1", b"x", PutOptions::default()).await.unwrap();
        d.put("user:2", b"x", PutOptions::default()).await.unwrap();
        d.put("admin:1", b"x", PutOptions::default()).await.unwrap();
        let mut cur = d
            .scan(
                0,
                ScanOptions {
                    match_pattern: Some("user:*".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut got = Vec::new();
        while let Some((k, _)) = cur.next().await.unwrap() {
            got.push(k);
        }
        got.sort();
        assert_eq!(got, vec!["user:1".to_string(), "user:2".to_string()]);
    }

    #[tokio::test]
    async fn lock_and_unlock_roundtrip() {
        let client = client();
        let typed = client.get_or_create("d", DMapOptions::default());
        let ctx = typed
            .clone()
            .lock_with_timeout("res", Duration::from_secs(5), Duration::from_secs(1))
            .await
            .unwrap();
        // Re-acquiring while held must time out.
        let err = typed
            .clone()
            .lock_with_timeout("res", Duration::from_secs(5), Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::LockNotAcquired));
        ctx.unlock().await.unwrap();
        // After unlock, acquire succeeds again.
        let ctx2 = typed
            .clone()
            .lock_with_timeout("res", Duration::from_secs(5), Duration::from_secs(1))
            .await
            .unwrap();
        ctx2.unlock().await.unwrap();
    }

    #[tokio::test]
    async fn unlock_with_wrong_token_fails() {
        let client = client();
        let typed = client.get_or_create("d", DMapOptions::default());
        let ctx = typed
            .clone()
            .lock_with_timeout("k", Duration::from_secs(5), Duration::from_secs(1))
            .await
            .unwrap();
        let res = typed.unlock_internal("k", b"wrong-token").await;
        assert!(matches!(res, Err(Error::NoSuchLock)));
        ctx.unlock().await.unwrap();
    }

    #[tokio::test]
    async fn stats_reports_per_dmap_counts() {
        let client = client();
        let d = client.new_dmap("a", DMapOptions::default()).await.unwrap();
        d.put("x", b"y", PutOptions::default()).await.unwrap();
        let stats = client.stats(StatsOptions).await.unwrap();
        assert_eq!(stats.dmaps["a"].len, 1);
        assert!(stats.dmaps["a"].inuse > 0);
    }

    #[tokio::test]
    async fn destroy_empties_dmap() {
        let d = dmap().await;
        for i in 0..10 {
            d.put(&format!("k{i}"), b"v", PutOptions::default())
                .await
                .unwrap();
        }
        d.destroy().await.unwrap();
        let mut c = d.scan(0, ScanOptions::default()).await.unwrap();
        assert!(c.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn invalid_partition_id_rejected() {
        let d = dmap().await;
        let err = match d.scan(99, ScanOptions::default()).await {
            Err(e) => e,
            Ok(_) => panic!("expected InvalidArgument"),
        };
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn nx_and_xx_both_set_rejected() {
        let d = dmap().await;
        let err = d
            .put(
                "k",
                b"v",
                PutOptions {
                    nx: true,
                    xx: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    // ---------- Phase 5: monotonic LWW timestamp + put_lww ----------

    #[tokio::test]
    async fn default_put_timestamps_are_strictly_monotonic() {
        // Two consecutive default-clocked puts must produce strictly
        // increasing `timestamp_nanos`. Even when the system clock has not
        // ticked between calls, `max(prev + 1, wall)` guarantees ordering.
        let d = dmap().await;
        d.put("k", b"v1", PutOptions::default()).await.unwrap();
        let r1 = d.get("k").await.unwrap();
        d.put("k", b"v2", PutOptions::default()).await.unwrap();
        let r2 = d.get("k").await.unwrap();
        assert!(
            r2.timestamp > r1.timestamp,
            "want monotonic, got {} -> {}",
            r1.timestamp,
            r2.timestamp,
        );
    }

    #[tokio::test]
    async fn client_override_ts_does_not_regress_clock() {
        // After a far-future override TS, the next default-clocked write
        // must still claim a strictly greater stamp.
        let d = dmap().await;
        let far_future = 4_000_000_000_000_000_000_i64; // year ~2096
        d.put(
            "k",
            b"v1",
            PutOptions {
                timestamp: Some(far_future),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        d.put("k2", b"v2", PutOptions::default()).await.unwrap();
        let r = d.get("k2").await.unwrap();
        assert!(
            r.timestamp > far_future,
            "default TS must exceed previous override {far_future}, got {}",
            r.timestamp,
        );
    }

    #[tokio::test]
    async fn put_lww_requires_explicit_timestamp() {
        let d = dmap().await;
        let err = d
            .put_lww("k", b"v", PutOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn put_lww_applies_newer_ts() {
        let d = dmap().await;
        d.put(
            "k",
            b"old",
            PutOptions {
                timestamp: Some(100),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let applied = d
            .put_lww(
                "k",
                b"new",
                PutOptions {
                    timestamp: Some(200),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(applied);
        assert_eq!(d.get("k").await.unwrap().value, b"new");
    }

    #[tokio::test]
    async fn put_lww_rejects_older_ts() {
        let d = dmap().await;
        d.put(
            "k",
            b"new",
            PutOptions {
                timestamp: Some(200),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let applied = d
            .put_lww(
                "k",
                b"old",
                PutOptions {
                    timestamp: Some(100),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(!applied, "older TS must lose the LWW merge");
        assert_eq!(d.get("k").await.unwrap().value, b"new");
    }

    // ---- Phase 6 partition migration ----------------------------------------

    /// Multi-partition deps so the partition_for filter actually buckets keys
    /// into more than one bucket.
    fn deps_multi(partition_count: u32) -> EmbeddedDeps {
        EmbeddedDeps {
            partition_count,
            ..deps()
        }
    }

    #[tokio::test]
    async fn local_partitions_lists_every_distinct_bucket() {
        let c = EmbeddedClient::new(deps_multi(8));
        let d = c.new_dmap("dm", DMapOptions::default()).await.unwrap();
        // 256 keys cover every partition with very high probability for 8
        // partitions under xxHash.
        for i in 0..256 {
            d.put(&format!("k{i:03}"), b"v", PutOptions::default())
                .await
                .unwrap();
        }
        let parts = c.local_partitions().await.unwrap();
        assert!(!parts.is_empty());
        assert!(parts.iter().all(|(name, _)| name == "dm"));
        let distinct: std::collections::HashSet<u32> = parts.iter().map(|(_, p)| *p).collect();
        assert!(
            distinct.len() >= 4,
            "expected ≥ 4 distinct partitions with 256 keys / 8 buckets, got {}",
            distinct.len()
        );
    }

    #[tokio::test]
    async fn export_partition_filters_by_routing_hash() {
        let c = EmbeddedClient::new(deps_multi(8));
        let d = c.new_dmap("dm", DMapOptions::default()).await.unwrap();
        for i in 0..64 {
            d.put(&format!("k{i:02}"), b"v", PutOptions::default())
                .await
                .unwrap();
        }
        // Pick a single partition we know has at least one key.
        let parts = c.local_partitions().await.unwrap();
        let (_, target) = parts.first().cloned().expect("at least one partition");
        let payload = c.export_partition("dm", target).await.unwrap();
        let (back_part, entries) = decode_fragment_payload(&payload).unwrap();
        assert_eq!(back_part, target);
        assert!(!entries.is_empty());
        for e in &entries {
            let p = c.partition_for(b"dm", &e.key);
            assert_eq!(p, target, "every exported entry must hash into {target}");
        }
    }

    #[tokio::test]
    async fn import_partition_lww_merges_into_destination() {
        let src = EmbeddedClient::new(deps_multi(4));
        let dst = EmbeddedClient::new(deps_multi(4));
        let _ = src.new_dmap("dm", DMapOptions::default()).await.unwrap();
        // Seed source with 32 keys, then export every partition the keys
        // landed in. Receiver imports each blob and ends up with the same
        // (key -> value) view.
        let d_src = src.get_or_create("dm", DMapOptions::default());
        for i in 0..32 {
            d_src
                .put(
                    &format!("k{i:02}"),
                    format!("v{i:02}").as_bytes(),
                    PutOptions {
                        timestamp: Some(1000 + i64::from(i)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }
        for (dmap, part) in src.local_partitions().await.unwrap() {
            let payload = src.export_partition(&dmap, part).await.unwrap();
            let applied = dst.import_partition(&dmap, part, &payload).await.unwrap();
            assert!(applied > 0, "import must accept fresh entries");
        }
        // Spot-check every key.
        let d_dst = dst.get_or_create("dm", DMapOptions::default());
        for i in 0..32 {
            let got = d_dst.get(&format!("k{i:02}")).await.unwrap();
            assert_eq!(got.as_str().unwrap(), format!("v{i:02}"));
        }
    }

    #[tokio::test]
    async fn import_partition_rejects_payload_partition_mismatch() {
        let src = EmbeddedClient::new(deps_multi(4));
        let dst = EmbeddedClient::new(deps_multi(4));
        let _ = src.new_dmap("dm", DMapOptions::default()).await.unwrap();
        let d_src = src.get_or_create("dm", DMapOptions::default());
        d_src.put("k", b"v", PutOptions::default()).await.unwrap();
        let parts = src.local_partitions().await.unwrap();
        let (dmap, part) = parts.into_iter().next().unwrap();
        let payload = src.export_partition(&dmap, part).await.unwrap();
        // Replay against a *different* partition id — receiver must refuse.
        let wrong = (part + 1) % 4;
        let err = dst
            .import_partition(&dmap, wrong, &payload)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn import_partition_lww_preserves_newer_destination_entry() {
        let src = EmbeddedClient::new(deps_multi(2));
        let dst = EmbeddedClient::new(deps_multi(2));
        let _ = src.new_dmap("dm", DMapOptions::default()).await.unwrap();
        let _ = dst.new_dmap("dm", DMapOptions::default()).await.unwrap();
        let d_src = src.get_or_create("dm", DMapOptions::default());
        let d_dst = dst.get_or_create("dm", DMapOptions::default());

        // Destination holds the newer write; source ships an older copy that
        // must lose the LWW merge.
        d_src
            .put(
                "shared",
                b"old",
                PutOptions {
                    timestamp: Some(100),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        d_dst
            .put(
                "shared",
                b"new",
                PutOptions {
                    timestamp: Some(200),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let parts = src.local_partitions().await.unwrap();
        let (dmap, part) = parts.into_iter().next().unwrap();
        let payload = src.export_partition(&dmap, part).await.unwrap();
        let applied = dst.import_partition(&dmap, part, &payload).await.unwrap();
        assert_eq!(applied, 0, "older payload entry must lose merge");
        assert_eq!(d_dst.get("shared").await.unwrap().value, b"new");
    }

    #[tokio::test]
    async fn clear_partition_removes_only_matching_entries() {
        let c = EmbeddedClient::new(deps_multi(4));
        let d = c.new_dmap("dm", DMapOptions::default()).await.unwrap();
        for i in 0..40 {
            d.put(&format!("k{i:02}"), b"v", PutOptions::default())
                .await
                .unwrap();
        }
        let parts = c.local_partitions().await.unwrap();
        let (dmap, target) = parts.first().cloned().expect("at least one partition");
        let before_total = c.local_partitions().await.unwrap().len();
        let cleared = c.clear_partition(&dmap, target).await.unwrap();
        assert!(cleared > 0);
        // After clearing, the target partition must be gone from
        // local_partitions, but other partitions persist.
        let after = c.local_partitions().await.unwrap();
        let still_has_target = after.iter().any(|(_, p)| *p == target);
        assert!(!still_has_target, "cleared partition must vanish");
        assert!(
            after.len() < before_total,
            "expected fewer partitions after clear: before={before_total} after={}",
            after.len()
        );
    }
}
