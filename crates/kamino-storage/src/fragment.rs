#![allow(clippy::doc_markdown)] // contract doc references are intentionally bare

//! `Fragment` — `tokio::sync::RwLock<Box<dyn StorageEngine>>` wrapper.
//!
//! One `Fragment` per `(dmap, partition)` pair. The `RwLock` keeps reads
//! concurrent while serialising writes (per `docs/07-concurrency.md`).

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::engine::StorageEngine;
use crate::entry::Entry;
use crate::error::Result;

/// Per-`(dmap, partition)` storage handle.
#[derive(Debug)]
pub struct Fragment {
    pub(crate) engine: RwLock<Box<dyn StorageEngine>>,
}

impl Fragment {
    /// Wrap an engine in a fresh fragment.
    #[must_use]
    pub fn new(engine: Box<dyn StorageEngine>) -> Arc<Self> {
        Arc::new(Self {
            engine: RwLock::new(engine),
        })
    }

    /// Store an entry under `hkey`, overwriting any prior value.
    pub async fn put(&self, hkey: u64, entry: &Entry) -> Result<()> {
        let mut guard = self.engine.write().await;
        guard.put(hkey, entry).await
    }

    /// LWW-merge variant of [`Self::put`]. See [`StorageEngine::put_lww`].
    pub async fn put_lww(&self, hkey: u64, entry: &Entry) -> Result<bool> {
        let mut guard = self.engine.write().await;
        guard.put_lww(hkey, entry).await
    }

    /// Retrieve the live entry for `hkey`, or `None`.
    pub async fn get(&self, hkey: u64) -> Result<Option<Entry>> {
        let guard = self.engine.read().await;
        guard.get(hkey).await
    }

    /// Delete the entry for `hkey`. Returns `true` if a live entry was deleted.
    pub async fn delete(&self, hkey: u64) -> Result<bool> {
        let mut guard = self.engine.write().await;
        guard.delete(hkey).await
    }

    /// Number of live entries.
    pub async fn len(&self) -> usize {
        self.engine.read().await.len()
    }

    /// `true` if there are no live entries.
    pub async fn is_empty(&self) -> bool {
        self.engine.read().await.is_empty()
    }

    /// Total bytes occupied by live entries.
    pub async fn inuse(&self) -> usize {
        self.engine.read().await.inuse()
    }

    /// Walk every live entry. The callback returns `false` to stop iteration.
    pub async fn scan<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(u64, &Entry) -> bool + Send,
    {
        let guard = self.engine.read().await;
        guard.scan(&mut callback).await
    }

    /// Walk every live entry whose key matches the regex `pattern`.
    pub async fn scan_regex_match<F>(&self, pattern: &str, mut callback: F) -> Result<()>
    where
        F: FnMut(u64, &Entry) -> bool + Send,
    {
        let guard = self.engine.read().await;
        guard.scan_regex_match(pattern, &mut callback).await
    }

    /// Export every live entry for migration.
    pub async fn export(&self) -> Result<Vec<u8>> {
        let guard = self.engine.read().await;
        guard.export().await
    }

    /// Import entries from an `export` payload. Existing entries are merged
    /// using LWW (`timestamp_nanos`).
    pub async fn import(&self, data: &[u8]) -> Result<()> {
        let mut guard = self.engine.write().await;
        guard.import(data).await
    }

    /// Run a compaction pass. Returns the number of bytes reclaimed.
    pub async fn compact(&self) -> Result<usize> {
        let mut guard = self.engine.write().await;
        guard.compact().await
    }
}

#[cfg(test)]
#[allow(clippy::redundant_pub_crate, clippy::significant_drop_tightening)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::engine::ScanCallback;

    /// HashMap-backed engine used only inside this crate's tests so the
    /// concurrency tests don't depend on Agent A's `RamBlock`.
    #[derive(Debug, Default)]
    pub(crate) struct MockEngine {
        inner: Mutex<HashMap<u64, Entry>>,
        bytes: Mutex<usize>,
    }

    impl MockEngine {
        pub(crate) fn boxed() -> Box<dyn StorageEngine> {
            Box::new(Self::default())
        }
    }

    #[async_trait]
    impl StorageEngine for MockEngine {
        async fn put(&mut self, hkey: u64, entry: &Entry) -> Result<()> {
            let mut map = self.inner.lock().unwrap();
            if let Some(prev) = map.insert(hkey, entry.clone()) {
                *self.bytes.lock().unwrap() -= prev.encoded_len();
            }
            *self.bytes.lock().unwrap() += entry.encoded_len();
            Ok(())
        }

        async fn get(&self, hkey: u64) -> Result<Option<Entry>> {
            Ok(self.inner.lock().unwrap().get(&hkey).cloned())
        }

        async fn delete(&mut self, hkey: u64) -> Result<bool> {
            let mut map = self.inner.lock().unwrap();
            if let Some(prev) = map.remove(&hkey) {
                *self.bytes.lock().unwrap() -= prev.encoded_len();
                Ok(true)
            } else {
                Ok(false)
            }
        }

        async fn scan(&self, callback: ScanCallback<'_>) -> Result<()> {
            let snapshot: Vec<(u64, Entry)> = {
                let map = self.inner.lock().unwrap();
                map.iter().map(|(k, v)| (*k, v.clone())).collect()
            };
            for (k, v) in &snapshot {
                if !callback(*k, v) {
                    break;
                }
            }
            Ok(())
        }

        async fn scan_regex_match(&self, pattern: &str, callback: ScanCallback<'_>) -> Result<()> {
            let re = regex::Regex::new(pattern).map_err(|e| crate::Error::Regex(e.to_string()))?;
            let snapshot: Vec<(u64, Entry)> = {
                let map = self.inner.lock().unwrap();
                map.iter().map(|(k, v)| (*k, v.clone())).collect()
            };
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

        async fn export(&self) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn import(&mut self, _data: &[u8]) -> Result<()> {
            Ok(())
        }
    }

    fn entry(key: &str, value: &str, ts: i64) -> Entry {
        Entry {
            key: key.as_bytes().to_vec(),
            ttl_nanos: 0,
            timestamp_nanos: ts,
            last_access_nanos: ts,
            value: value.as_bytes().to_vec(),
        }
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let frag = Fragment::new(MockEngine::boxed());
        let e = entry("hello", "world", 1);
        frag.put(1, &e).await.unwrap();
        assert_eq!(frag.get(1).await.unwrap(), Some(e));
        assert_eq!(frag.len().await, 1);
    }

    #[tokio::test]
    async fn delete_returns_true_only_when_present() {
        let frag = Fragment::new(MockEngine::boxed());
        frag.put(1, &entry("k", "v", 1)).await.unwrap();
        assert!(frag.delete(1).await.unwrap());
        assert!(!frag.delete(1).await.unwrap());
        assert!(frag.is_empty().await);
    }

    #[tokio::test]
    #[allow(clippy::cast_possible_wrap)]
    async fn scan_visits_every_entry() {
        let frag = Fragment::new(MockEngine::boxed());
        for i in 0..16_u64 {
            frag.put(i, &entry(&format!("k{i}"), "v", i as i64))
                .await
                .unwrap();
        }
        let mut seen = Vec::new();
        frag.scan(|h, _| {
            seen.push(h);
            true
        })
        .await
        .unwrap();
        seen.sort_unstable();
        assert_eq!(seen, (0..16).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn scan_regex_match_filters() {
        let frag = Fragment::new(MockEngine::boxed());
        frag.put(1, &entry("alpha", "v", 1)).await.unwrap();
        frag.put(2, &entry("beta", "v", 2)).await.unwrap();
        frag.put(3, &entry("alphabet", "v", 3)).await.unwrap();
        let mut keys = Vec::new();
        frag.scan_regex_match("^alpha", |_, e| {
            keys.push(String::from_utf8(e.key.clone()).unwrap());
            true
        })
        .await
        .unwrap();
        keys.sort();
        assert_eq!(keys, vec!["alpha".to_string(), "alphabet".to_string()]);
    }

    #[tokio::test]
    async fn concurrent_reads_share_lock() {
        // Two reads should be allowed to interleave on the read lock.
        let frag = Fragment::new(MockEngine::boxed());
        frag.put(1, &entry("k", "v", 1)).await.unwrap();
        let f1 = frag.clone();
        let f2 = frag.clone();
        let (a, b) = tokio::join!(f1.get(1), f2.get(1));
        assert!(a.unwrap().is_some());
        assert!(b.unwrap().is_some());
    }
}
