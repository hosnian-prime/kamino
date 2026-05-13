//! End-to-end smoke test for the `Kamino::embedded` builder.
//!
//! Until Agent A's `RamBlock` ships, this test uses the
//! `Kamino::embedded_with_engine` constructor with a HashMap-backed mock
//! engine. Once `RamBlock` is real, swap the factory for `RamBlock::new(...)`
//! (or use the default `Kamino::embedded` path directly).
#![allow(
    clippy::significant_drop_tightening,
    clippy::field_reassign_with_default
)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kamino::{
    Config, DMap, DMapOptions, Entry, Kamino, Mode, PutOptions, ScanOptions, StorageEngine,
    StorageError,
};
use kamino_client::embedded::EngineFactory;
use kamino_core::DMapConfig;
use kamino_storage::ScanCallback;

#[derive(Debug, Default)]
struct MockEngine {
    inner: Mutex<HashMap<u64, Entry>>,
    bytes: Mutex<usize>,
}

#[async_trait]
impl StorageEngine for MockEngine {
    async fn put(&mut self, hkey: u64, entry: &Entry) -> Result<(), StorageError> {
        let mut map = self.inner.lock().unwrap();
        if let Some(prev) = map.insert(hkey, entry.clone()) {
            *self.bytes.lock().unwrap() -= prev.encoded_len();
        }
        *self.bytes.lock().unwrap() += entry.encoded_len();
        Ok(())
    }
    async fn get(&self, hkey: u64) -> Result<Option<Entry>, StorageError> {
        Ok(self.inner.lock().unwrap().get(&hkey).cloned())
    }
    async fn delete(&mut self, hkey: u64) -> Result<bool, StorageError> {
        let mut map = self.inner.lock().unwrap();
        if let Some(prev) = map.remove(&hkey) {
            *self.bytes.lock().unwrap() -= prev.encoded_len();
            Ok(true)
        } else {
            Ok(false)
        }
    }
    async fn scan(&self, callback: ScanCallback<'_>) -> Result<(), StorageError> {
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
    ) -> Result<(), StorageError> {
        let re = regex::Regex::new(pattern).map_err(|e| StorageError::Regex(e.to_string()))?;
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
    async fn export(&self) -> Result<Vec<u8>, StorageError> {
        Ok(Vec::new())
    }
    async fn import(&mut self, _data: &[u8]) -> Result<(), StorageError> {
        Ok(())
    }
}

fn mock_factory() -> EngineFactory {
    Arc::new(|| Box::new(MockEngine::default()))
}

fn embedded_config() -> Config {
    let mut cfg = Config::default();
    cfg.mode = Mode::EmbeddedSolo;
    cfg.dmaps.insert(
        "cache".into(),
        DMapConfig {
            name: "cache".into(),
            ttl: Some(Duration::from_secs(60)),
            ..Default::default()
        },
    );
    cfg
}

#[tokio::test]
async fn embedded_round_trips_basic_ops() {
    let cfg = embedded_config();
    let node = Kamino::embedded_with_engine(cfg, mock_factory())
        .await
        .expect("Kamino::embedded_with_engine");

    let client = node.client();
    assert_eq!(client.partition_count(), 271);

    let cache = client
        .new_dmap("cache", DMapOptions::default())
        .await
        .expect("new_dmap");

    cache
        .put("user:1", b"alice", PutOptions::default())
        .await
        .expect("put");
    let r = cache.get("user:1").await.expect("get");
    assert_eq!(r.as_str().unwrap(), "alice");

    assert!(cache.delete("user:1").await.unwrap());

    // incr round-trip
    assert_eq!(cache.incr("count", 1).await.unwrap(), 1);
    assert_eq!(cache.incr("count", 5).await.unwrap(), 6);

    // expire
    cache
        .put("temp", b"x", PutOptions::default())
        .await
        .unwrap();
    cache
        .expire("temp", Duration::from_millis(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(matches!(
        cache.get("temp").await,
        Err(kamino::ClientError::KeyNotFound)
    ));

    node.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn embedded_locks_acquire_and_release() {
    let cfg = embedded_config();
    let node = Kamino::embedded_with_engine(cfg, mock_factory())
        .await
        .expect("Kamino::embedded_with_engine");
    let embedded = node.embedded_client();
    let dmap = embedded.get_or_create("cache", DMapOptions::default());

    let ctx = dmap
        .clone()
        .lock_with_timeout("resource", Duration::from_secs(5), Duration::from_secs(1))
        .await
        .expect("first acquire");
    // Second attempt with short deadline must fail.
    let err = dmap
        .clone()
        .lock_with_timeout(
            "resource",
            Duration::from_secs(5),
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, kamino::ClientError::LockNotAcquired));
    ctx.unlock().await.expect("unlock");
    // Re-acquire after release.
    let ctx2 = dmap
        .clone()
        .lock_with_timeout("resource", Duration::from_secs(5), Duration::from_secs(1))
        .await
        .expect("re-acquire");
    ctx2.unlock().await.unwrap();

    node.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn embedded_rejects_non_solo_mode() {
    let mut cfg = Config::default();
    cfg.mode = Mode::EmbeddedClustered;
    let err = Kamino::embedded_with_engine(cfg, mock_factory())
        .await
        .unwrap_err();
    assert!(matches!(err, kamino::KaminoError::WrongMode(_)));
}

#[tokio::test]
async fn scan_returns_seeded_keys() {
    let cfg = embedded_config();
    let node = Kamino::embedded_with_engine(cfg, mock_factory())
        .await
        .expect("Kamino::embedded_with_engine");
    let client = node.client();
    let cache = client
        .new_dmap("cache", DMapOptions::default())
        .await
        .unwrap();
    for i in 0..5 {
        cache
            .put(&format!("k{i}"), b"v", PutOptions::default())
            .await
            .unwrap();
    }
    let mut cursor = cache.scan(0, ScanOptions::default()).await.unwrap();
    let mut count = 0;
    while cursor.next().await.unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 5);
    node.shutdown().await.unwrap();
}
