//! `ScanCursor` trait and the in-memory implementation backed by a fragment scan.

use std::sync::Arc;

use async_trait::async_trait;

use crate::dmap::DMap;
use crate::error::Result;

/// Options applied to a single `scan` call.
#[derive(Debug, Default, Clone)]
pub struct ScanOptions {
    /// Approximate number of keys per round-trip. Ignored by the in-memory
    /// cursor (it materialises a single snapshot up front).
    pub count: Option<usize>,
    /// Glob pattern matched against keys. Translated to a regex by the impl.
    pub match_pattern: Option<String>,
}

/// Cursor returned by [`crate::DMap::scan`].
#[async_trait]
pub trait ScanCursor: Send {
    /// Return the next `(key, value)` pair or `None` once the partition is
    /// exhausted.
    async fn next(&mut self) -> Result<Option<(String, Vec<u8>)>>;

    /// Release any server-side cursor state. The default impl is a no-op.
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// In-memory cursor: snapshots a fragment's matching keys and iterates them.
#[derive(Debug)]
pub struct BufferedCursor {
    buffer: std::vec::IntoIter<(String, Vec<u8>)>,
}

impl BufferedCursor {
    /// Build a cursor from a pre-collected vector. The caller is responsible
    /// for applying `count` if a paginated wire protocol ever needs it.
    #[must_use]
    pub fn new(items: Vec<(String, Vec<u8>)>) -> Self {
        Self {
            buffer: items.into_iter(),
        }
    }
}

#[async_trait]
impl ScanCursor for BufferedCursor {
    async fn next(&mut self) -> Result<Option<(String, Vec<u8>)>> {
        Ok(self.buffer.next())
    }
}

/// Cross-partition scan aggregator (Phase 4): walks every partition id
/// `0..partition_count`, draining each partition's cursor before moving on.
///
/// Per `docs/06-network-protocol.md` the protocol defines `DM.SCAN` as
/// **single-partition**; cross-partition iteration is the client's job.
/// This aggregator is the supported in-library implementation.
///
/// `count` and `match_pattern` are forwarded to each per-partition scan.
pub struct CrossPartitionScan {
    dmap: Arc<dyn DMap>,
    partition_count: u32,
    options: ScanOptions,
    next_partition: u32,
    current: Option<Box<dyn ScanCursor>>,
}

impl std::fmt::Debug for CrossPartitionScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossPartitionScan")
            .field("partition_count", &self.partition_count)
            .field("next_partition", &self.next_partition)
            .finish_non_exhaustive()
    }
}

impl CrossPartitionScan {
    /// Build a cursor that visits every partition in order.
    #[must_use]
    pub fn new(dmap: Arc<dyn DMap>, partition_count: u32, options: ScanOptions) -> Self {
        Self {
            dmap,
            partition_count,
            options,
            next_partition: 0,
            current: None,
        }
    }
}

#[async_trait]
impl ScanCursor for CrossPartitionScan {
    async fn next(&mut self) -> Result<Option<(String, Vec<u8>)>> {
        loop {
            if let Some(c) = self.current.as_mut() {
                if let Some(kv) = c.next().await? {
                    return Ok(Some(kv));
                }
                let mut prev = self.current.take().expect("just matched Some");
                // Best-effort close — ignore failure; cursor is being
                // dropped anyway.
                let _ = prev.close().await;
            }
            if self.next_partition >= self.partition_count {
                return Ok(None);
            }
            let part_id = self.next_partition;
            self.next_partition += 1;
            let cursor = self.dmap.scan(part_id, self.options.clone()).await?;
            self.current = Some(cursor);
        }
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(mut c) = self.current.take() {
            c.close().await?;
        }
        // Skip the rest — drained or not, the resource is the per-partition
        // cursor, and we've released the only one we held.
        self.next_partition = self.partition_count;
        Ok(())
    }
}

/// Translate a Redis-style glob (`*`, `?`, `[abc]`) into a regex anchored at
/// both ends.
#[must_use]
pub fn glob_to_regex(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 2);
    out.push('^');
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            '[' => {
                out.push('[');
                while let Some(&inner) = chars.peek() {
                    chars.next();
                    if inner == ']' {
                        out.push(']');
                        break;
                    }
                    out.push(inner);
                }
            }
            '.' | '+' | '(' | ')' | '|' | '^' | '$' | '{' | '}' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            other => out.push(other),
        }
    }
    out.push('$');
    out
}

#[cfg(test)]
#[allow(clippy::unnecessary_literal_bound)] // `fn name(&self) -> &str` matches the trait
mod tests {
    use super::*;
    use crate::error::{Error, Result};
    use crate::lock::LockContext;
    use crate::types::{GetResponse, PutOptions};
    use async_trait::async_trait;
    use std::sync::Mutex;
    use std::time::Duration;

    #[test]
    fn glob_translates_stars() {
        assert_eq!(glob_to_regex("user:*"), "^user:.*$");
        assert_eq!(glob_to_regex("a?c"), "^a.c$");
        assert_eq!(glob_to_regex("[ab]c"), "^[ab]c$");
    }

    #[test]
    fn glob_escapes_regex_metas() {
        let r = glob_to_regex("a.b");
        assert_eq!(r, "^a\\.b$");
    }

    #[tokio::test]
    async fn buffered_cursor_drains() {
        let mut c = BufferedCursor::new(vec![
            ("a".into(), b"1".to_vec()),
            ("b".into(), b"2".to_vec()),
        ]);
        assert_eq!(c.next().await.unwrap().unwrap().0, "a");
        assert_eq!(c.next().await.unwrap().unwrap().0, "b");
        assert!(c.next().await.unwrap().is_none());
    }

    type PartitionMap = std::collections::HashMap<u32, Vec<(String, Vec<u8>)>>;

    /// Stub DMap whose `scan` returns a small `BufferedCursor` keyed by
    /// partition id. Lets us prove `CrossPartitionScan` visits every
    /// partition once and aggregates the results in order.
    #[derive(Debug)]
    struct PartitionDMap {
        per_partition: Mutex<PartitionMap>,
    }

    #[async_trait]
    impl DMap for PartitionDMap {
        fn name(&self) -> &str {
            "p"
        }
        async fn put(&self, _k: &str, _v: &[u8], _o: PutOptions) -> Result<()> {
            unimplemented!()
        }
        async fn get(&self, _k: &str) -> Result<GetResponse> {
            Err(Error::KeyNotFound)
        }
        async fn delete(&self, _k: &str) -> Result<bool> {
            unimplemented!()
        }
        async fn incr(&self, _k: &str, _d: i64) -> Result<i64> {
            unimplemented!()
        }
        async fn decr(&self, _k: &str, _d: i64) -> Result<i64> {
            unimplemented!()
        }
        async fn incr_by_float(&self, _k: &str, _d: f64) -> Result<f64> {
            unimplemented!()
        }
        async fn get_put(&self, _k: &str, _v: &[u8]) -> Result<Option<GetResponse>> {
            unimplemented!()
        }
        async fn expire(&self, _k: &str, _d: Duration) -> Result<()> {
            unimplemented!()
        }
        async fn lock(self: Arc<Self>, _k: &str, _d: Duration) -> Result<LockContext> {
            unimplemented!()
        }
        async fn lock_with_timeout(
            self: Arc<Self>,
            _k: &str,
            _l: Duration,
            _d: Duration,
        ) -> Result<LockContext> {
            unimplemented!()
        }
        async fn scan(&self, p: u32, _o: ScanOptions) -> Result<Box<dyn ScanCursor>> {
            let items = self
                .per_partition
                .lock()
                .unwrap()
                .get(&p)
                .cloned()
                .unwrap_or_default();
            Ok(Box::new(BufferedCursor::new(items)))
        }
        async fn destroy(&self) -> Result<()> {
            unimplemented!()
        }
        async fn unlock_internal(&self, _k: &str, _t: &[u8]) -> Result<()> {
            unimplemented!()
        }
        async fn lease_internal(&self, _k: &str, _t: &[u8], _d: Duration) -> Result<()> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn cross_partition_scan_visits_every_partition_in_order() {
        let mut data: std::collections::HashMap<u32, Vec<(String, Vec<u8>)>> =
            std::collections::HashMap::new();
        data.insert(
            0,
            vec![("a".into(), b"1".to_vec()), ("b".into(), b"2".to_vec())],
        );
        // Partition 1 is empty — confirms the aggregator skips holes.
        data.insert(2, vec![("c".into(), b"3".to_vec())]);
        let dmap = Arc::new(PartitionDMap {
            per_partition: Mutex::new(data),
        });
        let mut scan = CrossPartitionScan::new(dmap, 3, ScanOptions::default());
        let mut collected = Vec::new();
        while let Some(kv) = scan.next().await.unwrap() {
            collected.push(kv);
        }
        let keys: Vec<_> = collected.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }
}
