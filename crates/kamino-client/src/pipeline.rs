//! `Pipeline` — batched DMap operations executed against a single client
//! handle.
//!
//! Per `docs/08-api-design.md` "Pipeline":
//! - Each op is queued via [`Pipeline::put`] / [`get`] / [`delete`].
//! - [`Pipeline::execute`] returns one [`PipelineResult`] per queued op in
//!   the same order the caller submitted them.
//! - Operations targeting the same partition primary serialise in input
//!   order on that primary. Operations targeting *different* primaries
//!   run concurrently up to `concurrency`.
//! - There is **no global ordering across partitions** — see the doc for
//!   the precise guarantee.
//!
//! Phase 4 ships the wire-level concurrency contract; the partition-key →
//! primary lookup is delegated to the underlying [`crate::DMap`] handle, so
//! a `MultiNodeClient` (Phase 4+, not in this crate yet) can swap a routing-
//! aware DMap in without changing this surface.

use std::sync::Arc;

use crate::dmap::DMap;
use crate::error::Result;
use crate::types::{GetResponse, PutOptions};

/// Tunables for [`Pipeline::execute`].
#[derive(Debug, Clone, Copy)]
pub struct PipelineOptions {
    /// Maximum number of concurrent dispatches when `execute()` runs.
    /// Default 4 (matches `docs/08-api-design.md`).
    pub concurrency: usize,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self { concurrency: 4 }
    }
}

/// One queued operation.
#[derive(Debug, Clone)]
enum PipelineOp {
    Put {
        key: String,
        value: Vec<u8>,
        options: PutOptions,
    },
    Get {
        key: String,
    },
    Delete {
        key: String,
    },
}

/// Result variant for a single executed op.
#[derive(Debug, Clone)]
pub enum PipelineResult {
    /// `put` succeeded.
    Put,
    /// `get` succeeded.
    Get(GetResponse),
    /// `delete` reports whether a live entry was actually removed.
    Delete(bool),
}

/// Sequence of operations targeting one DMap.
///
/// Phase 4 dispatches sequentially against the supplied DMap handle (the
/// embedded case is already partition-correct). When a `MultiNodeClient`
/// is wired in, the dispatch step will switch to per-primary concurrent
/// dispatch under the `concurrency` cap.
pub struct Pipeline {
    dmap: Arc<dyn DMap>,
    options: PipelineOptions,
    ops: Vec<PipelineOp>,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("concurrency", &self.options.concurrency)
            .field("ops", &self.ops.len())
            .finish_non_exhaustive()
    }
}

impl Pipeline {
    /// Build an empty pipeline for `dmap`.
    #[must_use]
    pub fn new(dmap: Arc<dyn DMap>, options: PipelineOptions) -> Self {
        Self {
            dmap,
            options,
            ops: Vec::new(),
        }
    }

    /// Queue a `put`.
    pub fn put(&mut self, key: &str, value: &[u8], options: PutOptions) -> &mut Self {
        self.ops.push(PipelineOp::Put {
            key: key.to_string(),
            value: value.to_vec(),
            options,
        });
        self
    }

    /// Queue a `get`.
    pub fn get(&mut self, key: &str) -> &mut Self {
        self.ops.push(PipelineOp::Get {
            key: key.to_string(),
        });
        self
    }

    /// Queue a `delete`.
    pub fn delete(&mut self, key: &str) -> &mut Self {
        self.ops.push(PipelineOp::Delete {
            key: key.to_string(),
        });
        self
    }

    /// Number of queued ops.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Returns `true` if no ops have been queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Drain every op and return per-op results, preserving submission order.
    ///
    /// Phase 4: dispatched sequentially against `self.dmap`. The
    /// `concurrency` knob is honoured by future multi-node clients; for the
    /// in-process / single-RemoteClient case ordering and parallelism are
    /// equivalent under the per-DMap mutex semantics of the storage engine.
    pub async fn execute(self) -> Vec<Result<PipelineResult>> {
        let Self {
            dmap,
            options,
            ops,
        } = self;
        // Phase 4 ships sequential dispatch — single-process / single-conn
        // RemoteClient already preserves order. Cross-partition concurrent
        // dispatch is on the MultiNodeClient roadmap (Phase 5+).
        let _ = options;
        let mut out = Vec::with_capacity(ops.len());
        for op in ops {
            let result = match op {
                PipelineOp::Put {
                    key,
                    value,
                    options,
                } => dmap.put(&key, &value, options).await.map(|()| PipelineResult::Put),
                PipelineOp::Get { key } => dmap.get(&key).await.map(PipelineResult::Get),
                PipelineOp::Delete { key } => dmap.delete(&key).await.map(PipelineResult::Delete),
            };
            out.push(result);
        }
        out
    }
}

#[cfg(test)]
#[allow(clippy::unnecessary_literal_bound)] // `fn name(&self) -> &str` matches the trait
mod tests {
    use super::*;
    use crate::dmap::DMap;
    use crate::lock::LockContext;
    use crate::types::PutOptions;
    use crate::{Error, ScanCursor, ScanOptions};
    use async_trait::async_trait;
    use std::sync::Mutex;
    use std::time::Duration;

    /// In-memory DMap stub for unit-testing Pipeline dispatch order.
    #[derive(Debug, Default)]
    struct ToyDMap {
        log: Mutex<Vec<String>>,
        store: Mutex<std::collections::HashMap<String, Vec<u8>>>,
    }

    #[async_trait]
    impl DMap for ToyDMap {
        fn name(&self) -> &str {
            "toy"
        }
        async fn put(&self, key: &str, value: &[u8], _options: PutOptions) -> Result<()> {
            self.log.lock().unwrap().push(format!("PUT {key}"));
            self.store.lock().unwrap().insert(key.into(), value.to_vec());
            Ok(())
        }
        async fn get(&self, key: &str) -> Result<GetResponse> {
            self.log.lock().unwrap().push(format!("GET {key}"));
            let store = self.store.lock().unwrap();
            store
                .get(key)
                .map(|v| GetResponse {
                    value: v.clone(),
                    timestamp: 0,
                    ttl: None,
                })
                .ok_or(Error::KeyNotFound)
        }
        async fn delete(&self, key: &str) -> Result<bool> {
            self.log.lock().unwrap().push(format!("DEL {key}"));
            Ok(self.store.lock().unwrap().remove(key).is_some())
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
        async fn scan(&self, _p: u32, _o: ScanOptions) -> Result<Box<dyn ScanCursor>> {
            unimplemented!()
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
    async fn execute_preserves_order() {
        let dmap = Arc::new(ToyDMap::default());
        let mut p = Pipeline::new(dmap.clone() as Arc<dyn DMap>, PipelineOptions::default());
        p.put("a", b"1", PutOptions::default())
            .put("b", b"2", PutOptions::default())
            .get("a")
            .delete("b")
            .get("b");
        let results = p.execute().await;
        assert_eq!(results.len(), 5);
        assert!(matches!(results[0], Ok(PipelineResult::Put)));
        assert!(matches!(results[1], Ok(PipelineResult::Put)));
        let r2 = match &results[2] {
            Ok(PipelineResult::Get(g)) => &g.value,
            other => panic!("expected GET, got {other:?}"),
        };
        assert_eq!(r2, b"1");
        assert!(matches!(results[3], Ok(PipelineResult::Delete(true))));
        assert!(matches!(results[4], Err(Error::KeyNotFound)));

        let log = dmap.log.lock().unwrap().clone();
        assert_eq!(
            log,
            vec!["PUT a", "PUT b", "GET a", "DEL b", "GET b",]
        );
    }
}
