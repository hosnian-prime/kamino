//! `LockContext` — handle returned by `DMap::lock` / `lock_with_timeout`.

use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::dmap::DMap;
use crate::error::Result;

/// Opaque handle to a held distributed lock.
///
/// Holds a `Weak` reference to its DMap so dropping the lock context after the
/// DMap is destroyed is a no-op rather than a use-after-free.
#[derive(Debug)]
pub struct LockContext {
    token: Vec<u8>,
    dmap: Weak<dyn DMap>,
    name: String,
    key: String,
}

impl LockContext {
    /// Internal constructor — only `EmbeddedDMap` calls this.
    pub(crate) fn new(token: Vec<u8>, dmap: &Arc<dyn DMap>, name: String, key: String) -> Self {
        Self {
            token,
            dmap: Arc::downgrade(dmap),
            name,
            key,
        }
    }

    /// 16-byte opaque token. Required to unlock; never serialise it to logs.
    #[must_use]
    pub fn token(&self) -> &[u8] {
        &self.token
    }

    /// DMap name the lock was acquired on.
    #[must_use]
    pub fn dmap_name(&self) -> &str {
        &self.name
    }

    /// Locked key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Release the lock. Returns `Ok(())` even if the lease had already
    /// expired (idempotent unlock — see `docs/10-distributed-locking.md`).
    pub async fn unlock(&self) -> Result<()> {
        let Some(dmap) = self.dmap.upgrade() else {
            return Ok(());
        };
        dmap.unlock_internal(&self.key, &self.token).await
    }

    /// Extend the lease on this lock to `duration` from now.
    pub async fn lease(&self, duration: Duration) -> Result<()> {
        let Some(dmap) = self.dmap.upgrade() else {
            return Err(crate::error::Error::NoSuchLock);
        };
        dmap.lease_internal(&self.key, &self.token, duration).await
    }
}
