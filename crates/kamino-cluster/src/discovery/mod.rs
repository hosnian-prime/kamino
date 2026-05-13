//! Discovery plugins.
//!
//! A `DiscoveryPlugin` returns the addresses of peers a fresh node should
//! contact during join. The trait is async so plugins that perform network
//! I/O (Kubernetes Endpoints API, Consul, ...) fit naturally.
//!
//! See `docs/03-cluster-management.md` "Node Discovery".

use std::net::SocketAddr;

use async_trait::async_trait;

use crate::error::ClusterResult;

mod r#static;
pub use r#static::StaticDiscovery;

#[cfg(feature = "discovery-dns")]
mod dns;
#[cfg(feature = "discovery-dns")]
pub use dns::DnsDiscovery;

/// Pluggable peer discovery.
#[async_trait]
pub trait DiscoveryPlugin: Send + Sync {
    /// Initialise. Called once after construction; may register the local
    /// node in an external system.
    async fn init(&self) -> ClusterResult<()> {
        Ok(())
    }

    /// Return the current set of peer addresses. Called once per join
    /// attempt — implementations should treat this as a fresh lookup.
    async fn discover(&self) -> ClusterResult<Vec<SocketAddr>>;

    /// Deregister and tear down. Called during graceful leave.
    async fn shutdown(&self) -> ClusterResult<()> {
        Ok(())
    }

    /// Human-readable name for logs / metrics.
    fn name(&self) -> &'static str;
}
