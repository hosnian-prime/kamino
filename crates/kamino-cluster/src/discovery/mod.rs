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
///
/// Lifecycle (per `docs/03-cluster-management.md` "Node Discovery"):
///
/// ```text
/// init   → register → (discover loop) → deregister → shutdown
/// ```
///
/// `init` is called once after construction; for plugins that need to set
/// up sockets / clients this is where it happens. `register` advertises the
/// local node in an external registry (Consul, Kubernetes Endpoints, …) —
/// static and DNS plugins implement it as a no-op. `discover` is called per
/// join attempt. `deregister` undoes `register` on graceful leave;
/// `shutdown` releases plugin-owned resources.
#[async_trait]
pub trait DiscoveryPlugin: Send + Sync {
    /// Initialise the plugin. Called once after construction.
    async fn init(&self) -> ClusterResult<()> {
        Ok(())
    }

    /// Register this node in the external discovery system. No-op for static
    /// and DNS plugins; meaningful for Consul / Kubernetes / cloud APIs.
    async fn register(&self) -> ClusterResult<()> {
        Ok(())
    }

    /// Deregister this node from the external discovery system. Called from
    /// `Cluster::shutdown` before the SWIM tasks are cancelled.
    async fn deregister(&self) -> ClusterResult<()> {
        Ok(())
    }

    /// Return the current set of peer addresses. Called once per join
    /// attempt — implementations should treat this as a fresh lookup.
    async fn discover(&self) -> ClusterResult<Vec<SocketAddr>>;

    /// Tear down plugin-owned resources (HTTP clients, watchers, ...).
    async fn shutdown(&self) -> ClusterResult<()> {
        Ok(())
    }

    /// Human-readable name for logs / metrics.
    fn name(&self) -> &'static str;
}
