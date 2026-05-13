//! DNS-based discovery.
//!
//! Issues a fresh A/AAAA lookup through the system resolver (tokio's
//! `lookup_host`, which delegates to `getaddrinfo`) on each `discover()`
//! call so new pods are picked up as soon as DNS publishes them.

use std::net::SocketAddr;

use async_trait::async_trait;

use crate::error::{ClusterError, ClusterResult};

use super::DiscoveryPlugin;

/// DNS discovery: a single hostname + port. Tokio's `lookup_host` is used to
/// avoid pulling a heavy resolver crate (which inflates MSRV); SRV-record
/// support is out of scope until we have a documented need for it.
#[derive(Debug)]
pub struct DnsDiscovery {
    hostname: String,
    port: u16,
}

impl DnsDiscovery {
    /// Build a fresh DNS discovery instance. `hostname` may be a single name
    /// (e.g. `kamino-headless.default.svc.cluster.local`); IPv4 and IPv6
    /// results are both returned with the same port.
    #[must_use]
    pub fn new(hostname: impl Into<String>, port: u16) -> Self {
        Self {
            hostname: hostname.into(),
            port,
        }
    }
}

#[async_trait]
impl DiscoveryPlugin for DnsDiscovery {
    async fn discover(&self) -> ClusterResult<Vec<SocketAddr>> {
        let host_port = format!("{}:{}", self.hostname, self.port);
        let iter = tokio::net::lookup_host(&host_port)
            .await
            .map_err(|e| ClusterError::Discovery(format!("dns lookup '{host_port}': {e}")))?;
        Ok(iter.collect())
    }

    fn name(&self) -> &'static str {
        "dns"
    }
}
