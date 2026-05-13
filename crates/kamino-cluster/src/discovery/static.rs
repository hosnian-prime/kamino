//! Static peer list.

use std::net::SocketAddr;

use async_trait::async_trait;

use crate::error::{ClusterError, ClusterResult};

use super::DiscoveryPlugin;

/// Static peer list from `discovery.peers`. Strings are parsed once at
/// construction so misconfiguration is caught at startup.
#[derive(Debug)]
pub struct StaticDiscovery {
    peers: Vec<SocketAddr>,
}

impl StaticDiscovery {
    /// Parse a TOML-supplied list of `host:port` strings.
    pub fn new(peers: &[String]) -> ClusterResult<Self> {
        let mut parsed = Vec::with_capacity(peers.len());
        for raw in peers {
            let addr = raw.parse::<SocketAddr>().map_err(|e| {
                ClusterError::Config(format!("invalid peer '{raw}': {e}"))
            })?;
            parsed.push(addr);
        }
        Ok(Self { peers: parsed })
    }

    /// Build directly from already-parsed addresses (tests).
    #[must_use]
    pub fn from_addrs(peers: Vec<SocketAddr>) -> Self {
        Self { peers }
    }
}

#[async_trait]
impl DiscoveryPlugin for StaticDiscovery {
    async fn discover(&self) -> ClusterResult<Vec<SocketAddr>> {
        Ok(self.peers.clone())
    }

    fn name(&self) -> &'static str {
        "static"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parses_and_returns_peers() {
        let plugin = StaticDiscovery::new(&[
            "127.0.0.1:3322".into(),
            "10.0.0.1:3322".into(),
        ])
        .unwrap();
        let peers = plugin.discover().await.unwrap();
        assert_eq!(peers.len(), 2);
    }

    #[test]
    fn rejects_bad_peer() {
        let err = StaticDiscovery::new(&["not-a-socket-addr".into()]).unwrap_err();
        assert!(matches!(err, ClusterError::Config(_)));
    }
}
