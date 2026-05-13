//! End-to-end test for the `CLUSTER.MEMBERS` command path.
//!
//! Spins up `kamino-server` via [`Server::bind_with_cluster`] backed by a
//! freshly-assembled single-node [`Cluster`], then sends the raw
//! `*1\r\n$15\r\nCLUSTER.MEMBERS\r\n` RESP request over TCP and asserts
//! that the server returns a single-row array describing the local node
//! with `is_coordinator = "1"`.
//!
//! The test deliberately uses raw RESP framing instead of `RemoteClient`
//! because Phase 2's client doesn't yet expose a `CLUSTER.MEMBERS` helper
//! (that's Phase 4 surface). Using the wire directly verifies the dispatch
//! path end-to-end without depending on client-side conveniences.

#![allow(clippy::field_reassign_with_default)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use kamino_client::embedded::{EmbeddedClient, EmbeddedDeps, EngineFactory};
use kamino_client::{Client, RemoteClient};
use kamino_cluster::{Cluster, ClusterDeps, StaticDiscovery, UdpTransport};
use kamino_core::clock::SystemClock;
use kamino_core::ids::MemberId;
use kamino_core::member::Member;
use kamino_core::{Clock, Config, Hasher, Mode, XxHasher};
use kamino_server::Server;
use kamino_storage::{Locker, RamBlock, StorageEngine};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn build_single_node_cluster() -> Arc<Cluster> {
    let transport = Arc::new(
        UdpTransport::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .expect("bind udp"),
    );
    let local_disc = {
        use kamino_cluster::Transport as _;
        transport.local_addr().expect("disc local_addr")
    };
    let discovery = Arc::new(StaticDiscovery::from_addrs(Vec::new()));
    let member = Member::new(
        MemberId::new_random(),
        "local",
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3320),
        local_disc,
        // Birthdate doesn't matter for a 1-node cluster; coordinator is us.
        1,
    );
    let mut config = Config::default();
    // Keep timings short so the (best-effort) join inside bootstrap doesn't
    // delay the test.
    config.discovery.peers = Vec::new();
    config.discovery.max_join_attempts = 1;
    config.discovery.join_retry_interval = Duration::from_millis(10);
    config.discovery.bootstrap_timeout = Duration::from_millis(200);

    let deps = ClusterDeps {
        config,
        transport,
        discovery,
        clock: Arc::new(SystemClock),
        local: member,
    };
    Cluster::bootstrap(deps).await.expect("cluster bootstrap")
}

async fn build_server(cluster: Arc<Cluster>) -> (Server, Arc<EmbeddedClient>) {
    let mut config = Config::default();
    config.mode = Mode::Standalone;
    config.network.bind_port = 0;
    config.network.bind_addr = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let hasher: Arc<dyn Hasher> = Arc::new(XxHasher);
    let locker = Locker::new();
    let table_size = usize::try_from(config.storage.table_size.0).unwrap_or(usize::MAX);
    let max_garbage_ratio = config.storage.max_garbage_ratio;
    let factory: EngineFactory = Arc::new(move || -> Box<dyn StorageEngine> {
        Box::new(RamBlock::new(table_size, max_garbage_ratio))
    });
    let deps = EmbeddedDeps {
        clock,
        hasher,
        locker,
        engine_factory: factory,
        partition_count: config.core.partition_count,
    };
    let embedded = EmbeddedClient::new(deps);
    let erased: Arc<dyn Client> = Arc::clone(&embedded) as Arc<dyn Client>;
    let server = Server::bind_with_cluster(&config, erased, cluster)
        .await
        .expect("bind_with_cluster");
    (server, embedded)
}

/// Send the literal RESP frame `*1\r\n$15\r\nCLUSTER.MEMBERS\r\n` after a
/// `HELLO 2` handshake. Returns the body bytes the server replies with.
async fn send_cluster_members(addr: SocketAddr) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    // Authenticate-less HELLO 2 to flip the connection past the pre-auth gate.
    stream
        .write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n")
        .await
        .expect("hello write");
    let mut buf = vec![0u8; 4096];
    // Read whatever the server replies for HELLO. We don't need to parse it,
    // only consume it so the next read sees only the CLUSTER.MEMBERS reply.
    let n = stream.read(&mut buf).await.expect("hello read");
    assert!(n > 0, "server did not reply to HELLO");

    stream
        .write_all(b"*1\r\n$15\r\nCLUSTER.MEMBERS\r\n")
        .await
        .expect("cluster.members write");
    let mut reply = Vec::new();
    let n = stream.read(&mut buf).await.expect("cluster.members read");
    reply.extend_from_slice(&buf[..n]);
    reply
}

#[tokio::test]
async fn cluster_members_returns_local_node() {
    let cluster = build_single_node_cluster().await;
    let (server, _embedded) = build_server(Arc::clone(&cluster)).await;
    let addr = server.local_addr();
    let shutdown = server.shutdown_handle();
    let server_task = tokio::spawn(server.run());

    let reply = send_cluster_members(addr).await;
    let reply_str = String::from_utf8_lossy(&reply);

    // Top-level: outer array of length 1.
    assert!(
        reply_str.starts_with("*1\r\n"),
        "expected outer *1 array, got {reply_str:?}"
    );
    // Inner row is itself a 6-element array per `cluster_members` handler.
    assert!(
        reply_str.contains("*6\r\n"),
        "expected inner *6 row, got {reply_str:?}"
    );
    // Last element of the row is `is_coordinator`, the only single-node case
    // must be "1".
    assert!(
        reply_str.ends_with("$1\r\n1\r\n"),
        "expected last bulk to be \"1\", got {reply_str:?}"
    );

    shutdown.trigger();
    let _ = server_task.await;
    let _ = cluster.shutdown().await;
}

#[tokio::test]
async fn cluster_members_via_remote_client_round_trip() {
    // Smoke: a `RemoteClient` can connect, send PING, and the server stays
    // up while the cluster runtime is attached. Asserts no panic / hang.
    let cluster = build_single_node_cluster().await;
    let (server, _embedded) = build_server(Arc::clone(&cluster)).await;
    let addr = server.local_addr();
    let shutdown = server.shutdown_handle();
    let server_task = tokio::spawn(server.run());

    let client = RemoteClient::connect(&format!("{addr}"), None)
        .await
        .expect("connect");
    client.ping_once(Some(b"hi")).await.expect("ping");
    let _ = client.close().await;

    shutdown.trigger();
    let _ = server_task.await;
    let _ = cluster.shutdown().await;
}
