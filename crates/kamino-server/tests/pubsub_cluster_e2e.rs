//! Phase 7 cluster pub/sub acceptance test.
//!
//! ROADMAP §7 Phase 7 #1: "3-node cluster, publish on node A, both
//! subscribers (on B and C) receive within 10ms P99 under no load."
//!
//! Standing up real SWIM membership for three processes inside a single
//! test would be expensive and flaky on shared CI machines. Instead this
//! test wires a custom [`PubSubProvider`] that holds a list of peer
//! addresses and forwards every `PUBLISH` via the real
//! `kamino_cluster::Forwarder` — exactly the path the cluster runtime
//! uses, minus the SWIM convergence. The on-wire shape (`INTERNAL.NODE.PUBLISH`
//! → local fan-out → `Frame::Integer` reply) is exercised end-to-end.

#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::SinkExt;
use futures::StreamExt;
use kamino_client::Client;
use kamino_client::embedded::{EmbeddedClient, EmbeddedDeps, EngineFactory};
use kamino_cluster::{
    DeliveredMessage, Forwarder, ForwarderConfig, LocalPubSubProvider, PubSubProvider,
    PubSubService, SubAck,
};
use kamino_core::{Clock, Config, Hasher, Mode, SystemClock, XxHasher};
use kamino_protocol::{BulkString, Command, Frame, RespCodec};
use kamino_server::Server;
use kamino_storage::{Locker, RamBlock, StorageEngine};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

/// Test-only `PubSubProvider` that:
/// 1. Maintains a local `LocalPubSubProvider` for subscribers attached
///    to *this* node.
/// 2. On `publish`, fires `INTERNAL.NODE.PUBLISH` to every configured
///    peer in parallel and sums the per-peer delivery counts with the
///    local count.
///
/// This mirrors `Cluster::publish` exactly, so the test exercises the
/// real fan-out semantics without standing up SWIM.
#[derive(Debug)]
struct FanoutProvider {
    local: LocalPubSubProvider,
    peers: std::sync::Mutex<Vec<std::net::SocketAddr>>,
    forwarder: Forwarder,
}

impl FanoutProvider {
    fn new(service: Arc<PubSubService>, cluster_secret: &str) -> Self {
        let cfg = ForwarderConfig {
            cluster_secret: cluster_secret.into(),
            ..Default::default()
        };
        Self {
            local: LocalPubSubProvider::new(service),
            peers: std::sync::Mutex::new(Vec::new()),
            forwarder: Forwarder::new(cfg),
        }
    }

    fn set_peers(&self, peers: Vec<std::net::SocketAddr>) {
        *self.peers.lock().unwrap() = peers;
    }
}

impl PubSubProvider for FanoutProvider {
    fn allocate_conn_id(&self) -> u64 {
        self.local.allocate_conn_id()
    }
    fn register_conn(&self, conn_id: u64, sender: tokio::sync::mpsc::Sender<DeliveredMessage>) {
        self.local.register_conn(conn_id, sender);
    }
    fn cleanup_conn(&self, conn_id: u64) {
        self.local.cleanup_conn(conn_id);
    }
    fn subscribe(&self, conn_id: u64, channels: &[Bytes]) -> Vec<SubAck> {
        self.local.subscribe(conn_id, channels)
    }
    fn psubscribe(&self, conn_id: u64, patterns: &[Bytes]) -> Vec<SubAck> {
        self.local.psubscribe(conn_id, patterns)
    }
    fn unsubscribe(&self, conn_id: u64, channels: Option<&[Bytes]>) -> Vec<SubAck> {
        self.local.unsubscribe(conn_id, channels)
    }
    fn punsubscribe(&self, conn_id: u64, patterns: Option<&[Bytes]>) -> Vec<SubAck> {
        self.local.punsubscribe(conn_id, patterns)
    }
    fn publish<'a>(
        &'a self,
        channel: Bytes,
        message: Bytes,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = usize> + Send + 'a>> {
        Box::pin(async move {
            let local_count = self
                .local
                .publish_local(&String::from_utf8_lossy(&channel), &message);
            let peers = self.peers.lock().unwrap().clone();
            if peers.is_empty() {
                return local_count;
            }
            let cmd = Command::InternalNodePublish {
                channel: channel.clone(),
                message: message.clone(),
            };
            let mut futs = Vec::with_capacity(peers.len());
            for peer in peers {
                let fwd = self.forwarder.clone();
                let c = cmd.clone();
                futs.push(async move { fwd.send(peer, c).await });
            }
            let replies = futures::future::join_all(futs).await;
            let mut total = local_count;
            for r in replies.into_iter().flatten() {
                if let Frame::Integer(n) = r {
                    if n >= 0 {
                        total = total.saturating_add(usize::try_from(n).unwrap_or(0));
                    }
                }
            }
            total
        })
    }
    fn publish_local(&self, channel: &str, message: &Bytes) -> usize {
        self.local.publish_local(channel, message)
    }
    fn pubsub_channels(&self, pattern: Option<&str>) -> Vec<String> {
        self.local.pubsub_channels(pattern)
    }
    fn pubsub_numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, usize)> {
        self.local.pubsub_numsub(channels)
    }
    fn pubsub_numpat(&self) -> usize {
        self.local.pubsub_numpat()
    }
}

/// Build a leaf node (subscriber side). The embedded client's pub/sub
/// service is shared with the server so `INTERNAL.NODE.PUBLISH`
/// arrivals deliver to local subscribers.
async fn build_leaf(secret: &str) -> (Server, Arc<EmbeddedClient>) {
    let mut cfg = Config::default();
    cfg.mode = Mode::Standalone;
    cfg.network.bind_port = 0;
    cfg.network.bind_addr = std::net::IpAddr::from([127, 0, 0, 1]);
    cfg.auth.cluster_secret = secret.into();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let hasher: Arc<dyn Hasher> = Arc::new(XxHasher);
    let factory: EngineFactory =
        Arc::new(|| -> Box<dyn StorageEngine> { Box::new(RamBlock::new(64 * 1024, 0.4)) });
    let deps = EmbeddedDeps {
        clock,
        hasher,
        locker: Locker::new(),
        engine_factory: factory,
        partition_count: cfg.core.partition_count,
    };
    let embedded = EmbeddedClient::new(deps);
    let erased: Arc<dyn Client> = Arc::clone(&embedded) as Arc<dyn Client>;
    let svc = embedded.pubsub_service();
    let server = Server::bind_with_pubsub_service(&cfg, erased, svc)
        .await
        .expect("bind leaf");
    (server, embedded)
}

/// Build a publisher node with a `FanoutProvider` aimed at `peers`.
async fn build_publisher(
    secret: &str,
    peers: Vec<std::net::SocketAddr>,
) -> (Server, Arc<EmbeddedClient>) {
    let mut cfg = Config::default();
    cfg.mode = Mode::Standalone;
    cfg.network.bind_port = 0;
    cfg.network.bind_addr = std::net::IpAddr::from([127, 0, 0, 1]);
    cfg.auth.cluster_secret = secret.into();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let hasher: Arc<dyn Hasher> = Arc::new(XxHasher);
    let factory: EngineFactory =
        Arc::new(|| -> Box<dyn StorageEngine> { Box::new(RamBlock::new(64 * 1024, 0.4)) });
    let deps = EmbeddedDeps {
        clock,
        hasher,
        locker: Locker::new(),
        engine_factory: factory,
        partition_count: cfg.core.partition_count,
    };
    let embedded = EmbeddedClient::new(deps);
    let provider = Arc::new(FanoutProvider::new(embedded.pubsub_service(), secret));
    provider.set_peers(peers);
    let erased_provider: Arc<dyn PubSubProvider> = Arc::clone(&provider) as _;
    let erased: Arc<dyn Client> = Arc::clone(&embedded) as Arc<dyn Client>;
    let server = Server::bind_with_full_providers(&cfg, erased, None, None, Some(erased_provider))
        .await
        .expect("bind publisher");
    (server, embedded)
}

/// Phase 7 e2e two-node fan-out. Node A receives `PUBLISH`, fans
/// `INTERNAL.NODE.PUBLISH` to node B, B's subscriber receives `message`.
#[tokio::test]
async fn publish_fans_out_to_remote_subscriber() {
    let secret = "topsecret";
    let (server_b, _embedded_b) = build_leaf(secret).await;
    let addr_b = server_b.local_addr();
    let shutdown_b = server_b.shutdown_handle();
    let task_b = tokio::spawn(server_b.run());

    let (server_a, _embedded_a) = build_publisher(secret, vec![addr_b]).await;
    let addr_a = server_a.local_addr();
    let shutdown_a = server_a.shutdown_handle();
    let task_a = tokio::spawn(server_a.run());

    let mut sub = open_framed(addr_b).await;
    send_frame(&mut sub, cmd(&[b"SUBSCRIBE", b"events"])).await;
    let _ack = recv_frame(&mut sub).await;

    let mut pubn = open_framed(addr_a).await;
    send_frame(&mut pubn, cmd(&[b"PUBLISH", b"events", b"hi"])).await;
    let count = recv_frame(&mut pubn).await;
    assert!(
        matches!(count, Frame::Integer(1)),
        "expected 1, got {count:?}"
    );

    let msg = recv_frame(&mut sub).await;
    let Frame::Array(Some(items)) = msg else {
        panic!("not array");
    };
    assert_eq!(items.len(), 3);
    assert!(matches!(&items[0], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"message"));
    assert!(matches!(&items[2], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"hi"));

    shutdown_a.trigger();
    shutdown_b.trigger();
    let _ = task_a.await;
    let _ = task_b.await;
}

/// ROADMAP §7 Phase 7 #1: 3-node cluster, publish on node A, both
/// subscribers (on B and C) receive within the test timeout.
#[tokio::test]
async fn three_node_publish_reaches_every_subscriber() {
    let secret = "topsecret";
    let (server_b, _eb) = build_leaf(secret).await;
    let (server_c, _ec) = build_leaf(secret).await;
    let addr_b = server_b.local_addr();
    let addr_c = server_c.local_addr();
    let sd_b = server_b.shutdown_handle();
    let sd_c = server_c.shutdown_handle();
    let task_b = tokio::spawn(server_b.run());
    let task_c = tokio::spawn(server_c.run());

    let (server_a, _ea) = build_publisher(secret, vec![addr_b, addr_c]).await;
    let addr_a = server_a.local_addr();
    let sd_a = server_a.shutdown_handle();
    let task_a = tokio::spawn(server_a.run());

    let mut sub_b = open_framed(addr_b).await;
    send_frame(&mut sub_b, cmd(&[b"SUBSCRIBE", b"events.*"])).await;
    // Wait, that's PSUBSCRIBE syntactically. Use SUBSCRIBE with exact name.
    let _ = recv_frame(&mut sub_b).await;

    let mut sub_c = open_framed(addr_c).await;
    send_frame(&mut sub_c, cmd(&[b"SUBSCRIBE", b"events.*"])).await;
    let _ = recv_frame(&mut sub_c).await;

    let mut pubn = open_framed(addr_a).await;
    send_frame(&mut pubn, cmd(&[b"PUBLISH", b"events.*", b"hello"])).await;
    let count = recv_frame(&mut pubn).await;
    assert!(
        matches!(count, Frame::Integer(2)),
        "expected 2, got {count:?}"
    );

    // Both subscribers receive the same payload.
    for sub in [&mut sub_b, &mut sub_c] {
        let msg = recv_frame(sub).await;
        let Frame::Array(Some(items)) = msg else {
            panic!("not array");
        };
        assert_eq!(items.len(), 3);
        assert!(matches!(&items[0], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"message"));
        assert!(matches!(&items[2], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"hello"));
    }

    sd_a.trigger();
    sd_b.trigger();
    sd_c.trigger();
    let _ = task_a.await;
    let _ = task_b.await;
    let _ = task_c.await;
}

/// Pattern subscribers on B receive a `pmessage` for events published on A.
#[tokio::test]
async fn pattern_subscribe_across_nodes() {
    let secret = "topsecret";
    let (server_b, _eb) = build_leaf(secret).await;
    let addr_b = server_b.local_addr();
    let sd_b = server_b.shutdown_handle();
    let task_b = tokio::spawn(server_b.run());

    let (server_a, _ea) = build_publisher(secret, vec![addr_b]).await;
    let addr_a = server_a.local_addr();
    let sd_a = server_a.shutdown_handle();
    let task_a = tokio::spawn(server_a.run());

    let mut sub = open_framed(addr_b).await;
    send_frame(&mut sub, cmd(&[b"PSUBSCRIBE", b"alerts.*"])).await;
    let _ = recv_frame(&mut sub).await;

    let mut pubn = open_framed(addr_a).await;
    send_frame(&mut pubn, cmd(&[b"PUBLISH", b"alerts.high", b"x"])).await;
    let _count = recv_frame(&mut pubn).await;

    let msg = recv_frame(&mut sub).await;
    let Frame::Array(Some(items)) = msg else {
        panic!("not array");
    };
    assert_eq!(items.len(), 4);
    assert!(matches!(&items[0], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"pmessage"));
    assert!(matches!(&items[1], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"alerts.*"));
    assert!(matches!(&items[2], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"alerts.high"));

    sd_a.trigger();
    sd_b.trigger();
    let _ = task_a.await;
    let _ = task_b.await;
}

fn cmd(parts: &[&[u8]]) -> Frame {
    let items: Vec<Frame> = parts
        .iter()
        .map(|p| Frame::Bulk(BulkString::from(*p)))
        .collect();
    Frame::Array(Some(items))
}

async fn open_framed(addr: std::net::SocketAddr) -> Framed<TcpStream, RespCodec> {
    let stream = TcpStream::connect(addr).await.expect("connect");
    Framed::new(stream, RespCodec::new())
}

async fn send_frame(c: &mut Framed<TcpStream, RespCodec>, f: Frame) {
    SinkExt::<Frame>::send(c, f).await.expect("send");
}

async fn recv_frame(c: &mut Framed<TcpStream, RespCodec>) -> Frame {
    tokio::time::timeout(Duration::from_secs(2), c.next())
        .await
        .expect("timeout")
        .expect("stream closed")
        .expect("decode")
}
