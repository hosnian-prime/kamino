//! Phase 7 pub/sub end-to-end tests against the standalone RESP server.
//!
//! These bypass `RemoteClient` (which doesn't yet expose pub/sub) and
//! drive the RESP codec directly so the test asserts the wire-shape
//! invariants `docs/11-pubsub.md` makes (`message` / `pmessage` array
//! frames, ack triples, mode restrictions).

#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;
use std::time::Duration;

use futures::SinkExt;
use futures::StreamExt;
use kamino_client::Client;
use kamino_client::embedded::{EmbeddedClient, EmbeddedDeps, EngineFactory};
use kamino_core::{Clock, Config, Hasher, Mode, SystemClock, XxHasher};
use kamino_protocol::{BulkString, Frame, RespCodec};
use kamino_server::Server;
use kamino_storage::{Locker, RamBlock, StorageEngine};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

struct TestServer {
    server: Server,
    _embedded: Arc<EmbeddedClient>,
}

async fn start_server(mut config: Config) -> TestServer {
    config.mode = Mode::Standalone;
    config.network.bind_port = 0;
    config.network.bind_addr = std::net::IpAddr::from([127, 0, 0, 1]);

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let hasher: Arc<dyn Hasher> = Arc::new(XxHasher);
    let locker = Locker::new();
    let factory: EngineFactory =
        Arc::new(|| -> Box<dyn StorageEngine> { Box::new(RamBlock::new(64 * 1024, 0.4)) });
    let deps = EmbeddedDeps {
        clock,
        hasher,
        locker,
        engine_factory: factory,
        partition_count: config.core.partition_count,
    };
    let embedded = EmbeddedClient::new(deps);
    let erased: Arc<dyn Client> = Arc::clone(&embedded) as Arc<dyn Client>;
    let server = Server::bind_with_pubsub_service(&config, erased, embedded.pubsub_service())
        .await
        .expect("bind");
    TestServer {
        server,
        _embedded: embedded,
    }
}

fn cmd(parts: &[&[u8]]) -> Frame {
    let items: Vec<Frame> = parts
        .iter()
        .map(|p| Frame::Bulk(BulkString::from(*p)))
        .collect();
    Frame::Array(Some(items))
}

async fn open(addr: std::net::SocketAddr) -> Framed<TcpStream, RespCodec> {
    let stream = TcpStream::connect(addr).await.expect("connect");
    Framed::new(stream, RespCodec::new())
}

async fn send(c: &mut Framed<TcpStream, RespCodec>, f: Frame) {
    SinkExt::<Frame>::send(c, f).await.expect("send");
}

async fn recv(c: &mut Framed<TcpStream, RespCodec>) -> Frame {
    tokio::time::timeout(Duration::from_secs(2), c.next())
        .await
        .expect("timeout")
        .expect("stream closed")
        .expect("decode")
}

#[tokio::test]
async fn subscribe_publish_message_delivery() {
    let cfg = Config::default();
    let ts = start_server(cfg).await;
    let addr = ts.server.local_addr();
    let shutdown = ts.server.shutdown_handle();
    let task = tokio::spawn(ts.server.run());

    let mut sub = open(addr).await;
    let mut pubn = open(addr).await;

    // SUBSCRIBE events
    send(&mut sub, cmd(&[b"SUBSCRIBE", b"events"])).await;
    let ack = recv(&mut sub).await;
    let Frame::Array(Some(items)) = ack else {
        panic!("ack not array");
    };
    assert_eq!(items.len(), 3);
    assert!(matches!(&items[0], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"subscribe"));
    assert!(matches!(&items[1], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"events"));
    assert!(matches!(items[2], Frame::Integer(1)));

    // PUBLISH events "hello"
    send(&mut pubn, cmd(&[b"PUBLISH", b"events", b"hello"])).await;
    let count = recv(&mut pubn).await;
    assert!(matches!(count, Frame::Integer(1)));

    // Subscriber gets a `message` frame.
    let msg = recv(&mut sub).await;
    let Frame::Array(Some(items)) = msg else {
        panic!("not array");
    };
    assert_eq!(items.len(), 3);
    assert!(matches!(&items[0], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"message"));
    assert!(matches!(&items[1], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"events"));
    assert!(matches!(&items[2], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"hello"));

    shutdown.trigger();
    let _ = task.await;
}

#[tokio::test]
async fn psubscribe_pattern_match_delivers_pmessage() {
    let cfg = Config::default();
    let ts = start_server(cfg).await;
    let addr = ts.server.local_addr();
    let shutdown = ts.server.shutdown_handle();
    let task = tokio::spawn(ts.server.run());

    let mut sub = open(addr).await;
    let mut pubn = open(addr).await;

    send(&mut sub, cmd(&[b"PSUBSCRIBE", b"events.*"])).await;
    let _ack = recv(&mut sub).await;
    send(&mut pubn, cmd(&[b"PUBLISH", b"events.created", b"X"])).await;
    let count = recv(&mut pubn).await;
    assert!(matches!(count, Frame::Integer(1)));
    let msg = recv(&mut sub).await;
    let Frame::Array(Some(items)) = msg else {
        panic!("not array");
    };
    assert_eq!(items.len(), 4);
    assert!(matches!(&items[0], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"pmessage"));
    assert!(matches!(&items[1], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"events.*"));
    assert!(matches!(&items[2], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"events.created"));
    assert!(matches!(&items[3], Frame::Bulk(BulkString(Some(b))) if &b[..] == b"X"));

    shutdown.trigger();
    let _ = task.await;
}

#[tokio::test]
async fn subscribed_connection_rejects_dm_commands() {
    let cfg = Config::default();
    let ts = start_server(cfg).await;
    let addr = ts.server.local_addr();
    let shutdown = ts.server.shutdown_handle();
    let task = tokio::spawn(ts.server.run());

    let mut sub = open(addr).await;
    send(&mut sub, cmd(&[b"SUBSCRIBE", b"events"])).await;
    let _ack = recv(&mut sub).await;
    send(&mut sub, cmd(&[b"DM.GET", b"dm", b"k"])).await;
    let resp = recv(&mut sub).await;
    let Frame::Error(m) = resp else {
        panic!("expected error");
    };
    assert!(m.contains("only (P|S)SUBSCRIBE"));

    shutdown.trigger();
    let _ = task.await;
}

#[tokio::test]
async fn pubsub_channels_lists_active() {
    let cfg = Config::default();
    let ts = start_server(cfg).await;
    let addr = ts.server.local_addr();
    let shutdown = ts.server.shutdown_handle();
    let task = tokio::spawn(ts.server.run());

    let mut sub = open(addr).await;
    send(&mut sub, cmd(&[b"SUBSCRIBE", b"a", b"b", b"c"])).await;
    // Drain the three acks.
    let _ = recv(&mut sub).await;
    let _ = recv(&mut sub).await;
    let _ = recv(&mut sub).await;

    let mut inspector = open(addr).await;
    send(&mut inspector, cmd(&[b"PUBSUB", b"CHANNELS"])).await;
    let resp = recv(&mut inspector).await;
    let Frame::Array(Some(items)) = resp else {
        panic!("not array");
    };
    let count = items
        .into_iter()
        .filter(|f| matches!(f, Frame::Bulk(BulkString(Some(_)))))
        .count();
    assert_eq!(count, 3);

    shutdown.trigger();
    let _ = task.await;
}

#[tokio::test]
async fn unsubscribe_emits_total_zero() {
    let cfg = Config::default();
    let ts = start_server(cfg).await;
    let addr = ts.server.local_addr();
    let shutdown = ts.server.shutdown_handle();
    let task = tokio::spawn(ts.server.run());

    let mut sub = open(addr).await;
    send(&mut sub, cmd(&[b"SUBSCRIBE", b"x"])).await;
    let _ack = recv(&mut sub).await;
    send(&mut sub, cmd(&[b"UNSUBSCRIBE", b"x"])).await;
    let ack = recv(&mut sub).await;
    let Frame::Array(Some(items)) = ack else {
        panic!("not array");
    };
    assert!(matches!(items[2], Frame::Integer(0)));

    shutdown.trigger();
    let _ = task.await;
}
