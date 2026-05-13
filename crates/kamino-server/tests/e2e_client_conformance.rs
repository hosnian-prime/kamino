//! End-to-end conformance test: spin up `kamino-server` on a random port and
//! exercise the same operations Phase 1's `embedded_smoke.rs` covers, via
//! `RemoteClient`.
//!
//! NOTE: every test in this file is `#[ignore]`-gated. The orchestrator
//! un-ignores them after merging Phase 2A (the codec impl) — until then the
//! `RespCodec` encode/decode bodies panic with `unimplemented!()`. They are
//! kept compiled so the surface drift gets caught immediately.

#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;
use std::time::Duration;

use kamino_client::embedded::{EmbeddedClient, EmbeddedDeps, EngineFactory};
use kamino_client::{Client, DMapOptions, PutOptions, RemoteClient, StatsOptions};
use kamino_core::{Clock, Config, Hasher, Mode, SystemClock, XxHasher};
use kamino_server::Server;
use kamino_storage::{Locker, RamBlock, StorageEngine};

struct TestServer {
    server: Server,
    /// Held to keep the eviction-less embedded client alive for the duration
    /// of the test.
    _embedded: Arc<EmbeddedClient>,
}

async fn start_server(mut config: Config) -> TestServer {
    config.mode = Mode::Standalone;
    config.network.bind_port = 0;
    config.network.bind_addr = std::net::IpAddr::from([127, 0, 0, 1]);

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
    let server = Server::bind(&config, erased).await.expect("bind");
    TestServer {
        server,
        _embedded: embedded,
    }
}

#[tokio::test]
#[ignore = "enabled after merge with Phase 2A (codec impl)"]
async fn ping_round_trips() {
    let cfg = Config::default();
    let TestServer { server, _embedded } = start_server(cfg).await;
    let addr = server.local_addr();
    let shutdown = server.shutdown_handle();
    let server_task = tokio::spawn(server.run());

    let client = RemoteClient::connect(&format!("{addr}"), None)
        .await
        .expect("connect");
    client.ping(&format!("{addr}")).await.expect("ping");
    let _ = client.close().await;

    shutdown.trigger();
    let _ = server_task.await;
}

#[tokio::test]
#[ignore = "enabled after merge with Phase 2A (codec impl)"]
async fn put_get_delete_round_trip() {
    let cfg = Config::default();
    let TestServer { server, _embedded } = start_server(cfg).await;
    let addr = server.local_addr();
    let shutdown = server.shutdown_handle();
    let server_task = tokio::spawn(server.run());

    let client = RemoteClient::connect(&format!("{addr}"), None)
        .await
        .expect("connect");
    let cache = client
        .new_dmap("sessions", DMapOptions::default())
        .await
        .expect("new_dmap");
    cache
        .put("u1", b"hello", PutOptions::default())
        .await
        .expect("put");
    let resp = cache.get("u1").await.expect("get");
    assert_eq!(resp.value, b"hello".to_vec());
    assert!(cache.delete("u1").await.expect("delete"));

    let _ = client.close().await;
    shutdown.trigger();
    let _ = server_task.await;
}

#[tokio::test]
#[ignore = "enabled after merge with Phase 2A (codec impl)"]
async fn auth_required_blocks_unauthenticated_dm_put() {
    let mut cfg = Config::default();
    cfg.auth.password = "secret".into();
    let TestServer { server, _embedded } = start_server(cfg).await;
    let addr = server.local_addr();
    let shutdown = server.shutdown_handle();
    let server_task = tokio::spawn(server.run());

    let connect = RemoteClient::connect(&format!("{addr}"), None).await;
    if let Ok(client) = connect {
        let dmap = client
            .new_dmap("x", DMapOptions::default())
            .await
            .expect("new_dmap factory");
        let err = dmap
            .put("k", b"v", PutOptions::default())
            .await
            .expect_err("unauthenticated PUT must fail");
        let msg = format!("{err}");
        assert!(msg.contains("auth") || msg.contains("NOAUTH"));
        let _ = client.close().await;
    }

    let client = RemoteClient::connect(&format!("{addr}"), Some("secret"))
        .await
        .expect("authed connect");
    let dmap = client.new_dmap("x", DMapOptions::default()).await.unwrap();
    dmap.put("k", b"v", PutOptions::default())
        .await
        .expect("authed PUT");
    let _ = client.close().await;

    shutdown.trigger();
    let _ = server_task.await;
}

#[tokio::test]
#[ignore = "enabled after merge with Phase 2A (codec impl)"]
async fn idle_close_disconnects() {
    let mut cfg = Config::default();
    cfg.network.idle_close = Duration::from_millis(200);
    let TestServer { server, _embedded } = start_server(cfg).await;
    let addr = server.local_addr();
    let shutdown = server.shutdown_handle();
    let server_task = tokio::spawn(server.run());

    let client = RemoteClient::connect(&format!("{addr}"), None)
        .await
        .expect("connect");
    client.ping(&format!("{addr}")).await.expect("first ping");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let res = client.stats(StatsOptions).await;
    assert!(res.is_err(), "stats after idle close must fail");

    shutdown.trigger();
    let _ = server_task.await;
}

#[tokio::test]
#[ignore = "enabled after merge with Phase 2A (codec impl)"]
async fn hello_resp3_upgrade() {
    let cfg = Config::default();
    let TestServer { server, _embedded } = start_server(cfg).await;
    let addr = server.local_addr();
    let shutdown = server.shutdown_handle();
    let server_task = tokio::spawn(server.run());

    // `RemoteClient::connect` sends HELLO 3; if the server speaks back, the
    // client is ready to dispatch follow-up commands.
    let client = RemoteClient::connect(&format!("{addr}"), None)
        .await
        .expect("connect");
    client
        .ping(&format!("{addr}"))
        .await
        .expect("post-HELLO ping");
    let _ = client.close().await;

    shutdown.trigger();
    let _ = server_task.await;
}
