//! End-to-end criterion benches for the embedded surface.
//!
//! Run with `cargo bench -p kamino`. Measures the user-visible cost of
//! `DMap::put` and `DMap::get` — i.e. through `EmbeddedClient` and
//! `Fragment`, not the raw engine.

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use kamino::{Config, Kamino, Mode, PutOptions};
use kamino_client::{DMap, DMapOptions};
use tokio::runtime::Runtime;

fn build_node(rt: &Runtime) -> (Kamino, Arc<dyn DMap>) {
    rt.block_on(async {
        let config = Config {
            mode: Mode::EmbeddedSolo,
            ..Config::default()
        };
        let node = Kamino::embedded(config).await.expect("embedded");
        let client = node.client();
        let dmap = client
            .new_dmap("bench", DMapOptions::default())
            .await
            .expect("new_dmap");
        (node, dmap)
    })
}

fn bench_put(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (_node, dmap) = build_node(&rt);
    let mut group = c.benchmark_group("embedded_dmap_put");
    let value = vec![0u8; 64];
    group.throughput(Throughput::Elements(1));
    group.bench_function("value_64B", |b| {
        let mut i: u64 = 0;
        b.iter(|| {
            rt.block_on(async {
                let key = format!("k{i:010}");
                dmap.put(black_box(&key), black_box(&value), PutOptions::default())
                    .await
                    .expect("put");
                i = i.wrapping_add(1);
            });
        });
    });
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (_node, dmap) = build_node(&rt);
    let populated: u64 = 50_000;
    rt.block_on(async {
        let value = vec![0u8; 64];
        for i in 0..populated {
            let key = format!("k{i:010}");
            dmap.put(&key, &value, PutOptions::default())
                .await
                .expect("seed put");
        }
    });
    let mut group = c.benchmark_group("embedded_dmap_get");
    group.throughput(Throughput::Elements(1));
    group.bench_function("value_64B", |b| {
        let mut i: u64 = 0;
        b.iter(|| {
            rt.block_on(async {
                let key = format!("k{:010}", i % populated);
                let resp = dmap.get(black_box(&key)).await.expect("get");
                black_box(resp);
                i = i.wrapping_add(1);
            });
        });
    });
    group.finish();
}

criterion_group!(benches, bench_put, bench_get);
criterion_main!(benches);
