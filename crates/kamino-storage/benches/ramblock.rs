//! Engine-level criterion benches.
//!
//! Run with `cargo bench -p kamino-storage`. Aspirational targets from
//! `ROADMAP.md` §6 Phase 1 are 100k PUT/s and 250k GET/s on a single core —
//! these benches measure where we actually are.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use kamino_storage::{Entry, RamBlock, StorageEngine};
use tokio::runtime::Runtime;

fn make_entry(i: u64, value_len: usize) -> Entry {
    Entry {
        key: format!("k{i:010}").into_bytes(),
        ttl_nanos: 0,
        timestamp_nanos: i64::try_from(i + 1).unwrap_or(i64::MAX),
        last_access_nanos: i64::try_from(i + 1).unwrap_or(i64::MAX),
        value: vec![0u8; value_len],
    }
}

fn build_engine(populated: u64, value_len: usize, rt: &Runtime) -> RamBlock {
    let mut engine = RamBlock::new(1024 * 1024, 0.40);
    rt.block_on(async {
        for i in 0..populated {
            engine.put(i, &make_entry(i, value_len)).await.unwrap();
        }
    });
    engine
}

fn bench_put(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("ramblock_put");
    for value_len in [1usize, 64, 256] {
        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("value_{value_len}B"), |b| {
            b.iter_batched(
                || (RamBlock::new(1024 * 1024, 0.40), 0_u64),
                |(mut engine, mut i)| {
                    rt.block_on(async {
                        let e = make_entry(i, value_len);
                        engine.put(black_box(i), black_box(&e)).await.unwrap();
                        i += 1;
                        (engine, i)
                    })
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("ramblock_get");
    let populated: u64 = 50_000;
    for value_len in [1usize, 64, 256] {
        let engine = build_engine(populated, value_len, &rt);
        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("value_{value_len}B"), |b| {
            let mut i: u64 = 0;
            b.iter(|| {
                rt.block_on(async {
                    let hit = engine.get(black_box(i % populated)).await.unwrap();
                    black_box(hit);
                    i = i.wrapping_add(1);
                });
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_put, bench_get);
criterion_main!(benches);
