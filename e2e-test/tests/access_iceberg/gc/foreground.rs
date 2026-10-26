// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{frontend::TestFrontend, native::TestNativeStorage};
use super::seed::{seed_files, seed_table};
use bytes::Bytes;
use crowdb_access_s3::storage::S3StorageClients;
use crowdb_chunk_client::{ChunkIoWriter, ChunkReadPolicy, SmallWritePolicy};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

mod pressure;
mod progress;
use pressure::TestSdkPressure;
use progress::TestGcProgress;

fn latency(samples: &[u64]) -> serde_json::Value {
    assert!(!samples.is_empty());
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    serde_json::json!({
        "raw_ns": samples,
        "count": samples.len(),
        "p50_ns": ordered[(ordered.len() - 1) / 2],
        "p99_ns": ordered[(ordered.len() * 99).div_ceil(100) - 1],
    })
}

pub(super) async fn write_s3_with_delay(seeds: Vec<String>, progress: Arc<AtomicUsize>, delay: Duration) {
    write_s3_until_stopped(seeds, progress, delay, Arc::new(AtomicBool::new(true))).await;
}

async fn write_s3_until_stopped(
    seeds: Vec<String>,
    progress: Arc<AtomicUsize>,
    delay: Duration,
    stop: Arc<AtomicBool>,
) {
    let s3 = S3StorageClients::connect_with_read_policy(
        seeds,
        2,
        1,
        SmallWritePolicy {
            conversion_enabled: false,
            mirror_copies: 2,
            ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::IcebergTable)
        },
        ChunkReadPolicy::default(),
    )
    .await
    .unwrap();
    let mut samples = Vec::new();
    let mut index = 0;
    while index < 64 || !stop.load(Ordering::Acquire) {
        let started = Instant::now();
        let data = Bytes::from(vec![u8::try_from(index % 256).unwrap(); 128]);
        let mut writer = s3.chunks.prepare_small_write(data.len()).await.unwrap();
        writer.on_data(data).await.unwrap();
        writer.on_finish().await.unwrap();
        samples.push(u64::try_from(started.elapsed().as_nanos()).unwrap());
        progress.fetch_add(1, Ordering::Release);
        index += 1;
        tokio::time::sleep(delay).await;
    }
    s3.chunks.shutdown_small_writes().await.unwrap();
    println!("gc_foreground_small_write {}", latency(&samples));
}

fn start_small_writes(
    seeds: Vec<String>,
    progress: &Arc<AtomicUsize>,
    stop: &Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(write_s3_until_stopped(
        seeds,
        Arc::clone(progress),
        Duration::from_millis(50),
        Arc::clone(stop),
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the pinned PyIceberg environment"]
async fn official_sdk_foreground_progresses_under_gc_backlog() {
    let stack = TestNativeStorage::new();
    let (store, context, table) = seed_table(&stack).await;
    seed_files(store.as_ref(), context, table, 128).await;
    let server = TestFrontend::start(13).await;
    let mut client = TestSdkPressure::start(&server.origin);
    let s3_progress = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let s3_writes = start_small_writes(stack.seeds.clone(), &s3_progress, &stop);
    let mut progress = TestGcProgress::new(store, context, table);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(90), async {
        loop {
            if let Some(status) = client.try_wait() {
                break status;
            }
            progress.observe(&client, &s3_progress, &stop).await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await;
    client.stop();
    stop.store(true, Ordering::Release);
    let status =
        outcome.expect("GC and successful foreground work did not overlap within the existing budget");
    client.print_report();
    assert!(status.success(), "official SDK foreground operations failed");
    s3_writes.await.unwrap();
    assert!(s3_progress.load(Ordering::Acquire) >= 64);
    assert!(
        progress.overlapped,
        "GC did not advance while the SDK requests were active"
    );
    let continued = progress.continued().await;
    println!(
        "gc_foreground_backlog {}",
        serde_json::json!({
            "source_files":128,"observations_elapsed_ms_revision_phase":progress.revisions,
            "overlap_baseline_revision_sdk_cycles_small_writes":progress.baseline,
            "successful_sdk_cycles":client.progress(),"small_writes":s3_progress.load(Ordering::Acquire),
            "continued_revision":continued.revision,"continued_phase":format!("{:?}",continued.phase),
            "elapsed_ms":progress.started.elapsed().as_millis(),"idle_interval":"production default",
            "observer":"one task GET per 100ms during overlap and continuation checks; no failed-request retries",
            "scope":"same-host overlap and exploratory tails; no isolated baseline or production SLO",
        })
    );
}

pub(super) const SDK_PRESSURE_SCRIPT: &str = include_str!("sdk_pressure.py");
