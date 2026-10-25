// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{frontend::TestFrontend, native::TestNativeStorage};
use super::seed::{seed_files, seed_purge_marker, seed_table};
use bytes::Bytes;
use crowdb_access_iceberg::{gc::GcRepository, key::OperationId};
use crowdb_access_s3::storage::S3StorageClients;
use crowdb_chunk_client::{ChunkIoWriter, ChunkReadPolicy, SmallWritePolicy};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

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

async fn write_s3_during_gc(seeds: Vec<String>, progress: Arc<AtomicUsize>) {
    write_s3_with_delay(seeds, progress, Duration::from_millis(50)).await;
}

pub(super) async fn write_s3_with_delay(seeds: Vec<String>, progress: Arc<AtomicUsize>, delay: Duration) {
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
    for index in 0..64 {
        let started = Instant::now();
        let data = Bytes::from(vec![u8::try_from(index).unwrap(); 128]);
        let mut writer = s3.chunks.prepare_small_write(data.len()).await.unwrap();
        writer.on_data(data).await.unwrap();
        writer.on_finish().await.unwrap();
        samples.push(u64::try_from(started.elapsed().as_nanos()).unwrap());
        progress.fetch_add(1, Ordering::Release);
        tokio::time::sleep(delay).await;
    }
    s3.chunks.shutdown_small_writes().await.unwrap();
    println!("gc_foreground_small_write {}", latency(&samples));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the pinned PyIceberg environment"]
async fn official_sdk_foreground_progresses_under_gc_backlog() {
    let stack = TestNativeStorage::new();
    let (store, context, table) = seed_table(&stack).await;
    seed_files(store.as_ref(), context, table, 128).await;
    seed_purge_marker(store.as_ref(), context, table).await;
    let server = TestFrontend::start(12).await;
    let mut client = sdk_client(&server.origin);
    let s3_progress = Arc::new(AtomicUsize::new(0));
    let s3_writes = tokio::spawn(write_s3_during_gc(stack.seeds.clone(), Arc::clone(&s3_progress)));
    let repository = GcRepository::new(store);
    let identity = OperationId::from_bytes(table.as_bytes()).unwrap();
    let started = Instant::now();
    let mut revisions = Vec::new();
    let mut overlapped = false;
    let status = tokio::time::timeout(std::time::Duration::from_secs(90), async {
        loop {
            if let Some(status) = client.try_wait().unwrap() {
                break status;
            }
            if let Some(task) = repository.task(context.catalog, identity).await.unwrap() {
                if revisions
                    .last()
                    .is_none_or(|(_, revision, _)| *revision != task.revision)
                {
                    revisions.push((
                        started.elapsed().as_millis(),
                        task.revision,
                        format!("{:?}", task.phase),
                    ));
                }
                overlapped |= task.revision > 1 && s3_progress.load(Ordering::Acquire) > 0;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert!(status.success(), "official SDK foreground operations failed");
    s3_writes.await.unwrap();
    assert_eq!(s3_progress.load(Ordering::Acquire), 64);
    assert!(
        overlapped,
        "GC did not advance while the SDK requests were active"
    );
    let first_revision = revisions.first().expect("GC task was never observed").1;
    let continued = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let current = repository.task(context.catalog, identity).await.unwrap().unwrap();
            if current.revision >= first_revision + 2 {
                break current;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("backlogged GC did not continue beyond its first scheduled step");
    println!(
        "gc_foreground_backlog {}",
        serde_json::json!({
            "source_files":128,"observations_elapsed_ms_revision_phase":revisions,
            "continued_revision":continued.revision,"continued_phase":format!("{:?}",continued.phase),
            "elapsed_ms":started.elapsed().as_millis(),"idle_interval":"production default",
            "observer":"one task GET per 100ms during overlap and continuation checks; no failed-request retries",
            "scope":"same-host overlap and exploratory tails; no isolated baseline or production SLO",
        })
    );
}

pub(super) const SDK_PRESSURE_SCRIPT: &str = r#"import sys, json, time
from concurrent.futures import ThreadPoolExecutor
import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

def run(worker):
    samples = {}
    windows = {}
    def measure(operation, call):
        begin = time.time_ns() if len(sys.argv) > 2 else 0
        started = time.perf_counter_ns()
        result = call()
        samples.setdefault(operation, []).append(time.perf_counter_ns() - started)
        if begin:
            windows.setdefault(operation, []).append([begin, time.time_ns()])
        return result
    catalog = load_catalog(f"gc-{worker}", type="rest", uri=sys.argv[1], token="w" * 32)
    prefix = sys.argv[4] if len(sys.argv) > 4 else "gc-pressure"
    namespace = (f"{prefix}-{worker}",)
    measure("create_namespace", lambda: catalog.create_namespace(namespace))
    for index in range(int(sys.argv[2]) if len(sys.argv) > 2 else 3):
        identifier = namespace + (f"events-{index}",)
        table = measure("create_table", lambda: catalog.create_table(identifier, Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=True))))
        measure("commit", lambda: table.transaction().set_properties({"gc-probe": str(index)}).commit_transaction())
        loaded = measure("load_table", lambda: catalog.load_table(identifier))
        assert loaded.properties["gc-probe"] == str(index)
        response = measure("credentials", lambda: requests.get(
            f"{sys.argv[1]}/v1/namespaces/{namespace[0]}/tables/{identifier[1]}/credentials",
            headers={"Authorization": "Bearer " + "w" * 32},
            timeout=5,
        ))
        response.raise_for_status()
        loaded.io.properties.update(response.json()["storage-credentials"][0]["config"])
        def read_metadata():
            with loaded.io.new_input(loaded.metadata_location).open() as stream:
                return stream.read()
        assert measure("read_metadata", read_metadata).startswith(b"{")
        measure("drop_table", lambda: catalog.drop_table(identifier))
        if len(sys.argv) > 3:
            time.sleep(float(sys.argv[3]))
    measure("drop_namespace", lambda: catalog.drop_namespace(namespace))
    return samples, windows

with ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(run, range(4)))
combined = {}
combined_windows = {}
for result, windows in results:
    for operation, samples in result.items():
        combined.setdefault(operation, []).extend(samples)
        combined_windows.setdefault(operation, []).extend(windows.get(operation, []))
report = {}
for operation, samples in combined.items():
    ordered = sorted(samples)
    report[operation] = {"raw_ns":samples,"count":len(samples),"p50_ns":ordered[(len(ordered)-1)//2],"p99_ns":ordered[(99*len(ordered)+99)//100-1],"windows_unix_ns":combined_windows[operation]}
print("gc_foreground_official_sdk " + json.dumps(report), flush=True)
"#;

fn sdk_client(origin: &str) -> std::process::Child {
    let client = std::process::Command::new("pixi")
        .args(["run", "-e", "iceberg-e2e", "python"])
        .arg("-c")
        .arg(SDK_PRESSURE_SCRIPT)
        .arg(origin)
        .spawn()
        .unwrap();
    client
}
