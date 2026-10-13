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

async fn write_s3_during_gc(seeds: Vec<String>, progress: Arc<AtomicUsize>) {
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
    for index in 0..64 {
        let data = Bytes::from(vec![u8::try_from(index).unwrap(); 128]);
        let mut writer = s3.chunks.prepare_small_write(data.len()).await.unwrap();
        writer.on_data(data).await.unwrap();
        writer.on_finish().await.unwrap();
        progress.fetch_add(1, Ordering::Release);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    s3.chunks.shutdown_small_writes().await.unwrap();
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
    let overlapped = tokio::time::timeout(std::time::Duration::from_secs(90), async {
        loop {
            if client.try_wait().unwrap().is_some() {
                break false;
            }
            if repository
                .task(context.catalog, identity)
                .await
                .unwrap()
                .is_some_and(|task| task.revision > 1)
                && s3_progress.load(Ordering::Acquire) > 0
            {
                break true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let status = tokio::time::timeout(std::time::Duration::from_secs(90), async {
        loop {
            if let Some(status) = client.try_wait().unwrap() {
                break status;
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
}

fn sdk_client(origin: &str) -> std::process::Child {
    let script = r#"import sys
from concurrent.futures import ThreadPoolExecutor
import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

def run(worker):
    catalog = load_catalog(f"gc-{worker}", type="rest", uri=sys.argv[1], token="w" * 32)
    namespace = (f"gc-pressure-{worker}",)
    catalog.create_namespace(namespace)
    for index in range(3):
        identifier = namespace + (f"events-{index}",)
        table = catalog.create_table(identifier, Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=True)))
        table.transaction().set_properties({"gc-probe": str(index)}).commit_transaction()
        loaded = catalog.load_table(identifier)
        assert loaded.properties["gc-probe"] == str(index)
        response = requests.get(
            f"{sys.argv[1]}/v1/namespaces/{namespace[0]}/tables/{identifier[1]}/credentials",
            headers={"Authorization": "Bearer " + "w" * 32},
            timeout=5,
        )
        response.raise_for_status()
        loaded.io.properties.update(response.json()["storage-credentials"][0]["config"])
        with loaded.io.new_input(loaded.metadata_location).open() as stream:
            assert stream.read().startswith(b"{")
        catalog.drop_table(identifier)
    catalog.drop_namespace(namespace)

with ThreadPoolExecutor(max_workers=4) as executor:
    list(executor.map(run, range(4)))
"#;
    let client = std::process::Command::new("pixi")
        .args(["run", "-e", "iceberg-e2e", "python"])
        .arg("-c")
        .arg(script)
        .arg(origin)
        .spawn()
        .unwrap();
    client
}
