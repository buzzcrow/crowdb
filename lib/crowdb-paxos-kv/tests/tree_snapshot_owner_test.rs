// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_paxos_kv::kv::{CrowdbTreeConfig, CrowdbTreeEngine, KVEngine};
use crowdb_rpc_ffi::Buffer;
use crowdb_tree_ffi::{TreeRuntime, TreeRuntimeOptions};
use std::sync::Arc;

#[test]
fn snapshot_view_propagates_failed_handoff_without_returning_an_old_prefix() {
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let mut config = CrowdbTreeConfig {
        runtime: Some(runtime),
        ..Default::default()
    };
    config.runtime_binding.limits.handoff_bytes = 1024 * 1024;
    let engine = CrowdbTreeEngine::open(&config).unwrap();
    engine.handle().apply_put(1, b"key", b"value").unwrap();
    assert!(engine.handle().flush().is_err());
    let result = engine.snapshot_view();
    assert!(
        result.is_err(),
        "a rejected handoff cannot expose an older successful snapshot: {result:?}"
    );
    assert_eq!(engine.handle().get(b"key").unwrap(), Some((1, b"value".to_vec())));
}

#[test]
fn rpc_snapshot_buffer_retains_native_bytes_after_all_engine_handles_close() {
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let engine = CrowdbTreeEngine::open(&CrowdbTreeConfig {
        runtime: Some(Arc::clone(&runtime)),
        ..Default::default()
    })
    .unwrap();
    let tree = engine.handle();
    tree.apply_put(1, b"key", &vec![b'v'; 8192]).unwrap();
    tree.flush().unwrap();
    let mut export = engine.snapshot_export_begin(64).unwrap();
    let chunk = export.read(0).unwrap();
    assert_eq!(chunk.bytes.len(), 64);
    let retained = chunk.bytes.slice(4..36);
    let pointer = retained.as_ptr();
    let expected = retained.to_vec();
    assert!(retained.owner_allocation().is_some());
    let response = Buffer::try_from_owned_bytes(retained).unwrap();
    assert_eq!(response.as_slice().as_ptr(), pointer);
    drop(chunk);
    drop(export);
    drop(tree);
    drop(engine);
    drop(runtime);
    assert_eq!(response.as_slice(), expected);
}

#[tokio::test(flavor = "current_thread")]
async fn versioned_and_byte_reads_keep_the_account_on_resident_and_cold_paths() {
    let runtime = Arc::new(
        TreeRuntime::new(TreeRuntimeOptions {
            workers: 1,
            ..Default::default()
        })
        .unwrap(),
    );
    let config = CrowdbTreeConfig {
        runtime: Some(runtime.clone()),
        page_store: Some(Arc::new(crowdb_tree_ffi::PageStore::open_mem(4096).unwrap())),
        ..Default::default()
    };
    let engine = CrowdbTreeEngine::open(&config).unwrap();
    let expected = vec![b'v'; 8192];
    engine.handle().apply_put(1, b"key", &expected).unwrap();
    let (_, resident) = engine.get_bytes(b"key").await.unwrap();
    assert!(resident.owner_allocation().is_some());
    let checkpoint = engine.persist_snapshot_async().unwrap().await.unwrap();
    assert!(checkpoint.status.is_ok(), "{:?}", checkpoint.status);
    assert!(checkpoint.durable_committed);
    drop(engine);
    let reopened = CrowdbTreeEngine::open(&config).unwrap();
    // No yield between old pump cancellation and the replacement Tree opening.
    let (slot, cold) =
        tokio::time::timeout(std::time::Duration::from_secs(2), reopened.get_versioned(b"key"))
            .await
            .expect("replacement Tree completion remains registered")
            .unwrap()
            .unwrap();
    assert_eq!(slot, 1);
    assert!(cold.owner_allocation().is_some());
    let response = Buffer::try_from_owned_bytes(cold).unwrap();
    drop(reopened);
    drop(config);
    drop(runtime);
    assert_eq!(resident.as_ref(), expected);
    assert_eq!(response.as_slice(), expected);
}
