// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_later_registered_reader_survives_publication_writer_exclusion() {
    let metadata = metadata::TestMetadata::start().await;
    let owner = process(&metadata.client, 10).await;
    let original = open(metadata.client.clone(), 0, 1, &entry(7), owner.clone())
        .await
        .unwrap();
    original.store().publish(42, 0, 7, 1, manifest()).unwrap();
    original
        .store()
        .stage_protection()
        .unwrap()
        .writer_quiesced(42, 7)
        .unwrap();
    let reader_process = process(&metadata.client, 30).await;
    let reader = open(metadata.client.clone(), 0, 1, &entry(7), reader_process.clone())
        .await
        .unwrap();
    assert_eq!(
        reader
            .store()
            .load(42, crowdb_tree_ffi::RootCatalogObject::Manifest(1))
            .unwrap()
            .unwrap(),
        manifest()
    );
    reader.store().publish(42, 1, 7, 2, &typed_root()).unwrap();
    reader
        .store()
        .stage_protection()
        .unwrap()
        .writer_quiesced(42, 7)
        .unwrap();
    expire(&metadata.client, &owner).await;
    let current = process(&metadata.client, 20).await;
    let physical = Arc::new(TestPhysical::new());
    let progress = collect(
        metadata.client.clone(),
        current.clone(),
        physical.clone(),
        physical.clone(),
        64,
    )
    .await
    .unwrap();
    assert!(matches!(
        progress.pending,
        Some(Pending::LiveProcess { instance: 30, .. })
    ));
    assert_eq!(physical.deletes.load(Ordering::Relaxed), 0);
    assert_eq!(
        reader
            .store()
            .load(42, crowdb_tree_ffi::RootCatalogObject::Manifest(1)),
        Err(CtError::Unavailable)
    );
    expire(&metadata.client, &reader_process).await;
    for step in 0..16 {
        let progress = collect(
            metadata.client.clone(),
            current.clone(),
            physical.clone(),
            physical.clone(),
            64,
        )
        .await
        .unwrap();
        assert!(progress.pending.is_none(), "{progress:?}");
        if progress.complete {
            break;
        }
        assert!(step < 15);
    }
    assert!(physical.all_deleted());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_resume_does_not_start_a_namespace_upgrade() {
    let metadata = metadata::TestMetadata::start().await;
    let current = process(&metadata.client, 20).await;
    let physical = Arc::new(TestPhysical::new());
    let progress = crowdb_chunk_kv_server::storage::legacy_retirement::resume_legacy_storage_for_tests(
        metadata.client.clone(),
        current.clone(),
        physical.clone(),
        physical.clone(),
        64,
    )
    .await
    .unwrap();
    assert!(progress.is_none());
    let old = open(metadata.client.clone(), 0, 1, &entry(7), current.clone())
        .await
        .unwrap();
    old.store().publish(42, 0, 7, 1, manifest()).unwrap();
    assert_eq!(physical.deletes.load(Ordering::Relaxed), 0);
}
