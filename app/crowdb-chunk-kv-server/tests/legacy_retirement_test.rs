// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_chunk_kv_server::storage::{
    legacy_retirement::{collect_legacy_storage_for_tests as collect, LegacyRetirementPending as Pending},
    open_root_catalog_with_process_for_tests as open,
};
use crowdb_tree_ffi::CtError;
use std::sync::{atomic::Ordering, Arc};
#[path = "common/legacy_retirement.rs"]
mod fixture;
#[path = "common/metadata.rs"]
mod metadata;
use fixture::{entry, expire, process};
use fixture::{manifest, typed_root, TestPhysical};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quiesced_writer_is_not_a_reader_exclusion_and_deleted_bytes_are_confirmed() {
    let metadata = metadata::TestMetadata::start().await;
    let old_process = process(&metadata.client, 10).await;
    let old = open(metadata.client.clone(), 0, 1, &entry(7), old_process.clone())
        .await
        .unwrap();
    old.store().publish(42, 0, 7, 1, manifest()).unwrap();
    old.store().prepare_legacy_migration(42, 1, manifest()).unwrap();
    old.store().publish(42, 1, 7, 2, &typed_root()).unwrap();
    old.store()
        .stage_protection()
        .unwrap()
        .writer_quiesced(42, 7)
        .unwrap();
    let current = process(&metadata.client, 20).await;
    let physical = Arc::new(TestPhysical::new());
    let bytes = physical.physical_bytes();
    let count = physical.count();
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
        Some(Pending::LiveProcess {
            tree: 42,
            generation: 1,
            ..
        })
    ));
    assert_eq!(physical.deletes.load(Ordering::Relaxed), 0);
    expire(&metadata.client, &old_process).await;
    let mut reclaimed = 0;
    for step in 0..128 {
        let progress = collect(
            metadata.client.clone(),
            current.clone(),
            physical.clone(),
            physical.clone(),
            4,
        )
        .await
        .unwrap();
        assert!(progress.pending.is_none(), "{progress:?}");
        reclaimed += progress.reclaimed_bytes;
        if progress.complete {
            break;
        }
        assert!(step < 127, "bounded invocations did not finish");
    }
    assert!(physical.all_deleted());
    assert_eq!(physical.deletes.load(Ordering::Relaxed), count);
    assert_eq!(physical.drains.load(Ordering::Relaxed), count);
    assert_eq!(reclaimed, bytes);
    assert_eq!(
        old.store().load_legacy_migration(42, 1).unwrap().unwrap(),
        manifest()
    );
    let again = collect(
        metadata.client.clone(),
        current,
        physical.clone(),
        physical.clone(),
        4,
    )
    .await
    .unwrap();
    assert!(again.complete);
    assert_eq!(again.reclaimed_bytes, 0);
    assert_eq!(physical.deletes.load(Ordering::Relaxed), count);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_generation_pin_must_be_released_and_new_pins_are_fenced() {
    let metadata = metadata::TestMetadata::start().await;
    let owner = process(&metadata.client, 10).await;
    let old = open(metadata.client.clone(), 0, 1, &entry(7), owner.clone())
        .await
        .unwrap();
    old.store().publish(42, 0, 7, 1, manifest()).unwrap();
    old.store().pin_generation(42, 9, 3, 1).unwrap();
    old.store().publish(42, 1, 7, 2, &typed_root()).unwrap();
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
    assert_eq!(
        progress.pending,
        Some(Pending::GenerationPin {
            tree: 42,
            generation: 1
        })
    );
    assert_eq!(old.store().pin_generation(42, 9, 4, 1), Err(CtError::Unavailable));
    old.store().unpin_generation(42, 9, 3).unwrap();
    expire(&metadata.client, &owner).await;
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
async fn unknown_delete_reply_resumes_the_exact_claim_without_a_second_delete() {
    let metadata = metadata::TestMetadata::start().await;
    let owner = process(&metadata.client, 10).await;
    let old = open(metadata.client.clone(), 0, 1, &entry(7), owner.clone())
        .await
        .unwrap();
    old.store().publish(42, 0, 7, 1, manifest()).unwrap();
    old.store().publish(42, 1, 7, 2, &typed_root()).unwrap();
    expire(&metadata.client, &owner).await;
    let current = process(&metadata.client, 20).await;
    let physical = Arc::new(TestPhysical::new());
    physical.lose_reply.store(true, Ordering::Relaxed);
    assert_eq!(
        collect(
            metadata.client.clone(),
            current.clone(),
            physical.clone(),
            physical.clone(),
            64
        )
        .await,
        Err(CtError::Unavailable)
    );
    assert_eq!(physical.deletes.load(Ordering::Relaxed), 1);
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
    assert_eq!(physical.deletes.load(Ordering::Relaxed), physical.count());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn historical_manifest_without_publication_provenance_stays_durably_pending() {
    let metadata = metadata::TestMetadata::start().await;
    let old = crowdb_chunk_kv_server::storage::open_root_catalog_for_tests(
        metadata.client.clone(),
        0,
        1,
        &entry(7),
    )
    .await
    .unwrap();
    old.publish(42, 0, 7, 1, manifest()).unwrap();
    old.publish(42, 1, 7, 2, &typed_root()).unwrap();
    let current = process(&metadata.client, 20).await;
    let physical = Arc::new(TestPhysical::new());
    for _ in 0..2 {
        let progress = collect(
            metadata.client.clone(),
            current.clone(),
            physical.clone(),
            physical.clone(),
            64,
        )
        .await
        .unwrap();
        assert_eq!(
            progress.pending,
            Some(Pending::MissingOwnerProof {
                tree: 42,
                generation: 1
            })
        );
        assert!(!progress.complete);
    }
    assert_eq!(physical.deletes.load(Ordering::Relaxed), 0);
    assert_eq!(old.load_legacy_migration(42, 1).unwrap().unwrap(), manifest());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_legacy_root_blocks_the_namespace_and_shared_chunks_delete_once() {
    let metadata = metadata::TestMetadata::start().await;
    let owner = process(&metadata.client, 10).await;
    let parent = open(metadata.client.clone(), 0, 1, &entry(7), owner.clone())
        .await
        .unwrap();
    parent.store().publish(42, 0, 7, 1, manifest()).unwrap();
    let mut child_entry = entry(7);
    child_entry.artifact.tree_id = 43;
    let child = open(metadata.client.clone(), 0, 1, &child_entry, owner.clone())
        .await
        .unwrap();
    let foreign = fixture::foreign_manifest(43);
    child.store().publish(43, 0, 7, 1, &foreign).unwrap();
    parent.store().publish(42, 1, 7, 2, &typed_root()).unwrap();
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
    assert_eq!(progress.pending, Some(Pending::LegacyRoot { tree: 43 }));
    assert_eq!(physical.deletes.load(Ordering::Relaxed), 0);
    let replacement = open(metadata.client.clone(), 0, 1, &child_entry, current.clone())
        .await
        .unwrap();
    replacement
        .store()
        .publish(43, 1, 7, 2, &fixture::foreign_typed_root(43))
        .unwrap();
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
    assert_eq!(physical.deletes.load(Ordering::Relaxed), physical.count());
}

#[path = "legacy_retirement/readers.rs"]
mod readers;

#[path = "legacy_retirement/allocation.rs"]
mod allocation;
