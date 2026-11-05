// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_chunk_kv_server::storage::open_root_catalog_with_process_for_tests as open;
use crowdb_tree_ffi::chunk::protection::{reference_checksum, CandidateState, ManifestCandidate};
use crowdb_tree_ffi::chunk::stage::{StageId, StagePack, StageState};
use crowdb_tree_ffi::{CtError, RootCatalogObject};
#[path = "common/root_process.rs"]
mod fixture;
#[path = "common/metadata.rs"]
mod metadata;
use fixture::{entry, expire, next, process, recovered, stage};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_pin_pruning_commits_bounded_progress() {
    let metadata = metadata::TestMetadata::start().await;
    let old_process = process(&metadata.client, 10).await;
    let old = open(metadata.client.clone(), 0, 1, &entry(1), old_process.clone())
        .await
        .unwrap();
    let reserved = stage();
    old.store().compare_stage(42, 1, None, Some(&reserved)).unwrap();
    let mut writing = next(&reserved, StageState::Writing);
    writing.packs.push(StagePack {
        offset: 0,
        length: 100,
        checksum: 7,
    });
    old.store()
        .compare_stage(42, 1, Some(&reserved), Some(&writing))
        .unwrap();
    let ready = next(&writing, StageState::Reloadable);
    old.store()
        .compare_stage(42, 1, Some(&writing), Some(&ready))
        .unwrap();
    let protection = old.store().stage_protection().unwrap();
    for low in 100..164 {
        protection
            .pin_stage(
                42,
                ready.id,
                StageId {
                    operation_low: low,
                    ..ready.id
                },
            )
            .unwrap();
    }
    let replacement = open(
        metadata.client.clone(),
        0,
        1,
        &entry(2),
        process(&metadata.client, 20).await,
    )
    .await
    .unwrap();
    replacement
        .complete_recovery(&recovered(2).await, 0)
        .await
        .unwrap();
    expire(&metadata.client, &old_process).await;
    let protection = replacement.store().stage_protection().unwrap();
    let first = protection.prune_stage_pins(42, ready.id, None, 64).unwrap();
    assert_eq!(first.released, 62);
    assert!(first.more);
    let second = protection
        .prune_stage_pins(42, ready.id, first.after, 64)
        .unwrap();
    assert_eq!(second.released, 3);
    assert!(!second.more);
    replacement
        .store()
        .compare_stage(42, 2, Some(&ready), Some(&next(&ready, StageState::Retired)))
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_epoch_and_completed_recovery_do_not_erase_a_live_old_reader() {
    let metadata = metadata::TestMetadata::start().await;
    let old_process = process(&metadata.client, 10).await;
    let old = open(metadata.client.clone(), 0, 1, &entry(1), old_process.clone())
        .await
        .unwrap();
    let reserved = stage();
    old.store().compare_stage(42, 1, None, Some(&reserved)).unwrap();
    let mut writing = next(&reserved, StageState::Writing);
    writing.packs.push(StagePack {
        offset: 0,
        length: 100,
        checksum: 7,
    });
    old.store()
        .compare_stage(42, 1, Some(&reserved), Some(&writing))
        .unwrap();
    let ready = next(&writing, StageState::Reloadable);
    old.store()
        .compare_stage(42, 1, Some(&writing), Some(&ready))
        .unwrap();
    let pin = StageId {
        operation_low: 99,
        ..ready.id
    };
    let protection = old.store().stage_protection().unwrap();
    protection.pin_stage(42, ready.id, pin).unwrap();
    protection.unpin_stage(42, ready.id, ready.id).unwrap();
    protection.writer_quiesced(42, 1).unwrap();
    assert_eq!(protection.check_read_owner(), Ok(()));
    let replacement = open(
        metadata.client.clone(),
        0,
        1,
        &entry(2),
        process(&metadata.client, 20).await,
    )
    .await
    .unwrap();
    replacement
        .complete_recovery(&recovered(2).await, 0)
        .await
        .unwrap();
    let reader = replacement.store().stage_protection().unwrap();
    let progress = reader.prune_stage_pins(42, ready.id, None, 64).unwrap();
    assert_eq!(progress.released, 0);
    let retired = next(&ready, StageState::Retired);
    assert_eq!(
        replacement
            .store()
            .compare_stage(42, 2, Some(&ready), Some(&retired)),
        Err(CtError::Unavailable)
    );
    protection.unpin_stage(42, ready.id, pin).unwrap();
    replacement
        .store()
        .compare_stage(42, 2, Some(&ready), Some(&retired))
        .unwrap();
    assert_eq!(
        protection.pin_stage(
            42,
            ready.id,
            StageId {
                operation_low: 100,
                ..pin
            }
        ),
        Err(CtError::Unavailable)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_recovery_and_authoritative_process_exclusion_release_crash_pins() {
    let metadata = metadata::TestMetadata::start().await;
    let old_process = process(&metadata.client, 10).await;
    let old = open(metadata.client.clone(), 0, 1, &entry(1), old_process.clone())
        .await
        .unwrap();
    let reserved = stage();
    old.store().compare_stage(42, 1, None, Some(&reserved)).unwrap();
    let replacement = open(
        metadata.client.clone(),
        0,
        1,
        &entry(2),
        process(&metadata.client, 20).await,
    )
    .await
    .unwrap();
    let protection = replacement.store().stage_protection().unwrap();
    assert_eq!(
        protection.prune_stage_pins(42, reserved.id, None, 64),
        Err(CtError::Unavailable)
    );
    replacement
        .complete_recovery(&recovered(2).await, 0)
        .await
        .unwrap();
    assert_eq!(
        protection
            .prune_stage_pins(42, reserved.id, None, 64)
            .unwrap()
            .released,
        0
    );
    expire(&metadata.client, &old_process).await;
    assert_eq!(
        old.store().stage_protection().unwrap().check_read_owner(),
        Err(CtError::Unavailable)
    );
    let progress = protection.prune_stage_pins(42, reserved.id, None, 64).unwrap();
    assert_eq!(progress.examined, 1);
    assert_eq!(progress.released, 1);
    assert_eq!(
        protection
            .prune_stage_pins(42, reserved.id, None, 64)
            .unwrap()
            .released,
        0
    );
    replacement
        .store()
        .compare_stage(
            42,
            2,
            Some(&reserved),
            Some(&next(&reserved, StageState::Retired)),
        )
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_epoch_reopen_cannot_steal_a_live_writer_or_reuse_its_recovery_receipt() {
    let metadata = metadata::TestMetadata::start().await;
    let old_process = process(&metadata.client, 10).await;
    let old = open(metadata.client.clone(), 0, 1, &entry(1), old_process.clone())
        .await
        .unwrap();
    old.complete_recovery(&recovered(1).await, 0).await.unwrap();
    for generation in 1..=3 {
        old.store()
            .publish(42, generation - 1, 1, generation, b"root")
            .unwrap();
    }
    assert!(
        open(metadata.client.clone(), 0, 1, &entry(1), old_process.clone())
            .await
            .is_err()
    );
    let reserved = stage();
    old.store().compare_stage(42, 1, None, Some(&reserved)).unwrap();
    expire(&metadata.client, &old_process).await;
    let replacement = open(
        metadata.client.clone(),
        0,
        1,
        &entry(1),
        process(&metadata.client, 20).await,
    )
    .await
    .unwrap();
    assert_eq!(
        replacement
            .store()
            .stage_protection()
            .unwrap()
            .prune_stage_pins(42, reserved.id, None, 64),
        Err(CtError::Unavailable)
    );
    assert_eq!(old.store().reclaim_before(42, 3), 0);
    assert_eq!(old.store().pin_generation(42, 9, 9, 1), Err(CtError::Unavailable));
    assert_eq!(
        replacement
            .store()
            .load(42, RootCatalogObject::Manifest(1))
            .unwrap(),
        Some(b"root".to_vec())
    );
    replacement
        .complete_recovery(&recovered(1).await, 3)
        .await
        .unwrap();
    assert_eq!(replacement.store().reclaim_before(42, 3), 1);
    assert_eq!(
        replacement
            .store()
            .stage_protection()
            .unwrap()
            .prune_stage_pins(42, reserved.id, None, 64)
            .unwrap()
            .released,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovered_new_owner_aborts_old_candidate_before_releasing_its_references() {
    let metadata = metadata::TestMetadata::start().await;
    let old_process = process(&metadata.client, 10).await;
    let old = open(metadata.client.clone(), 0, 1, &entry(1), old_process.clone())
        .await
        .unwrap();
    let reserved = stage();
    old.store().compare_stage(42, 1, None, Some(&reserved)).unwrap();
    let mut writing = next(&reserved, StageState::Writing);
    writing.packs.push(StagePack {
        offset: 0,
        length: 100,
        checksum: 7,
    });
    old.store()
        .compare_stage(42, 1, Some(&reserved), Some(&writing))
        .unwrap();
    let ready = next(&writing, StageState::Reloadable);
    old.store()
        .compare_stage(42, 1, Some(&writing), Some(&ready))
        .unwrap();
    let id = StageId {
        operation_low: 66,
        ..ready.id
    };
    let candidate = ManifestCandidate {
        id,
        expected_generation: 0,
        reference_count: 1,
        reference_checksum: reference_checksum(0, &[ready.id]),
        manifest: b"pending-root".to_vec(),
    };
    let protection = old.store().stage_protection().unwrap();
    protection.begin_manifest_candidate(42, &candidate).unwrap();
    protection
        .prepare_manifest_references(42, id, 0, &[ready.id])
        .unwrap();
    let replacement = open(
        metadata.client.clone(),
        0,
        1,
        &entry(2),
        process(&metadata.client, 20).await,
    )
    .await
    .unwrap();
    let next_protection = replacement.store().stage_protection().unwrap();
    assert_eq!(
        next_protection.recover_manifest_candidate(42, id),
        Err(CtError::Unavailable)
    );
    replacement
        .complete_recovery(&recovered(2).await, 0)
        .await
        .unwrap();
    next_protection.recover_manifest_candidate(42, id).unwrap();
    assert_eq!(
        next_protection
            .load_manifest_candidate(42, id)
            .unwrap()
            .unwrap()
            .state,
        CandidateState::Aborted
    );
    assert_eq!(
        protection.commit_manifest_candidate(42, id),
        Err(CtError::Unavailable)
    );
    next_protection.release_manifest_references(42, id, 0).unwrap();
    next_protection.release_manifest_references(42, id, 0).unwrap();
    let retired = next(&ready, StageState::Retired);
    assert_eq!(
        replacement
            .store()
            .compare_stage(42, 2, Some(&ready), Some(&retired)),
        Err(CtError::Unavailable)
    );
    expire(&metadata.client, &old_process).await;
    assert_eq!(
        next_protection
            .prune_stage_pins(42, ready.id, None, 64)
            .unwrap()
            .released,
        1
    );
    replacement
        .store()
        .compare_stage(42, 2, Some(&ready), Some(&retired))
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsealed_stage_requires_creator_exclusion_even_after_the_last_pin() {
    let metadata = metadata::TestMetadata::start().await;
    let old_process = process(&metadata.client, 10).await;
    let old = open(metadata.client.clone(), 0, 1, &entry(1), old_process.clone())
        .await
        .unwrap();
    old.complete_recovery(&recovered(1).await, 0).await.unwrap();
    let reserved = stage();
    old.store().compare_stage(42, 1, None, Some(&reserved)).unwrap();
    old.store()
        .stage_protection()
        .unwrap()
        .unpin_stage(42, reserved.id, reserved.id)
        .unwrap();
    let retired = next(&reserved, StageState::Retired);
    assert_eq!(
        old.store().compare_stage(42, 1, Some(&reserved), Some(&retired)),
        Err(CtError::Unavailable)
    );
    let replacement = open(
        metadata.client.clone(),
        0,
        1,
        &entry(2),
        process(&metadata.client, 20).await,
    )
    .await
    .unwrap();
    replacement
        .complete_recovery(&recovered(2).await, 0)
        .await
        .unwrap();
    assert_eq!(
        replacement
            .store()
            .compare_stage(42, 2, Some(&reserved), Some(&retired)),
        Err(CtError::Unavailable)
    );
    expire(&metadata.client, &old_process).await;
    replacement
        .store()
        .compare_stage(42, 2, Some(&reserved), Some(&retired))
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_before_unknown_stage_creation_fences_its_late_retry() {
    let metadata = metadata::TestMetadata::start().await;
    let owner = open(
        metadata.client.clone(),
        0,
        1,
        &entry(1),
        process(&metadata.client, 10).await,
    )
    .await
    .unwrap();
    let reserved = stage();
    let protection = owner.store().stage_protection().unwrap();
    protection.unpin_stage(42, reserved.id, reserved.id).unwrap();
    protection.unpin_stage(42, reserved.id, reserved.id).unwrap();
    assert_eq!(
        owner.store().compare_stage(42, 1, None, Some(&reserved)),
        Err(CtError::Unavailable)
    );
    assert_eq!(owner.store().load_stage(42, reserved.id).unwrap(), None);
    let mut created_first = reserved.clone();
    created_first.id.operation_low += 1;
    created_first.chunk_low += 1;
    owner
        .store()
        .compare_stage(42, 1, None, Some(&created_first))
        .unwrap();
    protection
        .unpin_stage(42, created_first.id, created_first.id)
        .unwrap();
    protection
        .unpin_stage(42, created_first.id, created_first.id)
        .unwrap();
    assert_eq!(
        protection.prune_stage_pins(42, created_first.id, None, 64),
        Err(CtError::Unavailable)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_unknown_operation_can_retire_while_its_process_and_tree_keep_serving() {
    let metadata = metadata::TestMetadata::start().await;
    let process = process(&metadata.client, 10).await;
    let owner = open(metadata.client.clone(), 0, 1, &entry(1), process.clone())
        .await
        .unwrap();
    owner.complete_recovery(&recovered(1).await, 0).await.unwrap();
    let reserved = stage();
    owner.store().compare_stage(42, 1, None, Some(&reserved)).unwrap();
    let protection = owner.store().stage_protection().unwrap();
    protection.unpin_stage(42, reserved.id, reserved.id).unwrap();
    let retired = next(&reserved, StageState::Retired);
    assert_eq!(
        owner
            .store()
            .compare_stage(42, 1, Some(&reserved), Some(&retired)),
        Err(CtError::Unavailable)
    );
    fixture::age_operation_release(&metadata.client, reserved.id).await;
    protection.unpin_stage(42, reserved.id, reserved.id).unwrap();
    assert_eq!(protection.check_read_owner(), Ok(()));
    owner
        .store()
        .compare_stage(42, 1, Some(&reserved), Some(&retired))
        .unwrap();
    let mut ongoing = reserved;
    ongoing.id.operation_low += 1;
    ongoing.chunk_low += 1;
    owner.store().compare_stage(42, 1, None, Some(&ongoing)).unwrap();
    assert_eq!(protection.check_read_owner(), Ok(()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drained_writer_reopens_immediately_without_expiring_its_old_readers() {
    let metadata = metadata::TestMetadata::start().await;
    let process = process(&metadata.client, 10).await;
    let old = open(metadata.client.clone(), 0, 1, &entry(1), process.clone())
        .await
        .unwrap();
    let old_reader = old.store().stage_protection().unwrap();
    old_reader.writer_quiesced(42, 1).unwrap();
    let replacement = open(metadata.client.clone(), 0, 1, &entry(1), process)
        .await
        .unwrap();
    assert_eq!(old_reader.check_read_owner(), Ok(()));
    assert_eq!(
        old.store().compare_stage(42, 1, None, Some(&stage())),
        Err(CtError::Unavailable)
    );
    replacement
        .store()
        .compare_stage(42, 1, None, Some(&stage()))
        .unwrap();
    // An idempotent late release still refers only to the old incarnation.
    old_reader.writer_quiesced(42, 1).unwrap();
    let mut another = stage();
    another.id.operation_low += 1;
    another.chunk_low += 1;
    replacement
        .store()
        .compare_stage(42, 1, None, Some(&another))
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sealed_legacy_stage_can_retire_after_recovery_without_an_invented_process_owner() {
    let metadata = metadata::TestMetadata::start().await;
    let old = crowdb_chunk_kv_server::storage::open_root_catalog_for_tests(
        metadata.client.clone(),
        0,
        1,
        &entry(1),
    )
    .await
    .unwrap();
    let reserved = stage();
    old.compare_stage(42, 1, None, Some(&reserved)).unwrap();
    let mut writing = next(&reserved, StageState::Writing);
    writing.packs.push(StagePack {
        offset: 0,
        length: 100,
        checksum: 7,
    });
    old.compare_stage(42, 1, Some(&reserved), Some(&writing)).unwrap();
    let ready = next(&writing, StageState::Reloadable);
    old.compare_stage(42, 1, Some(&writing), Some(&ready)).unwrap();
    let replacement = open(
        metadata.client.clone(),
        0,
        1,
        &entry(2),
        process(&metadata.client, 20).await,
    )
    .await
    .unwrap();
    replacement
        .complete_recovery(&recovered(2).await, 0)
        .await
        .unwrap();
    replacement
        .store()
        .compare_stage(42, 2, Some(&ready), Some(&next(&ready, StageState::Retired)))
        .unwrap();
}
