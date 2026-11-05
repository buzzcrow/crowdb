// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_server::{storage::open_root_catalog_with_process_for_tests, Group0ControlStore};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, Id128, TailOverlayArtifact, TransferPhase,
    TransferReadinessLimits, TransferTransition,
};
use crowdb_tree_ffi::{
    chunk::stage::{StagePack, StageState},
    CtError,
};

#[path = "common/metadata.rs"]
mod metadata;
#[allow(dead_code)]
#[path = "common/root_process.rs"]
mod root_process;

fn preparation() -> (TransferTransition, ChunkKvRangeCatalogEntry) {
    let source = root_process::entry(1);
    let mut target = root_process::entry(2);
    target.state = ChunkKvRangeCatalogPartitionState::Prepared;
    target.transition_id = Some(Id128 { high: 3, low: 4 });
    target.artifact.stream_name.low = 3;
    target.artifact.tail_overlay = Some(TailOverlayArtifact {
        source_partition_id: source.partition_id,
        source_epoch: 1,
        source_stream_name: source.artifact.stream_name,
        source_stream_generation: 1,
        replay_offset: 0,
        cutover_offset: 0,
        base_root_manifest_generation: 1,
        base_tree_manifest: 1,
        base_applied_seq: 0,
        cutover_seq: 0,
        target_stream_start_seq: 1,
    });
    let transition = TransferTransition {
        transition_id: target.transition_id.unwrap(),
        partition_id: target.partition_id,
        range: target.range.clone(),
        source: source.owner,
        source_epoch: source.owner_epoch,
        target: target.owner.clone(),
        target_epoch: target.owner_epoch,
        artifact: source.artifact,
        target_artifact: target.artifact.clone(),
        readiness_limits: TransferReadinessLimits {
            max_tail_records: 100,
            max_tail_bytes: 1_000_000,
            max_estimated_catchup_ms: 1000,
            prepare_deadline_ms: 1000,
            forwarding_grace_ms: 1000,
        },
        planned_at_ms: 0,
        old_grant_expires_at_ms: 100,
        phase: TransferPhase::TargetPreparing,
        release_proof: None,
        readiness_proof: None,
        catchup_proof: None,
        failure: None,
    };
    (transition, target)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_preparation_stages_without_fencing_source_and_abort_stops_next_window() {
    let metadata = metadata::TestMetadata::start().await;
    let source_process = root_process::process(&metadata.client, 1).await;
    let source = open_root_catalog_with_process_for_tests(
        metadata.client.clone(),
        0,
        1,
        &root_process::entry(1),
        source_process,
    )
    .await
    .unwrap();
    source.store().publish(42, 0, 1, 1, b"source-one").unwrap();
    let (mut transition, entry) = preparation();
    let control = Group0ControlStore::from_client(metadata.client.clone());
    let revision = control.persist_transfer_transition(&transition, 0).await.unwrap();
    let target_process = root_process::process(&metadata.client, 2).await;
    let target =
        open_root_catalog_with_process_for_tests(metadata.client.clone(), 0, 1, &entry, target_process)
            .await
            .unwrap();
    let mut stage = root_process::stage();
    stage.id.owner_epoch = 2;
    target.store().compare_stage(42, 2, None, Some(&stage)).unwrap();
    source.store().publish(42, 1, 1, 2, b"source-two").unwrap();
    assert_eq!(
        target.store().publish(42, 2, 2, 3, b"premature-target"),
        Err(CtError::Unavailable)
    );
    let mut writing = root_process::next(&stage, StageState::Writing);
    writing.packs.push(StagePack {
        offset: 0,
        length: 4096,
        checksum: 1,
    });
    target
        .store()
        .compare_stage(42, 2, Some(&stage), Some(&writing))
        .unwrap();
    transition.phase = TransferPhase::Aborted;
    transition.failure = Some("preparation cancelled".into());
    control
        .persist_transfer_transition(&transition, revision)
        .await
        .unwrap();
    let reloadable = root_process::next(&writing, StageState::Reloadable);
    assert_eq!(
        target
            .store()
            .compare_stage(42, 2, Some(&writing), Some(&reloadable)),
        Err(CtError::Unavailable)
    );
    source.store().publish(42, 2, 1, 3, b"source-three").unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_requires_exact_recovery_base_and_fixed_target_process() {
    let metadata = metadata::TestMetadata::start().await;
    let source_process = root_process::process(&metadata.client, 1).await;
    let source = open_root_catalog_with_process_for_tests(
        metadata.client.clone(),
        0,
        1,
        &root_process::entry(1),
        source_process,
    )
    .await
    .unwrap();
    let (transition, entry) = preparation();
    Group0ControlStore::from_client(metadata.client.clone())
        .persist_transfer_transition(&transition, 0)
        .await
        .unwrap();
    let target_process = root_process::process(&metadata.client, 2).await;
    let mut wrong = entry.clone();
    wrong.artifact.tail_overlay.as_mut().unwrap().base_tree_manifest += 1;
    assert!(
        open_root_catalog_with_process_for_tests(metadata.client.clone(), 0, 1, &wrong, target_process,)
            .await
            .is_err()
    );
    let foreign_process = root_process::process(&metadata.client, 3).await;
    assert!(
        open_root_catalog_with_process_for_tests(metadata.client.clone(), 0, 1, &entry, foreign_process,)
            .await
            .is_err()
    );
    source.store().publish(42, 0, 1, 1, b"still-source").unwrap();
}
