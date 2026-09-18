// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv_server::{
    ChunkKvRangeCatalogCutover, ChunkKvRangeCatalogError, ChunkKvRangeCatalogPublisher,
    MemoryChunkKvRangeCatalogStore,
};
use crowdb_protocol::chunk_kv::{
    AuthorityReleaseProof, ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage,
    ChunkKvRangeCatalogPageRef, ChunkKvRangeCatalogPartitionState, Id128, KeyRange, OwnerDescriptor,
    PartitionArtifact, SplitChildAssignment, SplitPhase, SplitReadinessProof, SplitTransition,
    TailOverlayArtifact, TargetReadinessProof, TransferPhase, TransferReadinessLimits, TransferTransition,
};
use crowdb_protocol::chunk_stream::StreamName;

fn id(low: u64) -> Id128 {
    Id128 { high: 1, low }
}

fn owner(instance_id: u64) -> OwnerDescriptor {
    OwnerDescriptor {
        instance_id,
        rpc_endpoint: format!("127.0.0.1:{}", 9000 + instance_id),
    }
}

fn artifact(tree_id: u64) -> PartitionArtifact {
    PartitionArtifact {
        tree_id,
        stream_name: StreamName {
            high: 7,
            low: tree_id,
        },
        tail_overlay: None,
    }
}

fn entry(
    partition_id: Id128,
    start: &[u8],
    end: Option<&[u8]>,
    owner: OwnerDescriptor,
    owner_epoch: u64,
    artifact: PartitionArtifact,
) -> ChunkKvRangeCatalogEntry {
    ChunkKvRangeCatalogEntry {
        partition_id,
        range: KeyRange {
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
        },
        owner,
        owner_epoch,
        state: ChunkKvRangeCatalogPartitionState::Serving,
        artifact,
        transition_id: None,
    }
}

fn page(generation: u64, page_index: u64, entries: Vec<ChunkKvRangeCatalogEntry>) -> ChunkKvRangeCatalogPage {
    let mut page = ChunkKvRangeCatalogPage {
        generation,
        page_index,
        entries,
        checksum: [0; 32],
    };
    page.seal().unwrap();
    page
}

fn head(generation: u64, pages: &[ChunkKvRangeCatalogPage]) -> ChunkKvRangeCatalogHead {
    let mut head = ChunkKvRangeCatalogHead {
        generation,
        previous_generation: None,
        pages: pages
            .iter()
            .map(|page| ChunkKvRangeCatalogPageRef {
                page_generation: page.generation,
                page_index: page.page_index,
                first_key: page.entries[0].range.start.clone(),
                page_checksum: page.checksum,
            })
            .collect(),
        checksum: [0; 32],
    };
    head.seal().unwrap();
    head
}

async fn seeded_catalog() -> (
    Arc<MemoryChunkKvRangeCatalogStore>,
    ChunkKvRangeCatalogHead,
    Vec<ChunkKvRangeCatalogPage>,
) {
    let first = page(
        1,
        0,
        vec![entry(id(1), b"", Some(b"m"), owner(1), 3, artifact(11))],
    );
    let second = page(1, 1, vec![entry(id(2), b"m", None, owner(2), 4, artifact(12))]);
    let pages = vec![first, second];
    let head = head(1, &pages);
    let store = Arc::new(MemoryChunkKvRangeCatalogStore::default());
    ChunkKvRangeCatalogPublisher::new(store.clone())
        .publish(head.clone(), pages.clone())
        .await
        .unwrap();
    (store, head, pages)
}

fn prepared_transfer() -> TransferTransition {
    let source_artifact = artifact(11);
    let mut target_artifact = source_artifact.clone();
    target_artifact.stream_name = StreamName { high: 5, low: 15 };
    target_artifact.tail_overlay = Some(TailOverlayArtifact {
        source_partition_id: id(1),
        source_epoch: 3,
        source_stream_name: source_artifact.stream_name,
        source_stream_manifest_generation: 1,
        replay_offset: 0,
        cutover_offset: 40,
        base_root_manifest_generation: 1,
        base_tree_manifest: 1,
        base_applied_seq: 0,
        cutover_seq: 40,
        target_stream_start_seq: 41,
    });
    TransferTransition {
        transition_id: id(91),
        partition_id: id(1),
        range: KeyRange {
            start: Vec::new(),
            end: Some(b"m".to_vec()),
        },
        source: owner(1),
        source_epoch: 3,
        target: owner(3),
        target_epoch: 4,
        artifact: source_artifact,
        target_artifact: target_artifact.clone(),
        readiness_limits: TransferReadinessLimits {
            max_tail_records: 100,
            max_tail_bytes: 1_000_000,
            max_estimated_catchup_ms: 1_000,
            prepare_deadline_ms: 1_000,
            forwarding_grace_ms: 1_000,
        },
        planned_at_ms: 0,
        old_grant_expires_at_ms: 100,
        phase: TransferPhase::TargetReady,
        release_proof: Some(AuthorityReleaseProof::ExplicitFence {
            source_instance_id: 1,
            source_epoch: 3,
            durable_tail: 40,
            durable_tail_offset: 40,
        }),
        readiness_proof: Some(TargetReadinessProof {
            target_instance_id: 3,
            target_epoch: 4,
            artifact: target_artifact.clone(),
            durable_tail: 40,
        }),
        catchup_proof: Some(TargetReadinessProof {
            target_instance_id: 3,
            target_epoch: 4,
            artifact: target_artifact,
            durable_tail: 40,
        }),
        failure: None,
    }
}

#[tokio::test]
async fn transfer_rewrites_only_its_page_and_reconciles_committed_retry() {
    let (store, old_head, _) = seeded_catalog().await;
    let cutover = ChunkKvRangeCatalogCutover::new(store.clone());
    let transition = prepared_transfer();
    let mut catching_up = transition.clone();
    catching_up.phase = TransferPhase::TargetCatchingUp;
    catching_up.catchup_proof = None;

    assert_eq!(cutover.publish_transfer(&catching_up).await.unwrap(), 2);
    assert_eq!(cutover.publish_transfer(&transition).await.unwrap(), 3);
    let (new_head, pages) = ChunkKvRangeCatalogPublisher::new(store.clone())
        .load_current()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(new_head.previous_generation, Some(2));
    assert_eq!(new_head.pages[1], old_head.pages[1]);
    assert_eq!(new_head.pages[0].page_generation, 3);
    assert_eq!(pages[0].entries[0].owner, owner(3));
    assert_eq!(pages[0].entries[0].owner_epoch, 4);
    assert_eq!(pages[0].entries[0].artifact, transition.target_artifact);
    assert_eq!(pages[0].entries[0].transition_id, Some(id(91)));

    assert_eq!(cutover.publish_transfer(&transition).await.unwrap(), 3);
    assert_eq!(store.write_counts().await, (4, 3));
}

fn prepared_split() -> SplitTransition {
    let overlay = TailOverlayArtifact {
        source_partition_id: id(1),
        source_epoch: 3,
        source_stream_name: artifact(11).stream_name,
        source_stream_manifest_generation: 1,
        replay_offset: 0,
        cutover_offset: 55,
        base_root_manifest_generation: 1,
        base_tree_manifest: 1,
        base_applied_seq: 55,
        cutover_seq: 55,
        target_stream_start_seq: 56,
    };
    let mut retained_parent_artifact = artifact(12);
    retained_parent_artifact.tail_overlay = Some(overlay.clone());
    let mut child_artifact = artifact(14);
    child_artifact.tail_overlay = Some(overlay.clone());
    SplitTransition {
        transition_id: id(92),
        parent_id: id(1),
        parent_range: KeyRange {
            start: Vec::new(),
            end: Some(b"m".to_vec()),
        },
        parent_owner: owner(1),
        parent_epoch: 3,
        parent_artifact: artifact(11),
        retained_parent_artifact: retained_parent_artifact.clone(),
        parent_next_epoch: 4,
        split_key: b"g".to_vec(),
        child: SplitChildAssignment {
            partition_id: id(4),
            range: KeyRange {
                start: b"g".to_vec(),
                end: Some(b"m".to_vec()),
            },
            owner: owner(1),
            owner_epoch: 4,
            artifact: child_artifact,
        },
        planned_at_ms: 0,
        phase: SplitPhase::ChildPrepared,
        readiness_proof: Some(SplitReadinessProof {
            cutover_seq: 55,
            parent_next_epoch: 4,
            retained_parent_artifact,
            retained_parent_tree_manifest: 1,
            retained_parent_root_manifest_generation: 1,
            retained_parent_applied_seq: 55,
            child_applied_seq: 55,
            child_tree_manifest: 1,
            child_root_manifest_generation: 1,
            retained_parent_tail_overlay: overlay.clone(),
            child_tail_overlay: overlay,
        }),
        failure: None,
    }
}

#[tokio::test]
async fn split_shrinks_parent_and_adds_exact_child_in_one_generation() {
    let (store, old_head, _) = seeded_catalog().await;
    let cutover = ChunkKvRangeCatalogCutover::new(store.clone());
    let transition = prepared_split();

    assert_eq!(cutover.publish_split(&transition).await.unwrap(), 2);
    let (new_head, pages) = ChunkKvRangeCatalogPublisher::new(store.clone())
        .load_current()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(new_head.pages[1], old_head.pages[1]);
    assert_eq!(pages[0].entries.len(), 2);
    assert_eq!(pages[0].entries[0].range.end.as_deref(), Some(b"g".as_slice()));
    assert_eq!(pages[0].entries[1].range.start, b"g");
    assert!(pages[0]
        .entries
        .iter()
        .all(|entry| entry.transition_id == Some(id(92))));

    assert_eq!(cutover.publish_split(&transition).await.unwrap(), 2);
}

#[tokio::test]
async fn cutover_rejects_unprepared_or_stale_transition() {
    let (store, _, _) = seeded_catalog().await;
    let cutover = ChunkKvRangeCatalogCutover::new(store);
    let mut transition = prepared_transfer();
    transition.phase = TransferPhase::TargetPreparing;
    transition.release_proof = None;
    transition.readiness_proof = None;
    transition.catchup_proof = None;
    assert_eq!(
        cutover.publish_transfer(&transition).await,
        Err(ChunkKvRangeCatalogError::TransitionNotReady)
    );

    let mut stale = prepared_transfer();
    stale.source = owner(4);
    stale.release_proof = Some(AuthorityReleaseProof::ExplicitFence {
        source_instance_id: 4,
        source_epoch: 3,
        durable_tail: 40,
        durable_tail_offset: 40,
    });
    assert_eq!(
        cutover.publish_transfer(&stale).await,
        Err(ChunkKvRangeCatalogError::TransitionConflict)
    );
}
