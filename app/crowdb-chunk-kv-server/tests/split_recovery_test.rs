// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, Checkpoint, MutationOperation, Partition, PartitionConfig, PartitionId,
    PartitionLifecycle, PartitionRange, PartitionTree, PreparedSplitWriterArtifact, StreamPartitionJournal,
};
use crowdb_chunk_kv_server::ChunkKvService;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamBinding, StreamBindingState, StreamConfig, StreamName,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, Id128, KeyRange, OwnerDescriptor, PartitionArtifact,
    SplitChildAssignment, SplitPhase, SplitReadinessProof, SplitTransition, TailOverlayArtifact,
};
use std::sync::Arc;

#[tokio::test]
async fn committed_split_proves_both_recovered_writers_and_rejects_mismatches() {
    let transition = transition();
    transition.validate().unwrap();
    for retained in [true, false] {
        let entry = entry(&transition, retained);
        let partition = recovered(&entry).await;
        let service = ChunkKvService::new(1, 4).unwrap();
        install(&service, &entry, &partition);
        assert!(service
            .activate_recovered_partition(entry.partition_id, 2)
            .is_err());
        let mut uncommitted = transition.clone();
        uncommitted.phase = SplitPhase::ChildPrepared;
        assert!(service
            .activate_recovered_split_partition(entry.partition_id, 2, &uncommitted)
            .is_err());
        let mut wrong = transition.clone();
        wrong.transition_id.low += 1;
        assert!(service
            .activate_recovered_split_partition(entry.partition_id, 2, &wrong)
            .is_err());
        let mut wrong_owner = transition.clone();
        if retained {
            wrong_owner.parent_owner.instance_id = 2;
        } else {
            wrong_owner.child.owner.instance_id = 2;
        }
        wrong_owner.validate().unwrap();
        assert!(service
            .activate_recovered_split_partition(entry.partition_id, 2, &wrong_owner)
            .is_err());
        let mut wrong_artifact = transition.clone();
        if retained {
            wrong_artifact.retained_parent_artifact.tree_id += 1;
            wrong_artifact
                .readiness_proof
                .as_mut()
                .unwrap()
                .retained_parent_artifact = wrong_artifact.retained_parent_artifact.clone();
        } else {
            wrong_artifact.child.artifact.tree_id += 1;
        }
        wrong_artifact.validate().unwrap();
        assert!(service
            .activate_recovered_split_partition(entry.partition_id, 2, &wrong_artifact)
            .is_err());
        assert!(service
            .activate_recovered_split_partition(entry.partition_id, 1, &transition)
            .is_err());
        assert_eq!(partition.lifecycle(), PartitionLifecycle::Prepared);
        service
            .activate_recovered_split_partition(entry.partition_id, 2, &transition)
            .unwrap();
        assert_eq!(partition.lifecycle(), PartitionLifecycle::Serving);
        service
            .activate_recovered_split_partition(entry.partition_id, 2, &transition)
            .unwrap();
    }
}

#[tokio::test]
async fn materialized_split_half_recovers_while_sibling_keeps_the_transition_marker() {
    let transition = transition();
    for retained in [true, false] {
        let mut clean = entry(&transition, retained);
        clean.artifact.tail_overlay = None;
        let partition = recovered(&clean).await;
        let service = ChunkKvService::new(1, 4).unwrap();
        install(&service, &clean, &partition);
        assert_eq!(service.catalog_overlay_transition_id(clean.partition_id), None);
        let sibling = entry(&transition, !retained);
        assert_eq!(
            service.catalog_overlay_transition_id(sibling.partition_id),
            Some(transition.transition_id),
        );
        service
            .activate_recovered_partition(clean.partition_id, clean.owner_epoch)
            .unwrap();
        assert_eq!(partition.lifecycle(), PartitionLifecycle::Serving);
        assert!(service
            .activate_recovered_partition(clean.partition_id, clean.owner_epoch - 1)
            .is_err());
    }
}

#[tokio::test]
async fn materialized_catalog_replaces_unactivated_overlay_with_independent_recovery() {
    let transition = transition();
    let original = entry(&transition, true);
    let overlay = recovered(&original).await;
    let service = ChunkKvService::new(1, 4).unwrap();
    install(&service, &original, &overlay);
    let mut clean = original;
    clean.artifact.tail_overlay = None;
    clean.transition_id = None;
    assert!(
        !service.hosts_catalog_assignment(&clean),
        "a prepared overlay cannot serve a materialized catalog without independent recovery"
    );
    let mut page = ChunkKvRangeCatalogPage {
        generation: 2,
        page_index: 0,
        checksum: [0; 32],
        entries: vec![clean.clone()],
    };
    let plain = recovered(&clean).await;
    service
        .reconcile_partitions(std::slice::from_ref(&page), std::slice::from_ref(&plain))
        .unwrap();
    let mut sibling = entry(&transition, false);
    sibling.artifact.tail_overlay = None;
    sibling.transition_id = None;
    page.entries.push(sibling);
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation: 2,
        previous_generation: Some(1),
        checksum: [0; 32],
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 2,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
    };
    head.seal().unwrap();
    service.install_catalog(&head, &[page]).unwrap();
    service
        .activate_recovered_partition(clean.partition_id, 2)
        .unwrap();
    assert_eq!(plain.lifecycle(), PartitionLifecycle::Serving);
    assert_eq!(overlay.lifecycle(), PartitionLifecycle::Prepared);
}

fn transition() -> SplitTransition {
    let parent_id = Id128 { high: 1, low: 1 };
    let overlay = TailOverlayArtifact {
        source_partition_id: parent_id,
        source_epoch: 1,
        source_stream_name: StreamName { high: 1, low: 1 },
        source_stream_manifest_generation: 1,
        replay_offset: 0,
        cutover_offset: 0,
        base_root_manifest_generation: 1,
        base_tree_manifest: 1,
        base_applied_seq: 1,
        cutover_seq: 1,
        target_stream_start_seq: 2,
    };
    let retained = PartitionArtifact {
        tree_id: 2,
        stream_name: StreamName { high: 1, low: 2 },
        tail_overlay: Some(overlay.clone()),
    };
    let child = PartitionArtifact {
        tree_id: 3,
        stream_name: StreamName { high: 1, low: 3 },
        tail_overlay: Some(overlay.clone()),
    };
    let owner = OwnerDescriptor {
        instance_id: 1,
        rpc_endpoint: "127.0.0.1:12000".into(),
    };
    SplitTransition {
        transition_id: Id128 { high: 9, low: 9 },
        parent_id,
        parent_range: KeyRange {
            start: Vec::new(),
            end: None,
        },
        parent_owner: owner.clone(),
        parent_epoch: 1,
        parent_artifact: PartitionArtifact {
            tree_id: 1,
            stream_name: overlay.source_stream_name,
            tail_overlay: None,
        },
        retained_parent_artifact: retained.clone(),
        parent_next_epoch: 2,
        split_key: b"m".to_vec(),
        child: SplitChildAssignment {
            partition_id: Id128 { high: 1, low: 2 },
            range: KeyRange {
                start: b"m".to_vec(),
                end: None,
            },
            owner,
            owner_epoch: 2,
            artifact: child,
        },
        planned_at_ms: 0,
        phase: SplitPhase::CatalogCommitted,
        handoff_proof: None,
        readiness_proof: Some(SplitReadinessProof {
            cutover_seq: 1,
            parent_next_epoch: 2,
            retained_parent_artifact: retained,
            retained_parent_tree_manifest: 1,
            retained_parent_root_manifest_generation: 1,
            retained_parent_applied_seq: 1,
            child_applied_seq: 1,
            child_tree_manifest: 1,
            child_root_manifest_generation: 1,
            retained_parent_tail_overlay: Some(overlay.clone()),
            child_tail_overlay: overlay,
        }),
        failure: None,
    }
}

fn entry(transition: &SplitTransition, retained: bool) -> ChunkKvRangeCatalogEntry {
    ChunkKvRangeCatalogEntry {
        partition_id: if retained {
            transition.parent_id
        } else {
            transition.child.partition_id
        },
        range: if retained {
            KeyRange {
                start: Vec::new(),
                end: Some(transition.split_key.clone()),
            }
        } else {
            transition.child.range.clone()
        },
        owner: transition.parent_owner.clone(),
        owner_epoch: 2,
        state: ChunkKvRangeCatalogPartitionState::Serving,
        artifact: if retained {
            transition.retained_parent_artifact.clone()
        } else {
            transition.child.artifact.clone()
        },
        transition_id: Some(transition.transition_id),
    }
}

async fn recovered(entry: &ChunkKvRangeCatalogEntry) -> Partition {
    let store = Arc::new(MemoryStreamStore::new(4096));
    let name = entry.artifact.stream_name;
    let stream = ChunkStream::create(
        StreamBinding {
            purpose: crowdb_protocol::chunk_stream::StreamPurpose::Wal,
            stream_name: name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        },
        2,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store,
    )
    .await
    .unwrap();
    let tree = Arc::new(MemoryPartitionTree::with_tree_id(entry.artifact.tree_id));
    tree.apply(
        1,
        &MutationOperation::Put {
            key: entry.range.start.clone(),
            value: vec![1],
        },
    )
    .await
    .unwrap();
    let Some(overlay) = entry.artifact.tail_overlay.as_ref() else {
        return recover_plain(
            entry,
            tree,
            Arc::new(StreamPartitionJournal::new(stream, name).unwrap()),
        )
        .await;
    };
    Partition::recover_prepared(
        PreparedSplitWriterArtifact {
            partition_id: PartitionId {
                high: entry.partition_id.high,
                low: entry.partition_id.low,
            },
            range: PartitionRange {
                start: Some(entry.range.start.clone()),
                end: entry.range.end.clone(),
            },
            ownership_epoch: 2,
            tree_id: entry.artifact.tree_id,
            tree_manifest: 1,
            root_manifest_generation: 1,
            stream_name: name,
            base_applied_seq: 1,
            parent_id: PartitionId {
                high: overlay.source_partition_id.high,
                low: overlay.source_partition_id.low,
            },
            parent_epoch: 1,
            parent_stream_name: overlay.source_stream_name,
            parent_stream_manifest_generation: 1,
            parent_replay_offset: 0,
            parent_cutover_offset: 0,
            applied_seq: 1,
            child_stream_start_seq: 2,
        },
        Checkpoint {
            tree_id: entry.artifact.tree_id,
            tree_manifest: 1,
            root_manifest_generation: 1,
            applied_seq: 1,
            stream_name: name,
            stream_manifest_generation: 1,
            replay_offset: 0,
        },
        PartitionConfig::default(),
        tree,
        Arc::new(StreamPartitionJournal::new(stream, name).unwrap()),
    )
    .await
    .unwrap()
}

async fn recover_plain(
    entry: &ChunkKvRangeCatalogEntry,
    tree: Arc<MemoryPartitionTree>,
    journal: Arc<StreamPartitionJournal>,
) -> Partition {
    Partition::recover_prepared_assignment(
        PartitionId {
            high: entry.partition_id.high,
            low: entry.partition_id.low,
        },
        PartitionRange {
            start: Some(entry.range.start.clone()),
            end: entry.range.end.clone(),
        },
        entry.owner_epoch,
        Checkpoint {
            tree_id: entry.artifact.tree_id,
            tree_manifest: 1,
            root_manifest_generation: 1,
            applied_seq: 1,
            stream_name: entry.artifact.stream_name,
            stream_manifest_generation: 1,
            replay_offset: 0,
        },
        PartitionConfig::default(),
        tree,
        journal,
    )
    .await
    .unwrap()
}

fn install(service: &ChunkKvService, entry: &ChunkKvRangeCatalogEntry, partition: &Partition) {
    // Include its sibling so the published catalog has complete range coverage.
    let transition = transition();
    let mut page = ChunkKvRangeCatalogPage {
        generation: 1,
        page_index: 0,
        entries: vec![self::entry(&transition, true), self::entry(&transition, false)],
        checksum: [0; 32],
    };
    *page
        .entries
        .iter_mut()
        .find(|current| current.partition_id == entry.partition_id)
        .unwrap() = entry.clone();
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation: 1,
        previous_generation: None,
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 1,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    assert!(page.entries.contains(entry));
    service.install_catalog(&head, &[page]).unwrap();
    service.install_partition(partition).unwrap();
}
