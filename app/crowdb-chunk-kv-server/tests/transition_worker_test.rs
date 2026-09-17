// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use crowdb_chunk_kv::memory::MemoryPartitionTree;
use crowdb_chunk_kv::{
    Checkpoint, Partition, PartitionConfig, PartitionId, PartitionJournal, PartitionLifecycle,
    PartitionRange, PartitionTree, PreparedChildArtifact, SplitArtifact, StreamPartitionJournal,
    TransitionId,
};
use crowdb_chunk_kv_server::{
    ChunkKvService, Group0ControlStore, Group0Kv, Group0KvError, MonitorError, PreparedLocalSplit,
    TransitionExecutor, TransitionProcessor, TransitionStorage, VersionedValue,
};
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig, StreamMetadataStore,
    StreamName, StreamRegistry,
};
use crowdb_protocol::chunk_kv::{
    AuthorityReleaseProof, ChunkKvRangeCatalogEntry, Id128, KeyRange, OwnerDescriptor, PartitionArtifact,
    SplitChildAssignment, SplitPhase, SplitTransition, TargetReadinessProof, TransferPhase,
    TransferReadinessLimits, TransferTransition,
};
use tokio::sync::Mutex;

#[derive(Default)]
struct MemoryKv {
    values: Mutex<HashMap<Vec<u8>, VersionedValue>>,
}

#[async_trait]
impl Group0Kv for MemoryKv {
    async fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>, Group0KvError> {
        Ok(self.values.lock().await.get(key).cloned())
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, Group0KvError> {
        let values = self.values.lock().await;
        let mut found = values
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        found.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(found)
    }

    async fn put_cas(&self, key: &[u8], value: &[u8], expected_revision: u64) -> Result<(), Group0KvError> {
        let mut values = self.values.lock().await;
        let current_revision = values.get(key).map_or(0, |value| value.revision);
        if current_revision != expected_revision {
            return Err(Group0KvError::CasFailed { current_revision });
        }
        let revision = current_revision.saturating_add(1);
        values.insert(
            key.to_vec(),
            VersionedValue {
                value: value.to_vec(),
                revision,
            },
        );
        Ok(())
    }
}

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
            high: 5,
            low: tree_id,
        },
        tail_overlay: None,
    }
}

async fn partition(
    partition_id: Id128,
    range: KeyRange,
    epoch: u64,
    artifact: &PartitionArtifact,
    prepared: bool,
) -> Partition {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let stream = ChunkStream::create(
        StreamBinding {
            stream_name: artifact.stream_name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        },
        epoch,
        StreamConfig::default(),
        store.clone() as Arc<dyn StreamRegistry>,
        store.clone() as Arc<dyn StreamMetadataStore>,
        store as Arc<dyn StreamChunkStore>,
    )
    .await
    .unwrap();
    let tree: Arc<dyn PartitionTree> = Arc::new(MemoryPartitionTree::with_tree_id(artifact.tree_id));
    let journal: Arc<dyn PartitionJournal> =
        Arc::new(StreamPartitionJournal::new(stream, artifact.stream_name));
    let partition_range = PartitionRange {
        start: Some(range.start),
        end: range.end,
    };
    if prepared {
        Partition::recover_prepared_assignment(
            PartitionId {
                high: partition_id.high,
                low: partition_id.low,
            },
            partition_range,
            epoch,
            Checkpoint {
                tree_id: artifact.tree_id,
                tree_manifest: 0,
                applied_seq: 0,
                stream_name: artifact.stream_name,
                stream_manifest_generation: 1,
                replay_offset: 0,
            },
            PartitionConfig::default(),
            tree,
            journal,
        )
        .await
        .unwrap()
    } else {
        Partition::open(
            PartitionId {
                high: partition_id.high,
                low: partition_id.low,
            },
            partition_range,
            epoch,
            PartitionConfig::default(),
            tree,
            journal,
        )
        .unwrap()
    }
}

struct FakeStorage {
    recovered: Partition,
    split: SplitArtifact,
}

#[async_trait]
impl TransitionStorage for FakeStorage {
    async fn recover_partition(&self, _entry: &ChunkKvRangeCatalogEntry) -> Result<Partition, MonitorError> {
        Ok(self.recovered.clone())
    }

    async fn prepare_transfer_source(
        &self,
        _source: &Partition,
        transition: &TransferTransition,
    ) -> Result<PartitionArtifact, MonitorError> {
        Ok(transition.target_artifact.clone())
    }

    async fn prepare_split(
        &self,
        _parent: &Partition,
        transition: &SplitTransition,
        _max_fence_lag_records: u64,
    ) -> Result<PreparedLocalSplit, MonitorError> {
        let left = partition(
            transition.left.partition_id,
            transition.left.range.clone(),
            transition.left.owner_epoch,
            &transition.left.artifact,
            true,
        )
        .await;
        let right = partition(
            transition.right.partition_id,
            transition.right.range.clone(),
            transition.right.owner_epoch,
            &transition.right.artifact,
            true,
        )
        .await;
        Ok(PreparedLocalSplit {
            artifact: self.split.clone(),
            children: vec![left, right],
        })
    }
}

fn transfer(phase: TransferPhase) -> TransferTransition {
    let source_artifact = artifact(11);
    let mut target_artifact = source_artifact.clone();
    target_artifact.stream_name = StreamName { high: 5, low: 12 };
    target_artifact.tail_overlay = Some(crowdb_protocol::chunk_kv::TailOverlayArtifact {
        source_partition_id: id(1),
        source_epoch: 3,
        source_stream_name: source_artifact.stream_name,
        source_stream_manifest_generation: 1,
        replay_offset: 0,
        cutover_offset: 0,
        base_tree_manifest: 1,
        base_applied_seq: 0,
        cutover_seq: 0,
        target_stream_start_seq: 1,
    });
    TransferTransition {
        transition_id: id(90),
        partition_id: id(1),
        range: KeyRange {
            start: Vec::new(),
            end: None,
        },
        source: owner(1),
        source_epoch: 3,
        target: owner(2),
        target_epoch: 4,
        artifact: source_artifact,
        target_artifact,
        readiness_limits: TransferReadinessLimits {
            max_tail_records: 100,
            max_tail_bytes: 1_000_000,
            max_estimated_catchup_ms: 1_000,
            prepare_deadline_ms: 1_000,
            forwarding_grace_ms: 1_000,
        },
        planned_at_ms: 0,
        old_grant_expires_at_ms: 100,
        phase,
        release_proof: None,
        readiness_proof: None,
        catchup_proof: None,
        failure: None,
    }
}

fn split_transition() -> SplitTransition {
    SplitTransition {
        transition_id: id(91),
        parent_id: id(1),
        parent_range: KeyRange {
            start: Vec::new(),
            end: None,
        },
        parent_owner: owner(1),
        parent_epoch: 3,
        parent_artifact: artifact(11),
        split_key: b"m".to_vec(),
        left: SplitChildAssignment {
            partition_id: id(2),
            range: KeyRange {
                start: Vec::new(),
                end: Some(b"m".to_vec()),
            },
            owner: owner(1),
            owner_epoch: 4,
            artifact: artifact(12),
        },
        right: SplitChildAssignment {
            partition_id: id(3),
            range: KeyRange {
                start: b"m".to_vec(),
                end: None,
            },
            owner: owner(2),
            owner_epoch: 1,
            artifact: artifact(13),
        },
        planned_at_ms: 0,
        phase: SplitPhase::ParentPreparing,
        readiness_proof: None,
        failure: None,
    }
}

fn split_artifact(transition: &SplitTransition) -> SplitArtifact {
    let child = |assignment: &SplitChildAssignment| PreparedChildArtifact {
        partition_id: PartitionId {
            high: assignment.partition_id.high,
            low: assignment.partition_id.low,
        },
        range: PartitionRange {
            start: Some(assignment.range.start.clone()),
            end: assignment.range.end.clone(),
        },
        ownership_epoch: assignment.owner_epoch,
        tree_id: assignment.artifact.tree_id,
        tree_manifest: 2,
        stream_name: assignment.artifact.stream_name,
        base_applied_seq: 8,
        parent_id: PartitionId {
            high: transition.parent_id.high,
            low: transition.parent_id.low,
        },
        parent_epoch: transition.parent_epoch,
        parent_stream_name: StreamName { high: 5, low: 11 },
        parent_stream_manifest_generation: 1,
        parent_replay_offset: 0,
        parent_cutover_offset: 8,
        applied_seq: 8,
        child_stream_start_seq: 9,
    };
    SplitArtifact {
        transition_id: TransitionId {
            high: transition.transition_id.high,
            low: transition.transition_id.low,
        },
        parent_id: PartitionId {
            high: transition.parent_id.high,
            low: transition.parent_id.low,
        },
        parent_epoch: transition.parent_epoch,
        cutover_seq: 8,
        left: child(&transition.left),
        right: child(&transition.right),
    }
}

#[tokio::test]
async fn source_worker_fences_before_returning_release_proof() {
    let parent_artifact = artifact(11);
    let source = partition(
        id(1),
        KeyRange {
            start: Vec::new(),
            end: None,
        },
        3,
        &parent_artifact,
        false,
    )
    .await;
    let unused = partition(id(9), KeyRange::default(), 1, &artifact(99), true).await;
    let service = Arc::new(ChunkKvService::new(1, 4).unwrap());
    service.install_partition(&source).unwrap();
    let worker = TransitionExecutor::with_storage(
        1,
        service,
        Arc::new(FakeStorage {
            recovered: unused,
            split: split_artifact(&split_transition()),
        }),
        8,
    )
    .unwrap();

    assert_eq!(
        worker
            .fence_transfer_source(&{
                let mut transition = transfer(TransferPhase::TargetPreparing);
                transition.phase = TransferPhase::TargetPrepared;
                transition.readiness_proof = Some(TargetReadinessProof {
                    target_instance_id: 2,
                    target_epoch: 4,
                    artifact: transition.target_artifact.clone(),
                    durable_tail: 0,
                });
                transition
            })
            .await
            .unwrap(),
        AuthorityReleaseProof::ExplicitFence {
            source_instance_id: 1,
            source_epoch: 3,
            durable_tail: 0,
            durable_tail_offset: 0,
        }
    );
    assert_eq!(source.lifecycle(), PartitionLifecycle::SplitFenced);
}

#[tokio::test]
async fn target_worker_recovers_but_does_not_activate_assignment() {
    let target_artifact = artifact(11);
    let recovered = partition(id(1), KeyRange::default(), 4, &target_artifact, true).await;
    let service = Arc::new(ChunkKvService::new(2, 4).unwrap());
    let worker = TransitionExecutor::with_storage(
        2,
        Arc::clone(&service),
        Arc::new(FakeStorage {
            recovered: recovered.clone(),
            split: split_artifact(&split_transition()),
        }),
        8,
    )
    .unwrap();

    let proof = worker
        .prepare_transfer_target(&transfer(TransferPhase::TargetPreparing))
        .await
        .unwrap();
    assert_eq!(proof.target_instance_id, 2);
    assert_eq!(proof.target_epoch, 4);
    assert_eq!(proof.durable_tail, 0);
    assert_eq!(recovered.lifecycle(), PartitionLifecycle::Prepared);
}

#[tokio::test]
async fn split_worker_reports_only_a_common_child_frontier() {
    let transition = split_transition();
    let parent = partition(
        id(1),
        transition.parent_range.clone(),
        3,
        &transition.parent_artifact,
        false,
    )
    .await;
    let unused = partition(id(9), KeyRange::default(), 1, &artifact(99), true).await;
    let service = Arc::new(ChunkKvService::new(1, 4).unwrap());
    service.install_partition(&parent).unwrap();
    let worker = TransitionExecutor::with_storage(
        1,
        service,
        Arc::new(FakeStorage {
            recovered: unused,
            split: split_artifact(&transition),
        }),
        8,
    )
    .unwrap();

    let proof = worker.prepare_split_parent(&transition).await.unwrap();
    assert_eq!(proof.cutover_seq, 8);
    assert_eq!(proof.left_applied_seq, 8);
    assert_eq!(proof.right_applied_seq, 8);
    assert_eq!(
        worker.prepare_split_parent(&transition).await.unwrap(),
        proof,
        "a durable readiness retry requires both child handles to remain installed"
    );
}

#[tokio::test]
async fn processor_persists_target_preparing_before_readiness() {
    let kv = Arc::new(MemoryKv::default());
    let store = Arc::new(Group0ControlStore::new(kv));
    let transition = transfer(TransferPhase::TargetPreparing);
    assert_eq!(
        store.persist_transfer_transition(&transition, 0).await.unwrap(),
        1
    );
    let recovered = partition(id(1), KeyRange::default(), 4, &artifact(11), true).await;
    let service = Arc::new(ChunkKvService::new(2, 4).unwrap());
    let executor = Arc::new(
        TransitionExecutor::with_storage(
            2,
            service,
            Arc::new(FakeStorage {
                recovered,
                split: split_artifact(&split_transition()),
            }),
            8,
        )
        .unwrap(),
    );
    let processor = TransitionProcessor::new(2, Arc::clone(&store), executor);

    processor.tick().await.unwrap();
    let (stored, revision) = store
        .load_transfer_transition(transition.transition_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.phase, TransferPhase::TargetPrepared);
    assert!(stored.readiness_proof.is_some());
    assert_eq!(revision, 2);
}

#[tokio::test]
async fn processor_resumes_planned_split_through_durable_readiness() {
    let kv = Arc::new(MemoryKv::default());
    let store = Arc::new(Group0ControlStore::new(kv));
    let mut transition = split_transition();
    transition.phase = SplitPhase::Planned;
    store.persist_split_transition(&transition, 0).await.unwrap();
    let parent = partition(
        id(1),
        transition.parent_range.clone(),
        3,
        &transition.parent_artifact,
        false,
    )
    .await;
    let recovered = partition(id(9), KeyRange::default(), 1, &artifact(99), true).await;
    let service = Arc::new(ChunkKvService::new(1, 4).unwrap());
    service.install_partition(&parent).unwrap();
    let executor = Arc::new(
        TransitionExecutor::with_storage(
            1,
            service,
            Arc::new(FakeStorage {
                recovered,
                split: split_artifact(&transition),
            }),
            8,
        )
        .unwrap(),
    );
    let processor = TransitionProcessor::new(1, Arc::clone(&store), executor);

    processor.tick().await.unwrap();
    let (stored, revision) = store
        .load_split_transition(transition.transition_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.phase, SplitPhase::ChildrenPrepared);
    assert_eq!(stored.readiness_proof.unwrap().cutover_seq, 8);
    assert_eq!(revision, 3);
}
