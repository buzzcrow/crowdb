// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use crowdb_chunk_kv_server::{
    ChunkKvRangeCatalogPublisher, ChunkKvRangeCatalogStore, DomainMonitorRegistry, Group0ControlStore,
    Group0Kv, Group0KvError, SplitAction, SplitStateMachine, TransferStateMachine, VersionedValue,
};
use crowdb_protocol::chunk_kv::{
    AuthorityReleaseProof, ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage,
    ChunkKvRangeCatalogPageRef, ChunkKvRangeCatalogPartitionState, DomainFailurePolicy,
    DomainMonitorDescriptor, EnsureDomainMonitorOutcome, EnsureDomainMonitorRequest, Id128, KeyRange,
    OwnerDescriptor, PartitionArtifact, ServingAssignment, ServingGrant, SplitChildAssignment, SplitPhase,
    SplitReadinessProof, SplitTransition, TailOverlayArtifact, TransferPhase, TransferTransition,
};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::key::{ChunkKvRangeCatalogHeadKey, ServingGrantKey, TextKey};
use tokio::sync::Mutex;

#[derive(Clone)]
struct InjectedPut {
    path: String,
    error: Group0KvError,
    commit: bool,
}

#[derive(Default)]
struct TestKvState {
    values: HashMap<Vec<u8>, VersionedValue>,
    next_revision: u64,
    injected: Option<InjectedPut>,
}

#[derive(Default)]
struct TestKv {
    state: Mutex<TestKvState>,
}

impl TestKv {
    async fn inject(&self, put: InjectedPut) {
        self.state.lock().await.injected = Some(put);
    }
}

#[async_trait]
impl Group0Kv for TestKv {
    async fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>, Group0KvError> {
        Ok(self.state.lock().await.values.get(key).cloned())
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, Group0KvError> {
        let state = self.state.lock().await;
        let mut values = state
            .values
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        values.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(values)
    }

    async fn put_cas(&self, key: &[u8], value: &[u8], expected_revision: u64) -> Result<(), Group0KvError> {
        let mut state = self.state.lock().await;
        let current_revision = state.values.get(key).map_or(0, |value| value.revision);
        if current_revision != expected_revision {
            return Err(Group0KvError::CasFailed { current_revision });
        }
        let injected = state
            .injected
            .take()
            .filter(|injected| injected.path.as_bytes() == key);
        let commit = injected.as_ref().map_or(true, |injected| injected.commit);
        if commit {
            state.next_revision += 1;
            let revision = state.next_revision;
            state.values.insert(
                key.to_vec(),
                VersionedValue {
                    value: value.to_vec(),
                    revision,
                },
            );
        }
        injected.map_or(Ok(()), |injected| Err(injected.error))
    }
}

fn page(generation: u64) -> ChunkKvRangeCatalogPage {
    let mut page = ChunkKvRangeCatalogPage {
        generation,
        page_index: 0,
        entries: vec![ChunkKvRangeCatalogEntry {
            partition_id: Id128 { high: 1, low: 1 },
            range: KeyRange {
                start: Vec::new(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: 8,
                rpc_endpoint: "127.0.0.1:9900".into(),
            },
            owner_epoch: generation,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: 1,
                stream_name: StreamName { high: 2, low: 3 },
                tail_overlay: None,
            },
            transition_id: None,
        }],
        checksum: [0; 32],
    };
    page.seal().unwrap();
    page
}

fn head(
    generation: u64,
    previous_generation: Option<u64>,
    page: &ChunkKvRangeCatalogPage,
) -> ChunkKvRangeCatalogHead {
    let mut head = ChunkKvRangeCatalogHead {
        generation,
        previous_generation,
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: page.generation,
            page_index: page.page_index,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    head
}

fn descriptor() -> DomainMonitorDescriptor {
    DomainMonitorDescriptor {
        domain: "chunk-kv".into(),
        service_registry_name: "chunk-kv".into(),
        driver_version: 1,
        capability_version: 1,
        heartbeat_interval_ms: 2_000,
        suspect_after_ms: 6_000,
        dead_after_ms: 10_000,
        lease_duration_ms: 12_000,
        max_clock_skew_ms: 1_000,
        self_fence_margin_ms: 1_000,
        failure_policy: DomainFailurePolicy::AutomaticSharedStorage,
        balance_policy: "count-first-v1".into(),
        chunk_kv_range_balance: Some(crowdb_protocol::chunk_kv::ChunkKvRangeBalancePolicy::default()),
    }
}

fn transfer() -> TransferTransition {
    TransferTransition {
        transition_id: Id128 { high: 9, low: 10 },
        partition_id: Id128 { high: 1, low: 2 },
        range: KeyRange {
            start: Vec::new(),
            end: None,
        },
        source: OwnerDescriptor {
            instance_id: 11,
            rpc_endpoint: "127.0.0.1:9911".into(),
        },
        source_epoch: 3,
        target: OwnerDescriptor {
            instance_id: 12,
            rpc_endpoint: "127.0.0.1:9912".into(),
        },
        target_epoch: 4,
        artifact: PartitionArtifact {
            tree_id: 5,
            stream_name: StreamName { high: 6, low: 7 },
            tail_overlay: None,
        },
        planned_at_ms: 0,
        old_grant_expires_at_ms: 10_000,
        phase: TransferPhase::Planned,
        release_proof: None,
        readiness_proof: None,
        failure: None,
    }
}

fn split() -> SplitTransition {
    SplitTransition {
        transition_id: Id128 { high: 20, low: 21 },
        parent_id: Id128 { high: 1, low: 2 },
        parent_range: KeyRange {
            start: Vec::new(),
            end: None,
        },
        parent_owner: OwnerDescriptor {
            instance_id: 11,
            rpc_endpoint: "127.0.0.1:9911".into(),
        },
        parent_epoch: 3,
        parent_artifact: PartitionArtifact {
            tree_id: 5,
            stream_name: StreamName { high: 6, low: 7 },
            tail_overlay: None,
        },
        split_key: b"m".to_vec(),
        left: SplitChildAssignment {
            partition_id: Id128 { high: 22, low: 23 },
            range: KeyRange {
                start: Vec::new(),
                end: Some(b"m".to_vec()),
            },
            owner: OwnerDescriptor {
                instance_id: 11,
                rpc_endpoint: "127.0.0.1:9911".into(),
            },
            owner_epoch: 1,
            artifact: PartitionArtifact {
                tree_id: 24,
                stream_name: StreamName { high: 25, low: 26 },
                tail_overlay: None,
            },
        },
        right: SplitChildAssignment {
            partition_id: Id128 { high: 27, low: 28 },
            range: KeyRange {
                start: b"m".to_vec(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: 12,
                rpc_endpoint: "127.0.0.1:9912".into(),
            },
            owner_epoch: 1,
            artifact: PartitionArtifact {
                tree_id: 29,
                stream_name: StreamName { high: 30, low: 31 },
                tail_overlay: None,
            },
        },
        planned_at_ms: 0,
        phase: SplitPhase::Planned,
        readiness_proof: None,
        failure: None,
    }
}

fn split_overlay(cutover_seq: u64) -> TailOverlayArtifact {
    TailOverlayArtifact {
        source_partition_id: Id128 { high: 1, low: 2 },
        source_epoch: 3,
        source_stream_name: StreamName { high: 6, low: 7 },
        source_stream_manifest_generation: 1,
        replay_offset: 0,
        cutover_offset: cutover_seq,
        base_applied_seq: cutover_seq,
        cutover_seq,
        target_stream_start_seq: cutover_seq + 1,
    }
}

#[tokio::test]
async fn group0_catalog_reconciles_an_ambiguous_committed_head() {
    let kv = Arc::new(TestKv::default());
    let store = Arc::new(Group0ControlStore::new(kv.clone()));
    let publisher = ChunkKvRangeCatalogPublisher::new(store.clone());
    let catalog_page = page(1);
    let catalog_head = head(1, None, &catalog_page);
    kv.inject(InjectedPut {
        path: ChunkKvRangeCatalogHeadKey.to_path(),
        error: Group0KvError::OutcomeUnknown,
        commit: true,
    })
    .await;

    publisher
        .publish(catalog_head.clone(), vec![catalog_page.clone()])
        .await
        .unwrap();
    assert_eq!(store.get_head().await.unwrap(), Some(catalog_head));
    assert_eq!(store.get_page(1, 0).await.unwrap(), Some(catalog_page));
}

#[tokio::test]
async fn group0_monitor_ensure_reconciles_races_and_rejects_conflicts() {
    let kv = Arc::new(TestKv::default());
    let store = Arc::new(Group0ControlStore::new(kv));
    let registry = DomainMonitorRegistry::new(
        store,
        vec![crowdb_chunk_kv_server::serving::monitor::SupportedMonitor {
            domain: "chunk-kv".into(),
            driver_version: 1,
            max_capability_version: 1,
        }],
    );
    let request = EnsureDomainMonitorRequest {
        descriptor: descriptor(),
    };
    assert_eq!(
        registry.ensure(&request).await.unwrap(),
        EnsureDomainMonitorOutcome::Created
    );
    assert_eq!(
        registry.ensure(&request).await.unwrap(),
        EnsureDomainMonitorOutcome::AlreadyExists
    );
    let mut conflict = request;
    conflict.descriptor.balance_policy = "different".into();
    assert_eq!(
        registry.ensure(&conflict).await.unwrap(),
        EnsureDomainMonitorOutcome::DescriptorConflict
    );
}

#[tokio::test]
async fn group0_serving_grant_load_rejects_invalid_authority() {
    let kv = Arc::new(TestKv::default());
    let store = Group0ControlStore::new(kv.clone());
    let mut grant = ServingGrant {
        instance_id: 7,
        lease_sequence: 2,
        catalog_generation: 3,
        issued_at_ms: 1_000,
        expires_at_ms: 13_000,
        assignments: vec![ServingAssignment {
            partition_id: Id128 { high: 4, low: 5 },
            owner_epoch: 6,
        }],
        assignment_digest: [0; 32],
    };
    grant.seal();
    let path = ServingGrantKey { instance_id: 7 }.to_path();
    kv.put_cas(path.as_bytes(), &serde_json::to_vec(&grant).unwrap(), 0)
        .await
        .unwrap();
    assert_eq!(store.load_serving_grant(7).await.unwrap(), Some(grant));

    let invalid = ServingGrant {
        assignment_digest: [9; 32],
        ..store.load_serving_grant(7).await.unwrap().unwrap()
    };
    let revision = kv.get(path.as_bytes()).await.unwrap().unwrap().revision;
    kv.put_cas(path.as_bytes(), &serde_json::to_vec(&invalid).unwrap(), revision)
        .await
        .unwrap();
    assert!(matches!(
        store.load_serving_grant(7).await,
        Err(Group0KvError::Unavailable(_))
    ));
}

#[tokio::test]
async fn group0_transfer_store_reconciles_and_resumes_exact_phase() {
    let kv = Arc::new(TestKv::default());
    let store = Group0ControlStore::new(kv.clone());
    let planned = transfer();
    let revision = store.persist_transfer_transition(&planned, 0).await.unwrap();
    assert_eq!(
        store.persist_transfer_transition(&planned, 0).await.unwrap(),
        revision
    );

    let mut machine = TransferStateMachine::restore(planned).unwrap();
    machine
        .record_source_fence(AuthorityReleaseProof::ExplicitFence {
            source_instance_id: 11,
            source_epoch: 3,
            durable_tail: 19,
        })
        .unwrap();
    machine.begin_target_prepare().unwrap();
    let preparing = machine.transition().clone();
    let path = crowdb_protocol::key::ChunkKvTransferKey {
        transition_id: preparing.transition_id,
    }
    .to_path();
    kv.inject(InjectedPut {
        path: path.clone(),
        error: Group0KvError::OutcomeUnknown,
        commit: true,
    })
    .await;
    let next_revision = store
        .persist_transfer_transition(&preparing, revision)
        .await
        .unwrap();
    assert!(next_revision > revision);

    let (loaded, loaded_revision) = store
        .load_transfer_transition(preparing.transition_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded_revision, next_revision);
    assert_eq!(loaded, preparing);
    assert_eq!(
        TransferStateMachine::restore(loaded)
            .unwrap()
            .next_action(true, 1_000),
        crowdb_chunk_kv_server::TransferAction::PrepareTarget {
            instance_id: 12,
            owner_epoch: 4,
        }
    );

    let mut conflicting = transfer();
    conflicting.old_grant_expires_at_ms += 1;
    assert!(store
        .persist_transfer_transition(&conflicting, revision)
        .await
        .is_err());

    let invalid = TransferTransition {
        phase: TransferPhase::TargetPrepared,
        ..preparing
    };
    kv.put_cas(
        path.as_bytes(),
        &serde_json::to_vec(&invalid).unwrap(),
        next_revision,
    )
    .await
    .unwrap();
    let corrupt_revision = kv.get(path.as_bytes()).await.unwrap().unwrap().revision;
    assert!(store
        .persist_transfer_transition(&transfer(), corrupt_revision)
        .await
        .is_err());
}

#[tokio::test]
async fn group0_split_store_resumes_prepared_children_before_catalog_cutover() {
    let kv = Arc::new(TestKv::default());
    let store = Group0ControlStore::new(kv.clone());
    let planned = split();
    let revision = store.persist_split_transition(&planned, 0).await.unwrap();
    let mut machine = SplitStateMachine::restore(planned).unwrap();
    machine.begin_parent_prepare().unwrap();
    machine
        .record_children_ready(SplitReadinessProof {
            cutover_seq: 41,
            left_applied_seq: 41,
            right_applied_seq: 41,
            left_tail_overlay: split_overlay(41),
            right_tail_overlay: split_overlay(41),
        })
        .unwrap();
    let prepared = machine.transition().clone();
    let path = crowdb_protocol::key::ChunkKvSplitKey {
        transition_id: prepared.transition_id,
    }
    .to_path();
    kv.inject(InjectedPut {
        path,
        error: Group0KvError::OutcomeUnknown,
        commit: true,
    })
    .await;
    let next_revision = store.persist_split_transition(&prepared, revision).await.unwrap();
    let (loaded, loaded_revision) = store
        .load_split_transition(prepared.transition_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded_revision, next_revision);
    assert_eq!(loaded, prepared);
    assert_eq!(
        SplitStateMachine::restore(loaded).unwrap().next_action(),
        SplitAction::PublishCatalog
    );
}
