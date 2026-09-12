// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use crowdb_chunk_kv_server::{
    CatalogPublisher, CatalogStore, DomainMonitorRegistry, Group0ControlStore, Group0Kv, Group0KvError,
    VersionedValue,
};
use crowdb_protocol::chunk_kv::{
    CatalogEntry, CatalogHead, CatalogPage, CatalogPageRef, CatalogPartitionState, DomainFailurePolicy,
    DomainMonitorDescriptor, EnsureDomainMonitorOutcome, EnsureDomainMonitorRequest, Id128, KeyRange,
    OwnerDescriptor, PartitionArtifact, ServingAssignment, ServingGrant,
};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::key::{ChunkKvCatalogHeadKey, ServingGrantKey, TextKey};
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

fn page(generation: u64) -> CatalogPage {
    let mut page = CatalogPage {
        generation,
        page_index: 0,
        entries: vec![CatalogEntry {
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
            state: CatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: 1,
                stream_name: StreamName { high: 2, low: 3 },
            },
            transition_id: None,
        }],
        checksum: [0; 32],
    };
    page.seal().unwrap();
    page
}

fn head(generation: u64, previous_generation: Option<u64>, page: &CatalogPage) -> CatalogHead {
    let mut head = CatalogHead {
        generation,
        previous_generation,
        pages: vec![CatalogPageRef {
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
    }
}

#[tokio::test]
async fn group0_catalog_reconciles_an_ambiguous_committed_head() {
    let kv = Arc::new(TestKv::default());
    let store = Arc::new(Group0ControlStore::new(kv.clone()));
    let publisher = CatalogPublisher::new(store.clone());
    let catalog_page = page(1);
    let catalog_head = head(1, None, &catalog_page);
    kv.inject(InjectedPut {
        path: ChunkKvCatalogHeadKey.to_path(),
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
