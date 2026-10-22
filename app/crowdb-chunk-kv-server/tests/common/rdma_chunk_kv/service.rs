// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_chunk_kv::{
    CrowdbPartitionTree, Partition, PartitionConfig, PartitionId, PartitionJournal, PartitionRange,
    PartitionTree, StreamPartitionJournal,
};
use crowdb_chunk_kv_server::ChunkKvService;
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig, StreamMetadataStore,
    StreamName, StreamRegistry,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeBalancePolicy, ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage,
    ChunkKvRangeCatalogPageRef, ChunkKvRangeCatalogPartitionState, DomainFailurePolicy,
    DomainMonitorDescriptor, Id128, KeyRange, OwnerDescriptor, PartitionArtifact, ServingAssignment,
    ServingGrant,
};

pub(super) const PARTITION: Id128 = Id128 { high: 1, low: 2 };
pub(super) const EPOCH: u64 = 4;

pub(super) async fn create(endpoint: &str) -> Arc<ChunkKvService> {
    let stream_name = StreamName { high: 30, low: 31 };
    let store = Arc::new(MemoryStreamStore::new(32 << 20));
    let stream = ChunkStream::create(
        StreamBinding {
            purpose: crowdb_protocol::chunk_stream::StreamPurpose::Wal,
            stream_name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        },
        EPOCH,
        StreamConfig::default(),
        store.clone() as Arc<dyn StreamRegistry>,
        store.clone() as Arc<dyn StreamMetadataStore>,
        store as Arc<dyn StreamChunkStore>,
    )
    .await
    .unwrap();
    let tree: Arc<dyn PartitionTree> = Arc::new(
        CrowdbPartitionTree::open(
            1,
            &crowdb_tree_ffi::Config {
                buffer_pool_bytes: 64 << 20,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let journal: Arc<dyn PartitionJournal> =
        Arc::new(StreamPartitionJournal::new(stream, stream_name).unwrap());
    let partition = Partition::open(
        PartitionId { high: 1, low: 2 },
        PartitionRange {
            start: Some(Vec::new()),
            end: None,
        },
        EPOCH,
        PartitionConfig::default(),
        tree,
        journal,
    )
    .unwrap();
    let service = Arc::new(ChunkKvService::new(7, 8).unwrap());
    let (head, page) = catalog(endpoint, stream_name);
    service
        .install_catalog_and_reconcile(&head, &[page], &[partition])
        .unwrap();
    renew(&service, 1);
    service
}

fn catalog(endpoint: &str, stream_name: StreamName) -> (ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage) {
    let mut page = ChunkKvRangeCatalogPage {
        generation: 1,
        page_index: 0,
        entries: vec![ChunkKvRangeCatalogEntry {
            partition_id: PARTITION,
            range: KeyRange {
                start: Vec::new(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: 7,
                rpc_endpoint: endpoint.into(),
            },
            owner_epoch: EPOCH,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: 1,
                stream_name,
                tail_overlay: None,
            },
            transition_id: None,
        }],
        checksum: [0; 32],
    };
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
    (head, page)
}

pub(super) fn renew(service: &ChunkKvService, sequence: u64) {
    let policy = DomainMonitorDescriptor {
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
        chunk_kv_range_balance: Some(ChunkKvRangeBalancePolicy::default()),
    };
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap();
    let mut grant = ServingGrant {
        instance_id: 7,
        lease_sequence: sequence,
        catalog_generation: 1,
        issued_at_ms: now,
        expires_at_ms: now + policy.lease_duration_ms,
        assignments: vec![ServingAssignment {
            partition_id: PARTITION,
            owner_epoch: EPOCH,
        }],
        assignment_digest: [0; 32],
    };
    grant.seal();
    service
        .authority()
        .install(grant, &policy, now, service.monotonic_ms())
        .unwrap();
}
