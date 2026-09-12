// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv::memory::MemoryPartitionTree;
use crowdb_chunk_kv::{
    Partition, PartitionConfig, PartitionId, PartitionJournal, PartitionRange, PartitionTree,
    StreamPartitionJournal,
};
use crowdb_chunk_kv_server::{ChunkKvService, ServerLifecycle};
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig, StreamMetadataStore,
    StreamName, StreamRegistry,
};
use crowdb_protocol::chunk_kv::{
    CatalogEntry, CatalogHead, CatalogPage, CatalogPageRef, CatalogPartitionState, ChunkKvRpcErrorCode,
    ClientRequestId, DomainFailurePolicy, DomainMonitorDescriptor, Id128, KeyRange, OperationResult,
    OwnerDescriptor, PartitionArtifact, PointOperation, PointRequest, RequestRouting, ServingAssignment,
    ServingGrant,
};

const INSTANCE_ID: u64 = 7;
const EPOCH: u64 = 4;

fn policy() -> DomainMonitorDescriptor {
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

fn routing(sequence: u64) -> RequestRouting {
    RequestRouting {
        request_id: ClientRequestId {
            client_instance_id: Id128 { high: 20, low: 21 },
            client_sequence: sequence,
        },
        map_revision: 1,
        partition_id: Id128 { high: 1, low: 2 },
        owner_epoch: EPOCH,
        min_journal_position: None,
        deadline_ms: Some(2_000),
    }
}

async fn fixture() -> (ChunkKvService, Partition) {
    let stream_name = StreamName { high: 30, low: 31 };
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let stream = ChunkStream::create(
        StreamBinding {
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
    let tree: Arc<dyn PartitionTree> = Arc::new(MemoryPartitionTree::default());
    let journal: Arc<dyn PartitionJournal> = Arc::new(StreamPartitionJournal::new(stream, stream_name));
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

    let mut page = CatalogPage {
        generation: 1,
        page_index: 0,
        entries: vec![CatalogEntry {
            partition_id: Id128 { high: 1, low: 2 },
            range: KeyRange {
                start: Vec::new(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: INSTANCE_ID,
                rpc_endpoint: "127.0.0.1:9900".into(),
            },
            owner_epoch: EPOCH,
            state: CatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: 1,
                tree_manifest: 1,
                stream_name,
                applied_seq: 0,
            },
            transition_id: None,
        }],
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = CatalogHead {
        generation: 1,
        previous_generation: None,
        pages: vec![CatalogPageRef {
            page_generation: 1,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();

    let service = ChunkKvService::new(INSTANCE_ID, 8).unwrap();
    service.install_catalog(&head, &[page]).unwrap();
    service.install_partition(&partition).unwrap();
    let mut grant = ServingGrant {
        instance_id: INSTANCE_ID,
        lease_sequence: 1,
        catalog_generation: 1,
        issued_at_ms: 1_000,
        expires_at_ms: 13_000,
        assignments: vec![ServingAssignment {
            partition_id: Id128 { high: 1, low: 2 },
            owner_epoch: EPOCH,
        }],
        assignment_digest: [0; 32],
    };
    grant.seal();
    service
        .authority()
        .install(grant, &policy(), 1_000, 50_000)
        .unwrap();
    (service, partition)
}

#[tokio::test]
async fn direct_put_get_preserves_object_metadata_and_position() {
    let (service, _) = fixture().await;
    let put = service
        .handle_point(
            PointRequest {
                routing: routing(1),
                operation: PointOperation::Put {
                    key: b"bucket/object".to_vec(),
                    value: b"chunk=44;etag=abc".to_vec(),
                },
            },
            1_500,
            50_100,
        )
        .await;
    assert!(put.result.is_ok());
    assert!(put.journal_position.is_some());

    let get = service
        .handle_point(
            PointRequest {
                routing: routing(2),
                operation: PointOperation::Get {
                    key: b"bucket/object".to_vec(),
                },
            },
            1_500,
            50_100,
        )
        .await;
    let OperationResult::Value(Some(value)) = get.result.unwrap() else {
        panic!("expected stored metadata")
    };
    assert_eq!(value.value, b"chunk=44;etag=abc");
}

#[tokio::test]
async fn stale_route_and_expired_deadline_append_nothing() {
    let (service, partition) = fixture().await;
    assert_eq!(service.health(50_100).lifecycle, ServerLifecycle::Serving);
    let before = partition.snapshot().journal_durable_seq;
    let mut stale = routing(1);
    stale.owner_epoch -= 1;
    let response = service
        .handle_point(
            PointRequest {
                routing: stale,
                operation: PointOperation::Put {
                    key: b"object".to_vec(),
                    value: b"metadata".to_vec(),
                },
            },
            1_500,
            50_100,
        )
        .await;
    assert_eq!(response.result.unwrap_err().code, ChunkKvRpcErrorCode::NotMyRange);

    let response = service
        .handle_point(
            PointRequest {
                routing: routing(2),
                operation: PointOperation::Delete {
                    key: b"object".to_vec(),
                },
            },
            2_000,
            50_100,
        )
        .await;
    assert_eq!(
        response.result.unwrap_err().code,
        ChunkKvRpcErrorCode::RequestExpired
    );
    assert_eq!(partition.snapshot().journal_durable_seq, before);
}

#[tokio::test]
async fn local_self_fence_rejects_before_wal_admission() {
    let (service, partition) = fixture().await;
    let before = partition.snapshot().journal_durable_seq;
    let response = service
        .handle_point(
            PointRequest {
                routing: routing(1),
                operation: PointOperation::Put {
                    key: b"object".to_vec(),
                    value: b"metadata".to_vec(),
                },
            },
            1_500,
            60_000,
        )
        .await;
    assert_eq!(
        response.result.unwrap_err().code,
        ChunkKvRpcErrorCode::LeaseExpired
    );
    assert_eq!(partition.snapshot().journal_durable_seq, before);
    assert_eq!(service.metrics().snapshot().lease_rejections, 1);
}

#[tokio::test]
async fn drain_closes_admission_and_relinquishes_authority() {
    let (service, partition) = fixture().await;
    let before = partition.snapshot().journal_durable_seq;
    service.begin_drain();
    assert_eq!(service.health(50_100).lifecycle, ServerLifecycle::Draining);
    let response = service
        .handle_point(
            PointRequest {
                routing: routing(1),
                operation: PointOperation::Put {
                    key: b"object".to_vec(),
                    value: b"metadata".to_vec(),
                },
            },
            1_500,
            50_100,
        )
        .await;
    assert_eq!(response.result.unwrap_err().code, ChunkKvRpcErrorCode::Recovering);
    assert_eq!(partition.snapshot().journal_durable_seq, before);
    assert_eq!(service.metrics().snapshot().requests, 1);
}
