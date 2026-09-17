// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv::memory::MemoryPartitionTree;
use crowdb_chunk_kv::{
    Checkpoint, Partition, PartitionConfig, PartitionId, PartitionJournal, PartitionLifecycle,
    PartitionRange, PartitionTree, PreparedChildArtifact, SplitArtifact, SplitChild, SplitPlan,
    StreamPartitionJournal, TransitionId,
};
use crowdb_chunk_kv_server::{ChunkKvService, ServerLifecycle};
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig, StreamMetadataStore,
    StreamName, StreamRegistry,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, ChunkKvRpcErrorCode, ClientRequestId, DomainFailurePolicy,
    DomainMonitorDescriptor, Id128, KeyRange, OperationResult, OwnerDescriptor, PartitionArtifact,
    PointOperation, PointRequest, RequestRouting, ScanDirection, ScanRequest, SeekKind, SeekRequest,
    ServingAssignment, ServingGrant, TailOverlayArtifact,
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
        chunk_kv_range_balance: Some(crowdb_protocol::chunk_kv::ChunkKvRangeBalancePolicy::default()),
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

fn catalog(
    partition_id: Id128,
    stream_name: StreamName,
    owner_epoch: u64,
    generation: u64,
    previous_generation: Option<u64>,
) -> (ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage) {
    let mut page = ChunkKvRangeCatalogPage {
        generation,
        page_index: 0,
        entries: vec![ChunkKvRangeCatalogEntry {
            partition_id,
            range: KeyRange {
                start: Vec::new(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: INSTANCE_ID,
                rpc_endpoint: "127.0.0.1:9900".into(),
            },
            owner_epoch,
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
        generation,
        previous_generation,
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: generation,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    (head, page)
}

fn prepared_split_child(
    partition_id: PartitionId,
    range: PartitionRange,
    tree_id: u64,
    cutover_seq: u64,
) -> PreparedChildArtifact {
    PreparedChildArtifact {
        partition_id,
        range,
        ownership_epoch: EPOCH + 1,
        tree_id,
        tree_manifest: 1,
        stream_name: StreamName {
            high: tree_id,
            low: 1,
        },
        base_applied_seq: cutover_seq,
        parent_id: PartitionId { high: 1, low: 2 },
        parent_epoch: EPOCH,
        parent_stream_name: StreamName { high: 10, low: 11 },
        parent_stream_manifest_generation: 1,
        parent_replay_offset: 0,
        parent_cutover_offset: cutover_seq,
        applied_seq: cutover_seq,
        child_stream_start_seq: cutover_seq + 1,
    }
}

fn split_catalog_page(artifact: &SplitArtifact) -> ChunkKvRangeCatalogPage {
    let mut page = ChunkKvRangeCatalogPage {
        generation: 2,
        page_index: 0,
        entries: [&artifact.left, &artifact.right]
            .into_iter()
            .map(|child| ChunkKvRangeCatalogEntry {
                partition_id: Id128 {
                    high: child.partition_id.high,
                    low: child.partition_id.low,
                },
                range: KeyRange {
                    start: child.range.start.clone().unwrap_or_default(),
                    end: child.range.end.clone(),
                },
                owner: OwnerDescriptor {
                    instance_id: INSTANCE_ID,
                    rpc_endpoint: "127.0.0.1:9900".into(),
                },
                owner_epoch: child.ownership_epoch,
                state: ChunkKvRangeCatalogPartitionState::Serving,
                artifact: PartitionArtifact {
                    tree_id: child.tree_id,
                    stream_name: child.stream_name,
                    tail_overlay: Some(TailOverlayArtifact {
                        source_partition_id: Id128 {
                            high: child.parent_id.high,
                            low: child.parent_id.low,
                        },
                        source_epoch: child.parent_epoch,
                        source_stream_name: child.parent_stream_name,
                        source_stream_manifest_generation: child.parent_stream_manifest_generation,
                        replay_offset: child.parent_replay_offset,
                        cutover_offset: child.parent_cutover_offset,
                        base_tree_manifest: child.tree_manifest,
                        base_applied_seq: child.base_applied_seq,
                        cutover_seq: child.applied_seq,
                        target_stream_start_seq: child.child_stream_start_seq,
                    }),
                },
                transition_id: Some(Id128 {
                    high: artifact.transition_id.high,
                    low: artifact.transition_id.low,
                }),
            })
            .collect(),
        checksum: [0; 32],
    };
    page.seal().unwrap();
    page
}

async fn prepared_partition(
    partition_id: PartitionId,
    stream_name: StreamName,
    owner_epoch: u64,
) -> Partition {
    prepared_partition_range(
        partition_id,
        PartitionRange {
            start: Some(Vec::new()),
            end: None,
        },
        stream_name,
        owner_epoch,
        1,
    )
    .await
}

async fn prepared_partition_range(
    partition_id: PartitionId,
    range: PartitionRange,
    stream_name: StreamName,
    owner_epoch: u64,
    tree_id: u64,
) -> Partition {
    let store = Arc::new(MemoryStreamStore::new(4_096));
    let stream = ChunkStream::create(
        StreamBinding {
            stream_name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        },
        owner_epoch,
        StreamConfig::default(),
        store.clone() as Arc<dyn StreamRegistry>,
        store.clone() as Arc<dyn StreamMetadataStore>,
        store as Arc<dyn StreamChunkStore>,
    )
    .await
    .unwrap();
    Partition::recover_prepared_assignment(
        partition_id,
        range,
        owner_epoch,
        Checkpoint {
            tree_id,
            tree_manifest: 0,
            applied_seq: 0,
            stream_name,
            stream_manifest_generation: 1,
            replay_offset: 0,
        },
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::with_tree_id(tree_id)),
        Arc::new(StreamPartitionJournal::new(stream, stream_name)),
    )
    .await
    .unwrap()
}

async fn activate_split_catalog(
    service: &ChunkKvService,
    artifact: &SplitArtifact,
    page: &ChunkKvRangeCatalogPage,
) -> Partition {
    let prepare_child = |child: &PreparedChildArtifact| {
        prepared_partition_range(
            child.partition_id,
            child.range.clone(),
            child.stream_name,
            child.ownership_epoch,
            child.tree_id,
        )
    };
    let (left, right) = tokio::join!(prepare_child(&artifact.left), prepare_child(&artifact.right));
    service.install_partition(&left).unwrap();
    service.install_partition(&right).unwrap();
    service
        .commit_catalog_splits(page.generation, std::slice::from_ref(page))
        .await
        .unwrap();

    let mut head = ChunkKvRangeCatalogHead {
        generation: page.generation,
        previous_generation: Some(page.generation - 1),
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: page.generation,
            page_index: page.page_index,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    service
        .install_catalog_and_reconcile(&head, std::slice::from_ref(page), &[])
        .unwrap();
    let assignment = |child: &PreparedChildArtifact| ServingAssignment {
        partition_id: Id128 {
            high: child.partition_id.high,
            low: child.partition_id.low,
        },
        owner_epoch: child.ownership_epoch,
    };
    let mut grant = ServingGrant {
        instance_id: INSTANCE_ID,
        lease_sequence: 2,
        catalog_generation: page.generation,
        issued_at_ms: 1_100,
        expires_at_ms: 13_100,
        assignments: vec![assignment(&artifact.left), assignment(&artifact.right)],
        assignment_digest: [0; 32],
    };
    grant.seal();
    service
        .authority()
        .install(grant, &policy(), 1_100, 50_000)
        .unwrap();
    for child in [&artifact.left, &artifact.right] {
        service
            .activate_recovered_partition(
                Id128 {
                    high: child.partition_id.high,
                    low: child.partition_id.low,
                },
                child.ownership_epoch,
            )
            .unwrap();
    }
    left
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

    let (head, page) = catalog(Id128 { high: 1, low: 2 }, stream_name, EPOCH, 1, None);

    let service = ChunkKvService::new(INSTANCE_ID, 8).unwrap();
    service
        .install_catalog_and_reconcile(&head, &[page], std::slice::from_ref(&partition))
        .unwrap();
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
async fn matching_grant_activation_promotes_a_replayed_partition() {
    let stream_name = StreamName { high: 30, low: 32 };
    let partition = prepared_partition(PartitionId { high: 1, low: 3 }, stream_name, EPOCH).await;
    let service = ChunkKvService::new(INSTANCE_ID, 4).unwrap();
    let (head, page) = catalog(Id128 { high: 1, low: 3 }, stream_name, EPOCH, 1, None);
    service
        .install_catalog_and_reconcile(&head, &[page], std::slice::from_ref(&partition))
        .unwrap();
    assert_eq!(partition.snapshot().lifecycle, PartitionLifecycle::Prepared);
    assert!(!service.registry_observation(1_024, 0).hosted[0].recovering);
    assert!(matches!(
        service.activate_recovered_partition(Id128 { high: 1, low: 3 }, EPOCH - 1),
        Err(crowdb_chunk_kv::ChunkKvError::NotServing(_))
    ));
    assert_eq!(partition.snapshot().lifecycle, PartitionLifecycle::Prepared);

    service
        .activate_recovered_partition(Id128 { high: 1, low: 3 }, EPOCH)
        .unwrap();

    assert_eq!(partition.snapshot().lifecycle, PartitionLifecycle::Serving);
}

#[tokio::test]
async fn matching_grant_refresh_keeps_a_preparing_parent_active() {
    let (service, partition) = fixture().await;
    partition
        .begin_split(SplitPlan {
            transition_id: TransitionId { high: 22, low: 23 },
            parent_id: PartitionId { high: 1, low: 2 },
            parent_epoch: EPOCH,
            parent_range: PartitionRange {
                start: Some(Vec::new()),
                end: None,
            },
            split_key: b"m".to_vec(),
            left: SplitChild {
                partition_id: PartitionId { high: 22, low: 24 },
                range: PartitionRange {
                    start: Some(Vec::new()),
                    end: Some(b"m".to_vec()),
                },
                ownership_epoch: EPOCH + 1,
            },
            right: SplitChild {
                partition_id: PartitionId { high: 22, low: 25 },
                range: PartitionRange {
                    start: Some(b"m".to_vec()),
                    end: None,
                },
                ownership_epoch: EPOCH + 1,
            },
        })
        .await
        .unwrap();

    service
        .activate_recovered_partition(Id128 { high: 1, low: 2 }, EPOCH)
        .unwrap();

    assert_eq!(partition.snapshot().lifecycle, PartitionLifecycle::SplitPreparing);
}

#[tokio::test]
async fn catalog_cutover_commits_the_exact_fenced_split_parent() {
    let (service, parent) = fixture().await;
    let transition_id = TransitionId { high: 8, low: 9 };
    let left_id = PartitionId { high: 8, low: 10 };
    let right_id = PartitionId { high: 8, low: 11 };
    let left_range = PartitionRange {
        start: Some(Vec::new()),
        end: Some(b"m".to_vec()),
    };
    let right_range = PartitionRange {
        start: Some(b"m".to_vec()),
        end: None,
    };
    parent
        .begin_split(SplitPlan {
            transition_id,
            parent_id: PartitionId { high: 1, low: 2 },
            parent_epoch: EPOCH,
            parent_range: PartitionRange {
                start: Some(Vec::new()),
                end: None,
            },
            split_key: b"m".to_vec(),
            left: SplitChild {
                partition_id: left_id,
                ownership_epoch: EPOCH + 1,
                range: left_range.clone(),
            },
            right: SplitChild {
                partition_id: right_id,
                ownership_epoch: EPOCH + 1,
                range: right_range.clone(),
            },
        })
        .await
        .unwrap();
    parent.fence_split(transition_id).await.unwrap();
    let cutover_seq = parent.snapshot().applied_seq;
    let artifact = SplitArtifact {
        transition_id,
        parent_id: PartitionId { high: 1, low: 2 },
        parent_epoch: EPOCH,
        cutover_seq,
        left: prepared_split_child(left_id, left_range, 81, cutover_seq),
        right: prepared_split_child(right_id, right_range, 82, cutover_seq),
    };
    parent.record_split_artifact(artifact.clone()).await.unwrap();
    let page = split_catalog_page(&artifact);

    let left = activate_split_catalog(&service, &artifact, &page).await;

    let response = service
        .handle_point(
            PointRequest {
                routing: routing(41),
                operation: PointOperation::Put {
                    key: b"left-key".to_vec(),
                    value: b"after-cutover".to_vec(),
                },
            },
            1_500,
            50_100,
        )
        .await;

    assert_eq!(parent.lifecycle(), PartitionLifecycle::Retired);
    assert!(response.result.is_ok());
    assert_eq!(left.snapshot().journal_durable_seq, cutover_seq + 1);
    assert_eq!(service.metrics_snapshot().split_commits, 1);
    assert_eq!(service.metrics_snapshot().split_stale_route_forwards, 1);
}

#[tokio::test]
async fn newer_catalog_replaces_hosted_assignments_only_after_recovery() {
    let (service, _) = fixture().await;
    let partition_id = Id128 { high: 1, low: 4 };
    let stream_name = StreamName { high: 30, low: 33 };
    let next_epoch = EPOCH + 1;
    let partition = prepared_partition(
        PartitionId {
            high: partition_id.high,
            low: partition_id.low,
        },
        stream_name,
        next_epoch,
    )
    .await;
    let (head, page) = catalog(partition_id, stream_name, next_epoch, 2, Some(1));

    assert!(service
        .install_catalog_and_reconcile(&head, std::slice::from_ref(&page), &[])
        .is_err());
    assert_eq!(service.health(50_000).catalog_generation, 1);

    service
        .install_catalog_and_reconcile(&head, &[page], std::slice::from_ref(&partition))
        .unwrap();
    let health = service.health(50_000);
    assert_eq!(health.catalog_generation, 2);
    assert_eq!(health.partitions.len(), 1);
    assert_eq!(health.partitions[0].partition_id, partition_id);
    assert_eq!(health.partitions[0].owner_epoch, next_epoch);
    assert_eq!(health.partitions[0].lifecycle, PartitionLifecycle::Prepared);
}

#[tokio::test]
async fn catching_up_target_returns_typed_retry_without_wal_admission() {
    let (service, _) = fixture().await;
    let partition_id = Id128 { high: 1, low: 2 };
    let stream_name = StreamName { high: 30, low: 34 };
    let target = prepared_partition(
        PartitionId {
            high: partition_id.high,
            low: partition_id.low,
        },
        stream_name,
        EPOCH + 1,
    )
    .await;
    let (head, mut page) = catalog(partition_id, stream_name, EPOCH + 1, 2, Some(1));
    page.entries[0].state = ChunkKvRangeCatalogPartitionState::TargetCatchingUp;
    page.entries[0].transition_id = Some(Id128 { high: 40, low: 41 });
    page.seal().unwrap();
    let mut head = head;
    head.pages[0].page_checksum = page.checksum;
    head.seal().unwrap();
    service
        .install_catalog_and_reconcile(&head, &[page], std::slice::from_ref(&target))
        .unwrap();
    let mut route = routing(50);
    route.map_revision = 2;
    route.owner_epoch = EPOCH + 1;
    let before = target.snapshot().journal_durable_seq;
    let response = service
        .handle_point(
            PointRequest {
                routing: route,
                operation: PointOperation::Put {
                    key: b"object".to_vec(),
                    value: b"metadata".to_vec(),
                },
            },
            1_500,
            50_100,
        )
        .await;
    let failure = response.result.unwrap_err();
    assert_eq!(failure.code, ChunkKvRpcErrorCode::TargetNotReady);
    assert_eq!(failure.retry_after_ms, Some(10));
    assert_eq!(target.snapshot().journal_durable_seq, before);
}

#[tokio::test]
async fn direct_put_get_preserves_object_metadata_and_position() {
    let (service, _) = fixture().await;
    let observation = service.registry_observation(1_024, 7);
    assert_eq!(observation.capacity_bytes, 1_024);
    assert_eq!(observation.request_rate, 7);
    assert_eq!(observation.hosted.len(), 1);
    assert!(!observation.hosted[0].recovering);
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

#[tokio::test]
async fn ordered_seek_and_scan_preserve_inclusive_range_start() {
    let (service, _) = fixture().await;
    for (sequence, key) in [(1, b"a".as_slice()), (2, b"b"), (3, b"c")] {
        let response = service
            .handle_point(
                PointRequest {
                    routing: routing(sequence),
                    operation: PointOperation::Put {
                        key: key.to_vec(),
                        value: key.to_vec(),
                    },
                },
                1_500,
                50_100,
            )
            .await;
        assert!(response.result.is_ok());
    }

    let seek = service
        .handle_seek(
            SeekRequest {
                routing: routing(4),
                key: b"b".to_vec(),
                kind: SeekKind::Ceiling,
            },
            1_500,
            50_100,
        )
        .await;
    let OperationResult::Value(Some(value)) = seek.result.unwrap() else {
        panic!("expected ceiling result");
    };
    assert_eq!(value.key, b"b");

    let scan = service
        .handle_scan(
            ScanRequest {
                routing: routing(5),
                start: Some(b"b".to_vec()),
                end: None,
                direction: ScanDirection::Forward,
                limit: 1,
                continuation: None,
            },
            1_500,
            50_100,
        )
        .await;
    let OperationResult::Scan { items, continuation } = scan.result.unwrap() else {
        panic!("expected scan result");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].key, b"b");
    let continuation = continuation.expect("truncated scan continuation");

    let continued = service
        .handle_scan(
            ScanRequest {
                routing: routing(6),
                start: Some(b"b".to_vec()),
                end: None,
                direction: ScanDirection::Forward,
                limit: 10,
                continuation: Some(continuation),
            },
            1_500,
            50_100,
        )
        .await;
    let OperationResult::Scan { items, .. } = continued.result.unwrap() else {
        panic!("expected continued scan result");
    };
    assert_eq!(
        items.iter().map(|value| value.key.as_slice()).collect::<Vec<_>>(),
        vec![b"c".as_slice()]
    );
}
