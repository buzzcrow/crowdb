// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/split_storage.rs"]
mod split_storage;

use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, MutationOperation, Partition, PartitionConfig, PartitionId, PartitionRange,
    RequestId, SplitChild, SplitHandoffStore, SplitPlan, SplitWriterTarget, TransitionId,
};
use crowdb_chunk_kv_server::ChunkKvService;
use crowdb_protocol::chunk_kv::*;
use split_storage::TestSplitStorage;
use std::sync::Arc;

struct TestCommittedHandoff;
#[async_trait::async_trait]
impl SplitHandoffStore for TestCommittedHandoff {
    async fn commit(&self, _: &crowdb_chunk_kv::PreparedSplitWriterArtifact) -> crowdb_chunk_kv::Result<()> {
        Ok(())
    }
}

fn entry(partition: &Partition) -> ChunkKvRangeCatalogEntry {
    let snapshot = partition.snapshot();
    ChunkKvRangeCatalogEntry {
        partition_id: Id128 {
            high: snapshot.partition_id.high,
            low: snapshot.partition_id.low,
        },
        range: KeyRange {
            start: snapshot.range.start.unwrap_or_default(),
            end: snapshot.range.end,
        },
        owner: OwnerDescriptor {
            instance_id: 1,
            rpc_endpoint: "127.0.0.1:9900".into(),
        },
        owner_epoch: snapshot.ownership_epoch,
        state: ChunkKvRangeCatalogPartitionState::Serving,
        artifact: PartitionArtifact {
            tree_id: partition.tree_id(),
            stream_name: snapshot.stream_name,
            tail_overlay: None,
        },
        transition_id: None,
    }
}

fn publish(service: &ChunkKvService, generation: u64, partitions: &[Partition]) {
    let mut page = ChunkKvRangeCatalogPage {
        generation,
        page_index: 0,
        entries: partitions.iter().map(entry).collect(),
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation,
        previous_generation: (generation > 1).then_some(generation - 1),
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: generation,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    service
        .install_catalog_and_reconcile(&head, &[page], partitions)
        .unwrap();
    let mut grant = ServingGrant {
        instance_id: 1,
        lease_sequence: generation,
        catalog_generation: generation,
        issued_at_ms: 1000,
        expires_at_ms: 13000,
        assignments: partitions
            .iter()
            .map(|p| ServingAssignment {
                partition_id: entry(p).partition_id,
                owner_epoch: p.snapshot().ownership_epoch,
            })
            .collect(),
        assignment_digest: [0; 32],
    };
    grant.seal();
    let descriptor = DomainMonitorDescriptor {
        domain: "chunk-kv".into(),
        service_registry_name: "chunk-kv".into(),
        driver_version: 1,
        capability_version: 1,
        heartbeat_interval_ms: 2000,
        suspect_after_ms: 6000,
        dead_after_ms: 10000,
        lease_duration_ms: 12000,
        max_clock_skew_ms: 1000,
        self_fence_margin_ms: 1000,
        failure_policy: DomainFailurePolicy::AutomaticSharedStorage,
        balance_policy: "count-first-v1".into(),
        chunk_kv_range_balance: Some(ChunkKvRangeBalancePolicy::default()),
    };
    service
        .authority()
        .install(grant, &descriptor, 1000, 50000)
        .unwrap();
}

async fn write(partition: &Partition, sequence: u64, value: &[u8]) {
    partition
        .mutate(
            partition.snapshot().ownership_epoch,
            RequestId {
                client_high: 1,
                client_low: 1,
                client_sequence: sequence,
            },
            MutationOperation::Put {
                key: b"b".to_vec(),
                value: value.to_vec(),
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn current_assignment_supersedes_historical_split_dispatcher() {
    let storage = TestSplitStorage::new();
    let id = PartitionId { high: 90, low: 1 };
    let (parent, _) = storage
        .partition(
            id,
            PartitionRange {
                start: Some(Vec::new()),
                end: None,
            },
            1,
            Arc::new(MemoryPartitionTree::with_tree_id(90)),
        )
        .await;
    write(&parent, 1, b"old-view").await;
    let service = ChunkKvService::new(1, 8).unwrap();
    publish(&service, 1, std::slice::from_ref(&parent));
    let plan = SplitPlan {
        transition_id: TransitionId { high: 90, low: 2 },
        parent_id: id,
        parent_range: parent.snapshot().range,
        parent_epoch: 1,
        parent_next_epoch: 2,
        split_key: b"m".to_vec(),
        child: SplitChild {
            partition_id: PartitionId { high: 91, low: 1 },
            ownership_epoch: 1,
            range: PartitionRange {
                start: Some(b"m".to_vec()),
                end: None,
            },
        },
    };
    let prepared = parent
        .prepare_split_child_session(
            plan,
            SplitWriterTarget {
                tree_id: 91,
                tree_config: crowdb_tree_ffi::Config::default(),
                journal: storage
                    .journal(crowdb_chunk_stream::StreamName { high: 91, low: 1 }, 1)
                    .await,
            },
            8,
            Arc::new(TestCommittedHandoff),
        )
        .await
        .unwrap();
    let child = prepared.child.open_warmed(PartitionConfig::default()).unwrap();
    service.install_partition(&child).unwrap();
    service
        .record_local_split_ready(&prepared.artifact)
        .await
        .unwrap();
    assert!(
        service.hosts_catalog_assignment(&entry(&parent)),
        "the unpublished parent assignment must reuse its live split dispatcher"
    );
    publish(&service, 2, std::slice::from_ref(&parent));
    let retained = parent.split_ingress().unwrap().retained_parent();
    publish(&service, 3, &[retained.clone(), child.clone()]);
    let journal = storage
        .journal(crowdb_chunk_stream::StreamName { high: 92, low: 1 }, 3)
        .await;
    let current = Partition::open(
        id,
        retained.snapshot().range,
        3,
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::with_tree_id(90)),
        journal,
    )
    .unwrap();
    write(&current, 2, b"current-view").await;
    publish(&service, 4, &[current.clone(), child]);
    for (map_revision, owner_epoch) in [(1, 1), (4, 3)] {
        let response = service
            .handle_point(
                PointRequest {
                    routing: RequestRouting {
                        request_id: ClientRequestId {
                            client_instance_id: Id128 { high: 93, low: 1 },
                            client_sequence: map_revision,
                        },
                        map_revision,
                        partition_id: entry(&current).partition_id,
                        owner_epoch,
                        min_journal_position: None,
                        deadline_ms: Some(2000),
                    },
                    operation: PointOperation::Get { key: b"b".to_vec() },
                },
                1500,
                50100,
            )
            .await;
        let OperationResult::Value(Some(value)) = response.result.unwrap() else {
            panic!("expected current record")
        };
        assert_eq!(value.value, b"current-view");
    }
}
