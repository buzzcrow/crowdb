// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv_server::storage::open_root_catalog_for_tests;
use crowdb_chunk_kv_server::{ChunkKvRangeCatalogPublisher, Group0ControlStore};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, Id128, KeyRange, OwnerDescriptor, PartitionArtifact,
};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_test_harness::cluster::KvCluster;
use crowdb_tree_ffi::{CtError, RootCatalogObject};

fn source() -> ChunkKvRangeCatalogEntry {
    ChunkKvRangeCatalogEntry {
        partition_id: Id128 { high: 1, low: 1 },
        range: KeyRange {
            start: Vec::new(),
            end: None,
        },
        owner: OwnerDescriptor {
            instance_id: 1,
            rpc_endpoint: "127.0.0.1:1".into(),
        },
        owner_epoch: 1,
        state: ChunkKvRangeCatalogPartitionState::Serving,
        artifact: PartitionArtifact {
            tree_id: 42,
            stream_name: StreamName { high: 1, low: 2 },
            tail_overlay: None,
        },
        transition_id: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparing_target_does_not_fence_source_or_mutate_shared_root() {
    let cluster = KvCluster::start().await;
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
        cluster.mgmt_endpoints.clone(),
    )));
    let source_entry = source();
    let source = open_root_catalog_for_tests(kv.clone(), 0, 1, &source_entry)
        .await
        .unwrap();
    publish_assignment(kv.clone(), source_entry.clone(), 1).await;
    source.publish(42, 0, 1, 1, b"source-checkpoint-1").unwrap();
    let mut target_entry = source_entry.clone();
    target_entry.owner.instance_id = 2;
    target_entry.owner_epoch = 2;
    target_entry.state = ChunkKvRangeCatalogPartitionState::Prepared;
    target_entry.artifact.stream_name.low = 3;
    target_entry.transition_id = Some(Id128 { high: 2, low: 2 });
    let target = open_root_catalog_for_tests(kv.clone(), 0, 1, &target_entry)
        .await
        .unwrap();
    assert_eq!(
        target.load(42, RootCatalogObject::Manifest(1)).unwrap(),
        Some(b"source-checkpoint-1".to_vec())
    );
    assert_eq!(
        target.publish(42, 1, 2, 2, b"unpublished-target"),
        Err(CtError::Unavailable)
    );
    // Private preparation metadata and exact pin cleanup do not publish a root.
    let reference = target.allocate_reference_segment_id(42).unwrap();
    target
        .store(
            42,
            RootCatalogObject::ReferenceSegment(reference),
            b"prepared-reference",
        )
        .unwrap();
    source.pin_generation(42, 2, 2, 1).unwrap();
    target.unpin_generation(42, 2, 2).unwrap();
    source.publish(42, 1, 1, 2, b"source-checkpoint-2").unwrap();
    drop(source);
    let recovered = open_root_catalog_for_tests(kv, 0, 1, &source_entry)
        .await
        .unwrap();
    assert_eq!(
        recovered.load(42, RootCatalogObject::CurrentManifest).unwrap(),
        Some(b"source-checkpoint-2".to_vec())
    );
    recovered.publish(42, 2, 1, 3, b"source-checkpoint-3").unwrap();
}

async fn publish_assignment(kv: Arc<CrowdbKvClient>, entry: ChunkKvRangeCatalogEntry, generation: u64) {
    let mut page = ChunkKvRangeCatalogPage {
        generation,
        page_index: 0,
        entries: vec![entry],
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
    ChunkKvRangeCatalogPublisher::new(Arc::new(Group0ControlStore::from_client(kv)))
        .publish(head, vec![page])
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_published_matching_target_can_claim_mutable_root() {
    let cluster = KvCluster::start().await;
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
        cluster.mgmt_endpoints.clone(),
    )));
    let source_entry = source();
    let source = open_root_catalog_for_tests(kv.clone(), 0, 1, &source_entry)
        .await
        .unwrap();
    source.publish(42, 0, 1, 1, b"source-root").unwrap();
    publish_assignment(kv.clone(), source_entry.clone(), 1).await;
    let mut target_entry = source_entry.clone();
    target_entry.owner.instance_id = 2;
    target_entry.owner_epoch = 2;
    target_entry.artifact.stream_name.low = 3;
    target_entry.transition_id = Some(Id128 { high: 3, low: 4 });
    target_entry.state = ChunkKvRangeCatalogPartitionState::Prepared;
    let target = open_root_catalog_for_tests(kv.clone(), 0, 1, &target_entry)
        .await
        .unwrap();
    assert_eq!(
        target.publish(42, 1, 2, 2, b"target-root"),
        Err(CtError::Unavailable)
    );
    let mut wrong = target_entry.clone();
    wrong.state = ChunkKvRangeCatalogPartitionState::TargetCatchingUp;
    wrong.transition_id = Some(Id128 { high: 3, low: 5 });
    publish_assignment(kv.clone(), wrong, 2).await;
    assert_eq!(
        target.publish(42, 1, 2, 2, b"target-root"),
        Err(CtError::Unavailable)
    );
    source.publish(42, 1, 1, 2, b"source-root-2").unwrap();
    target_entry.state = ChunkKvRangeCatalogPartitionState::TargetCatchingUp;
    publish_assignment(kv.clone(), target_entry.clone(), 3).await;
    assert_eq!(
        target.publish(42, 2, 2, 3, b"target-root"),
        Err(CtError::Unavailable)
    );
    target_entry.state = ChunkKvRangeCatalogPartitionState::Serving;
    publish_assignment(kv.clone(), target_entry, 4).await;
    target.publish(42, 2, 2, 3, b"target-root").unwrap();
    assert_eq!(
        source.publish(42, 3, 1, 4, b"stale-source"),
        Err(CtError::Unavailable)
    );
    assert!(open_root_catalog_for_tests(kv, 0, 1, &source_entry)
        .await
        .is_err());
}
