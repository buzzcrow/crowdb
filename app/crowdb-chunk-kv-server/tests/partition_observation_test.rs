// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, Partition, PartitionConfig, PartitionId, PartitionRange,
    StreamPartitionJournal,
};
use crowdb_chunk_kv_server::{management_router, ChunkKvService, ManagementState};
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamBinding, StreamBindingState, StreamConfig,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, Id128, KeyRange, OwnerDescriptor, PartitionArtifact,
};
use crowdb_protocol::chunk_stream::StreamName;
use serde_json::Value;
use tower::ServiceExt;

const ID: Id128 = Id128 {
    high: u64::MAX,
    low: 1,
};

async fn journal() -> StreamPartitionJournal {
    let store = Arc::new(MemoryStreamStore::new(4096));
    let stream_name = StreamName { high: 2, low: 3 };
    let stream = ChunkStream::create(
        StreamBinding {
            stream_name,
            metadata_group_id: 7,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("chunk-kv-partition".into()),
        },
        u64::MAX,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store,
    )
    .await
    .unwrap();
    StreamPartitionJournal::new(stream, stream_name)
}

fn catalog() -> (ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage) {
    let mut page = ChunkKvRangeCatalogPage {
        generation: 9,
        page_index: 0,
        checksum: [0; 32],
        entries: vec![ChunkKvRangeCatalogEntry {
            partition_id: ID,
            range: KeyRange {
                start: vec![],
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: 7,
                rpc_endpoint: "127.0.0.1:45200".into(),
            },
            owner_epoch: u64::MAX,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            transition_id: None,
            artifact: PartitionArtifact {
                tree_id: 1,
                stream_name: StreamName { high: 2, low: 3 },
                tail_overlay: None,
            },
        }],
    };
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation: 9,
        previous_generation: None,
        checksum: [0; 32],
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 9,
            page_index: 0,
            first_key: vec![],
            page_checksum: page.checksum,
        }],
    };
    head.seal().unwrap();
    (head, page)
}

async fn get(app: axum::Router, id: &str, generation: u64, epoch: u64) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::get(format!(
                "/partitions/{id}/observation?generation={generation}&epoch={epoch}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn observation_keeps_exact_fences_and_does_not_confuse_serving_with_live_authority() {
    let service = Arc::new(ChunkKvService::new(7, 8).unwrap());
    let (head, page) = catalog();
    service.install_catalog(&head, &[page]).unwrap();
    let id = format!("{:016x}{:016x}", ID.high, ID.low);
    let app = management_router(ManagementState::new(service.clone()));
    assert_eq!(
        get(app.clone(), "invalid", 9, u64::MAX).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(get(app.clone(), &id, 9, u64::MAX).await.0, StatusCode::NOT_FOUND);
    let partition = Partition::open(
        PartitionId {
            high: ID.high,
            low: ID.low,
        },
        PartitionRange {
            start: None,
            end: None,
        },
        u64::MAX,
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::with_tree_id(1)),
        Arc::new(journal().await),
    )
    .unwrap();
    service.install_partition(&partition).unwrap();
    let (status, body) = get(app.clone(), &id, 9, u64::MAX).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["partition_id"], id);
    assert_eq!(body["owner_epoch"], u64::MAX.to_string());
    assert_eq!(body["lifecycle"], "Serving");
    assert_eq!(body["live_grant"], false);
    assert_eq!(body["journal_durable_offset"], "0");
    assert_eq!(body["applied_seq"], "0");
    assert_eq!(get(app.clone(), &id, 8, u64::MAX).await.0, StatusCode::CONFLICT);
    assert_eq!(get(app.clone(), &id, 9, 1).await.0, StatusCode::CONFLICT);
    partition
        .mutate(
            u64::MAX,
            crowdb_chunk_kv::RequestId {
                client_high: 1,
                client_low: 1,
                client_sequence: 1,
            },
            crowdb_chunk_kv::MutationOperation::Put {
                key: b"observed".to_vec(),
                value: b"value".to_vec(),
            },
        )
        .await
        .unwrap();
    let (_, advanced) = get(app.clone(), &id, 9, u64::MAX).await;
    assert_eq!(advanced["journal_durable_seq"], "1");
    assert_eq!(advanced["applied_seq"], "1");
    assert!(
        advanced["journal_durable_offset"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );
    service.begin_drain();
    assert_eq!(get(app, &id, 9, u64::MAX).await.1["admitting"], false);
}
