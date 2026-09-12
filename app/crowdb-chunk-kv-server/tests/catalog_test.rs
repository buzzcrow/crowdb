// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv_server::{
    CatalogError, CatalogPublisher, CatalogStore, HeadWriteOutcome, MemoryCatalogStore,
};
use crowdb_protocol::chunk_kv::{
    CatalogEntry, CatalogHead, CatalogPage, CatalogPageRef, CatalogPartitionState, Id128, KeyRange,
    OwnerDescriptor, PartitionArtifact,
};
use crowdb_protocol::chunk_stream::StreamName;

fn page(generation: u64, owner_epoch: u64) -> CatalogPage {
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
            owner_epoch,
            state: CatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: 1,
                tree_manifest: 7,
                stream_name: StreamName { high: 2, low: 3 },
                applied_seq: 11,
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

#[tokio::test]
async fn publisher_writes_pages_before_head_and_reuses_unchanged_pages() {
    let store = Arc::new(MemoryCatalogStore::default());
    let publisher = CatalogPublisher::new(store.clone());
    let first_page = page(1, 1);
    let first_head = head(1, None, &first_page);
    publisher
        .publish(first_head.clone(), vec![first_page.clone()])
        .await
        .unwrap();
    assert_eq!(store.write_counts().await, (1, 1));
    assert_eq!(store.get_head().await.unwrap(), Some(first_head));

    let second_head = head(2, Some(1), &first_page);
    publisher
        .publish(second_head.clone(), vec![first_page])
        .await
        .unwrap();
    assert_eq!(store.write_counts().await, (1, 2));
    assert_eq!(store.get_head().await.unwrap(), Some(second_head));
}

#[tokio::test]
async fn ambiguous_head_is_accepted_only_when_reread_proves_exact_commit() {
    let store = Arc::new(MemoryCatalogStore::default());
    let publisher = CatalogPublisher::new(store.clone());
    let first_page = page(1, 1);
    let first_head = head(1, None, &first_page);
    store
        .set_next_head_outcome(HeadWriteOutcome::Ambiguous, true)
        .await;
    publisher.publish(first_head, vec![first_page]).await.unwrap();

    let second_page = page(2, 2);
    let second_head = head(2, Some(1), &second_page);
    store
        .set_next_head_outcome(HeadWriteOutcome::Ambiguous, false)
        .await;
    assert_eq!(
        publisher.publish(second_head, vec![second_page]).await,
        Err(CatalogError::AmbiguousHead)
    );
}

#[tokio::test]
async fn publisher_rejects_epoch_regression_before_head_write() {
    let store = Arc::new(MemoryCatalogStore::default());
    let publisher = CatalogPublisher::new(store.clone());
    let first_page = page(1, 4);
    publisher
        .publish(head(1, None, &first_page), vec![first_page])
        .await
        .unwrap();

    let regressed = page(2, 3);
    assert!(publisher
        .publish(head(2, Some(1), &regressed), vec![regressed])
        .await
        .is_err());
    assert_eq!(store.write_counts().await, (1, 1));
}
