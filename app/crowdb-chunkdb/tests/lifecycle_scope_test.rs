// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunkdb::range_guard::RangeGuard;
use crowdb_chunkdb::routing::{BindingCache, BindingTable};
use crowdb_chunkdb::storage::{ChunkStore, StoreError};
use crowdb_protocol::chunk_domain::ChunkDomain;
use crowdb_protocol::chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, StripReservationGroup};
use crowdb_protocol::common::ChunkId;

#[path = "common/slot_groups.rs"]
mod slot_groups;
use slot_groups::{finalize, TestGroups};

#[tokio::test]
async fn lifecycle_metadata_and_reservations_cannot_escape_domain_authority() {
    let test = TestGroups::start().await;
    let layout = ChunkSlotBootstrap {
        service_instances: vec![1, 2],
        storage_groups: (1..=3)
            .map(|group_id| ChunkStorageGroup {
                store_id: 0,
                group_id,
            })
            .collect(),
    };
    let bindings = BindingCache::new();
    bindings
        .replace(BindingTable::new(layout.storage_map().unwrap()))
        .unwrap();
    let guard = Arc::new(RangeGuard::new());
    guard.install(&layout.service_map().unwrap(), 1).unwrap();
    let writer = ChunkStore::new(Arc::clone(&test.kv), bindings.clone());
    let user = ChunkStore::new(Arc::clone(&test.kv), bindings.clone())
        .with_scope(Arc::clone(&guard), ChunkDomain::UserData);
    let system =
        ChunkStore::new(Arc::clone(&test.kv), bindings).with_scope(Arc::clone(&guard), ChunkDomain::System);
    let mut expected = Vec::new();
    for (purpose, count) in [(1_u64, 300), (5, 10)] {
        for low in 1..=count {
            let id = ChunkId {
                high: purpose << 56,
                low,
            };
            let chunk = Chunk {
                id: Some(id),
                modify_ts: 1,
                state: ChunkState::Active as i32,
                chunk_type: i32::try_from(purpose).unwrap(),
                ..Default::default()
            };
            writer
                .create_chunk_with_finalize_task(&chunk, &finalize(id))
                .await
                .unwrap();
            let group = StripReservationGroup {
                chunk_id: Some(id),
                group_id: Some(id),
                ..Default::default()
            };
            writer.put_reservation_group(&group).await.unwrap();
            if purpose == 5 && guard.check(&id).is_ok() {
                expected.push(id);
                assert_eq!(user.get_chunk(&id).await.unwrap(), chunk);
            } else {
                assert!(matches!(user.get_chunk(&id).await, Err(StoreError::Authority)));
                assert!(matches!(user.put_chunk(&chunk).await, Err(StoreError::Authority)));
                assert!(matches!(user.delete_chunk(&id).await, Err(StoreError::Authority)));
                assert!(matches!(
                    user.get_reservation_group(&id, &id).await,
                    Err(StoreError::Authority)
                ));
                assert!(matches!(
                    user.put_reservation_group(&group).await,
                    Err(StoreError::Authority)
                ));
                assert!(matches!(
                    user.delete_reservation_group(&id, &id).await,
                    Err(StoreError::Authority)
                ));
            }
        }
    }
    assert!(!expected.is_empty());
    let mut listed = Vec::new();
    loop {
        let page = user.list_chunks(listed.last(), 1).await.unwrap();
        if page.is_empty() {
            break;
        }
        listed.push(page[0].id.unwrap());
    }
    assert_eq!(listed, expected);
    let mut listed = Vec::new();
    loop {
        let page = user
            .scan_reservation_groups_after(1, listed.last().map(|id| (id, id)))
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        listed.push(page[0].chunk_id.unwrap());
    }
    assert_eq!(listed, expected);
    assert!(system.list_chunks(None, 400).await.unwrap().iter().all(|chunk| {
        let id = chunk.id.unwrap();
        guard.check(&id).is_ok() && ChunkDomain::for_chunk(&id) == Some(ChunkDomain::System)
    }));
}
