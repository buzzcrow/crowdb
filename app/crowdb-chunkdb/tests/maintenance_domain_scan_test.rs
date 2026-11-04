// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::range_guard::RangeGuard;
use crowdb_chunkdb::routing::{BindingCache, BindingTable};
use crowdb_chunkdb::storage::ChunkStore;
use crowdb_protocol::chunk_domain::ChunkDomain;
use crowdb_protocol::chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, StripReservationGroup};
use crowdb_protocol::common::ChunkId;
use std::sync::Arc;

#[path = "common/slot_groups.rs"]
#[allow(dead_code)]
mod slot_groups;
use slot_groups::TestGroups;

async fn seed(writer: &ChunkStore, purpose: u8, low: u64) -> ChunkId {
    let id = ChunkId {
        high: u64::from(purpose) << 56,
        low,
    };
    writer
        .put_chunk(&Chunk {
            id: Some(id),
            modify_ts: 1,
            state: ChunkState::Active as i32,
            chunk_type: i32::from(purpose),
            ..Default::default()
        })
        .await
        .unwrap();
    writer
        .put_reservation_group(&StripReservationGroup {
            chunk_id: Some(id),
            group_id: Some(id),
            ..Default::default()
        })
        .await
        .unwrap();
    id
}

async fn assert_domain_scan(domain: ChunkDomain, foreign: u8, purposes: &[u8]) {
    let test = TestGroups::start().await;
    let layout = ChunkSlotBootstrap {
        service_instances: vec![1],
        storage_groups: vec![ChunkStorageGroup {
            store_id: 0,
            group_id: 1,
        }],
    };
    let bindings = BindingCache::new();
    bindings
        .replace(BindingTable::new(layout.storage_map().unwrap()))
        .unwrap();
    let guard = Arc::new(RangeGuard::new());
    guard.install(&layout.service_map().unwrap(), 1).unwrap();
    let writer = ChunkStore::new(Arc::clone(&test.kv), bindings.clone());
    let reader = ChunkStore::new(Arc::clone(&test.kv), bindings).with_scope(guard, domain);
    for low in 1..=600 {
        seed(&writer, foreign, low).await;
    }
    let mut expected = Vec::new();
    for purpose in purposes {
        expected.push(seed(&writer, *purpose, 0).await);
    }
    let _ = test.kv.drain_window();
    let chunks = reader.list_chunks(None, 100).await.unwrap();
    assert_eq!(
        chunks.iter().map(|chunk| chunk.id.unwrap()).collect::<Vec<_>>(),
        expected
    );
    assert_eq!(
        test.kv.drain_window().scan.len(),
        1,
        "a scoped scan must not transfer pages belonging only to another maintenance domain"
    );
    let reservations = reader.scan_reservation_groups(100).await.unwrap();
    assert_eq!(
        reservations
            .iter()
            .map(|group| group.chunk_id.unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(test.kv.drain_window().scan.len(), 1);
    assert_eq!(
        writer.list_chunks(None, 1000).await.unwrap().len(),
        600 + expected.len()
    );
    for (index, id) in expected.iter().enumerate() {
        let page = reader.list_chunks(Some(id), 1).await.unwrap();
        assert_eq!(
            page.first().and_then(|chunk| chunk.id),
            expected.get(index + 1).copied()
        );
        let page = reader
            .scan_reservation_groups_after(1, Some((id, id)))
            .await
            .unwrap();
        assert_eq!(
            page.first().and_then(|group| group.chunk_id),
            expected.get(index + 1).copied()
        );
    }
    let after_domain = ChunkId {
        high: 8 << 56,
        low: 0,
    };
    let _ = test.kv.drain_window();
    assert!(reader
        .list_chunks(Some(&after_domain), 1)
        .await
        .unwrap()
        .is_empty());
    assert!(test.kv.drain_window().scan.is_empty());
}

#[tokio::test]
async fn user_data_scans_exclude_system_pages_before_transfer() {
    Box::pin(assert_domain_scan(ChunkDomain::UserData, 1, &[4, 5, 6, 7])).await;
}

#[tokio::test]
async fn system_scans_exclude_user_data_pages_before_transfer() {
    Box::pin(assert_domain_scan(ChunkDomain::System, 5, &[1, 2, 3])).await;
}
