// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/runtime_context.rs"]
#[allow(dead_code)]
mod runtime_context;
#[path = "common/slot_groups.rs"]
#[allow(dead_code)]
mod slot_groups;

use crowdb_chunkdb::routing::{BindingCache, BindingTable};
use crowdb_chunkdb::storage::{range_retirement::RetiredByteRange, ChunkStore, StoreError};
use crowdb_protocol::chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkType, DeleteChunkRangeRequest};
use crowdb_protocol::common::ChunkId;
use std::sync::Arc;

fn store(test: &slot_groups::TestGroups) -> ChunkStore {
    ChunkStore::new(Arc::clone(&test.kv), bindings())
}
fn bindings() -> BindingCache {
    let bindings = BindingCache::new();
    bindings
        .replace(BindingTable::new(
            ChunkSlotBootstrap {
                service_instances: vec![1],
                storage_groups: vec![ChunkStorageGroup {
                    store_id: 0,
                    group_id: 1,
                }],
            }
            .storage_map()
            .unwrap(),
        ))
        .unwrap();
    bindings
}
fn chunk() -> Chunk {
    Chunk {
        id: Some(ChunkId {
            high: 5 << 56,
            low: 1,
        }),
        modify_ts: 1,
        state: ChunkState::Sealed as i32,
        chunk_type: ChunkType::S3 as i32,
        writer_epoch: 7,
        acknowledged_cursor: 8192,
        sealed_length: 8,
        create_ts_ms: 10,
        sealed_ts_ms: 20,
        ..Default::default()
    }
}
fn request(chunk: &Chunk, offset: u32, size: u32) -> DeleteChunkRangeRequest {
    DeleteChunkRangeRequest {
        chunk_id: chunk.id,
        chunk_offset: offset,
        chunk_size: size,
        expected_modify_ts: chunk.modify_ts,
    }
}

#[tokio::test]
async fn exact_union_survives_restart_without_changing_layout_or_claiming_capacity() {
    let test = slot_groups::TestGroups::start().await;
    let store = store(&test);
    let chunk = chunk();
    let id = chunk.id.unwrap();
    store.put_chunk(&chunk).await.unwrap();
    for (start, size) in [(134, 234), (300, 200), (500, 1), (1025, 2047)] {
        assert!(
            !store
                .retire_chunk_range(&request(&chunk, start, size))
                .await
                .unwrap()
                .reclaimed
        );
    }
    let expected = store.get_chunk_range_retirement(&id).await.unwrap().unwrap();
    assert_eq!(
        expected.ranges(),
        [
            RetiredByteRange { start: 134, end: 501 },
            RetiredByteRange {
                start: 1025,
                end: 3072
            }
        ]
    );
    assert_eq!(store.get_chunk(&id).await.unwrap(), chunk);

    let restarted = crate::store(&test);
    assert!(
        !restarted
            .retire_chunk_range(&request(&chunk, 134, 367))
            .await
            .unwrap()
            .reclaimed
    );
    assert_eq!(
        restarted.get_chunk_range_retirement(&id).await.unwrap().unwrap(),
        expected
    );
    assert_eq!(restarted.get_chunk(&id).await.unwrap(), chunk);
}

#[tokio::test]
async fn changed_layout_rejects_new_ranges_but_known_retirement_replays() {
    let test = slot_groups::TestGroups::start().await;
    let store = store(&test);
    let mut chunk = chunk();
    let id = chunk.id.unwrap();
    store.put_chunk(&chunk).await.unwrap();
    let retired = request(&chunk, 0, 10);
    let stale = request(&chunk, 20, 10);
    store.retire_chunk_range(&retired).await.unwrap();
    let before = store.get_chunk_range_retirement(&id).await.unwrap();
    chunk.modify_ts += 1;
    store.put_chunk(&chunk).await.unwrap();
    assert!(matches!(
        store.retire_chunk_range(&stale).await,
        Err(StoreError::Conflict)
    ));
    assert!(!store.retire_chunk_range(&retired).await.unwrap().reclaimed);
    assert_eq!(store.get_chunk_range_retirement(&id).await.unwrap(), before);
    store.retire_chunk_range(&request(&chunk, 20, 10)).await.unwrap();
    assert_eq!(
        store
            .get_chunk_range_retirement(&id)
            .await
            .unwrap()
            .unwrap()
            .revision,
        2
    );
}

#[tokio::test]
async fn concurrent_retirements_are_atomic_and_conflicts_never_lose_a_range() {
    let test = slot_groups::TestGroups::start().await;
    let store = store(&test);
    let chunk = chunk();
    store.put_chunk(&chunk).await.unwrap();
    let (a, b) = (request(&chunk, 0, 10), request(&chunk, 20, 10));
    let (first, second) = tokio::join!(store.retire_chunk_range(&a), store.retire_chunk_range(&b));
    for (result, req) in [(first, a), (second, b)] {
        match result {
            Ok(result) => assert!(!result.reclaimed),
            Err(StoreError::Conflict) => {
                store.retire_chunk_range(&req).await.unwrap();
            }
            error => panic!("unexpected retirement outcome: {error:?}"),
        }
    }
    assert_eq!(
        store
            .get_chunk_range_retirement(&chunk.id.unwrap())
            .await
            .unwrap()
            .unwrap()
            .ranges(),
        [
            RetiredByteRange { start: 0, end: 10 },
            RetiredByteRange { start: 20, end: 30 }
        ]
    );
}

#[tokio::test]
async fn invalid_ranges_active_writers_and_system_chunks_never_create_retirement() {
    let test = slot_groups::TestGroups::start().await;
    let store = store(&test);
    for case in 0..7 {
        let mut chunk = chunk();
        chunk.id.as_mut().unwrap().low += case;
        let mut req = request(&chunk, 0, 1);
        match case {
            0 => req.chunk_size = 0,
            1 => req.chunk_offset = 8192,
            2 => chunk.modify_ts = 0,
            3 => chunk.state = ChunkState::Active as i32,
            4 => {
                chunk.id.as_mut().unwrap().high = 1 << 56;
                req.chunk_id = chunk.id;
            }
            5 => chunk.writer_epoch = 0,
            6 => {
                req.chunk_offset = u32::MAX;
                req.chunk_size = u32::MAX;
            }
            _ => unreachable!(),
        }
        store.put_chunk(&chunk).await.unwrap();
        assert!(
            matches!(
                store.retire_chunk_range(&req).await,
                Err(StoreError::InvalidRange(_) | StoreError::Conflict)
            ),
            "case {case}"
        );
        assert!(store
            .get_chunk_range_retirement(&chunk.id.unwrap())
            .await
            .unwrap()
            .is_none());
        assert_eq!(store.get_chunk(&chunk.id.unwrap()).await.unwrap(), chunk);
    }
}

#[tokio::test]
async fn reused_physical_identity_cannot_adopt_old_retired_ranges() {
    let test = slot_groups::TestGroups::start().await;
    let store = store(&test);
    let mut chunk = chunk();
    store.put_chunk(&chunk).await.unwrap();
    store.retire_chunk_range(&request(&chunk, 0, 10)).await.unwrap();
    chunk.writer_epoch += 1;
    chunk.modify_ts += 1;
    store.put_chunk(&chunk).await.unwrap();
    assert!(matches!(
        store.retire_chunk_range(&request(&chunk, 0, 10)).await,
        Err(StoreError::InvalidRange(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn production_rpc_qualifies_notifications_and_keeps_explicit_layout_conditions() {
    use crowdb_chunkdb::range_guard::RangeGuard;
    use crowdb_chunkdb_client::{ChunkdbClientError, ChunkdbRpcTransport};
    use crowdb_protocol::{chunk_domain::ChunkDomain, port::alloc::alloc_test_port, ServicePort};
    let test = slot_groups::TestGroups::start().await;
    let store = store(&test);
    let chunk = chunk();
    store.put_chunk(&chunk).await.unwrap();
    let guard = Arc::new(RangeGuard::new());
    guard
        .install(
            &ChunkSlotBootstrap {
                service_instances: vec![1],
                storage_groups: vec![ChunkStorageGroup {
                    store_id: 0,
                    group_id: 1,
                }],
            }
            .service_map()
            .unwrap(),
            1,
        )
        .unwrap();
    let (context, stop) = runtime_context::runtime_context(Arc::clone(&test.kv), bindings(), guard);
    let runtime = context.start(ChunkDomain::UserData).await.unwrap();
    let service = Arc::new(runtime.rpc_service);
    let server = Arc::new(crowdb_rpc_ffi::RpcServer::new(None));
    let port = alloc_test_port(ServicePort::ChunkdbRpc);
    server.listen("127.0.0.1", i32::from(port)).unwrap();
    service.register_handlers(&server);
    server.start();
    let endpoint = format!("http://127.0.0.1:{port}");
    let client = ChunkdbRpcTransport::new();
    let mut req = request(&chunk, 134, 234);
    req.expected_modify_ts = chunk.modify_ts + 1;
    assert!(matches!(
        client.send_delete_chunk_range(&endpoint, &req).await,
        Err(ChunkdbClientError::Aborted(_))
    ));
    assert!(store
        .get_chunk_range_retirement(&chunk.id.unwrap())
        .await
        .unwrap()
        .is_none());
    req.expected_modify_ts = 0;
    for _ in 0..2 {
        assert!(
            !client
                .send_delete_chunk_range(&endpoint, &req)
                .await
                .unwrap()
                .reclaimed
        );
    }
    assert_eq!(
        store
            .get_chunk_range_retirement(&chunk.id.unwrap())
            .await
            .unwrap()
            .unwrap()
            .ranges(),
        [RetiredByteRange { start: 134, end: 368 }]
    );
    assert_eq!(store.get_chunk(&chunk.id.unwrap()).await.unwrap(), chunk);
    req.chunk_id.as_mut().unwrap().low += 1;
    assert!(matches!(
        client.send_delete_chunk_range(&endpoint, &req).await,
        Err(ChunkdbClientError::NotFound(_))
    ));
    server.stop();
    stop.send(true).unwrap();
    for worker in runtime.handles {
        tokio::time::timeout(std::time::Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn server_qualifies_current_layout_but_retains_resource_identity_and_atomicity() {
    let test = slot_groups::TestGroups::start().await;
    let store = store(&test);
    let mut chunk = chunk();
    store.put_chunk(&chunk).await.unwrap();
    let mut notification = request(&chunk, 134, 234);
    notification.expected_modify_ts = 0;
    chunk.modify_ts += 1;
    store.put_chunk(&chunk).await.unwrap();
    assert!(!store.retire_chunk_range(&notification).await.unwrap().reclaimed);
    let retired = store
        .get_chunk_range_retirement(&chunk.id.unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retired.revision, chunk.modify_ts);
    assert_eq!(retired.ranges(), [RetiredByteRange { start: 134, end: 368 }]);
    chunk.writer_epoch += 1;
    chunk.modify_ts += 1;
    store.put_chunk(&chunk).await.unwrap();
    assert!(matches!(
        store.retire_chunk_range(&notification).await,
        Err(StoreError::InvalidRange(_))
    ));
    assert_eq!(
        store
            .get_chunk_range_retirement(&chunk.id.unwrap())
            .await
            .unwrap()
            .unwrap(),
        retired
    );
}

#[tokio::test]
async fn dedicated_notification_accepts_only_its_complete_sealed_extent() {
    let test = slot_groups::TestGroups::start().await;
    let store = store(&test);
    let mut chunk = chunk();
    chunk.writer_epoch = 0;
    chunk.acknowledged_cursor = 0;
    store.put_chunk(&chunk).await.unwrap();
    for (offset, length) in [(1, 8191), (0, 7168), (0, 8193)] {
        let mut notification = request(&chunk, offset, length);
        notification.expected_modify_ts = 0;
        assert!(matches!(
            store.retire_chunk_range(&notification).await,
            Err(StoreError::InvalidRange(_))
        ));
    }
    let mut notification = request(&chunk, 0, 8100);
    notification.expected_modify_ts = 0;
    assert!(!store.retire_chunk_range(&notification).await.unwrap().reclaimed);
    let restarted = crate::store(&test);
    assert!(
        !restarted
            .retire_chunk_range(&notification)
            .await
            .unwrap()
            .reclaimed
    );
    assert_eq!(
        restarted
            .get_chunk_range_retirement(&chunk.id.unwrap())
            .await
            .unwrap()
            .unwrap()
            .ranges(),
        [RetiredByteRange { start: 0, end: 8100 }]
    );
    assert_eq!(restarted.get_chunk(&chunk.id.unwrap()).await.unwrap(), chunk);
}

#[tokio::test]
async fn deleted_chunk_identity_cannot_be_allocated_again() {
    let test = slot_groups::TestGroups::start().await;
    let store = store(&test);
    let mut chunk = chunk();
    chunk.state = ChunkState::Deleted as i32;
    store.put_chunk(&chunk).await.unwrap();
    let mut reused = chunk.clone();
    reused.state = ChunkState::Active as i32;
    reused.writer_epoch += 1;
    assert!(matches!(
        store
            .create_chunk_with_finalize_task(&reused, &slot_groups::finalize(chunk.id.unwrap()))
            .await,
        Err(StoreError::ChunkAlreadyExists)
    ));
    assert_eq!(store.get_chunk(&chunk.id.unwrap()).await.unwrap(), chunk);
}

#[tokio::test]
async fn interval_budget_rejects_growth_but_allows_coalescing_without_losing_live_gaps() {
    let test = slot_groups::TestGroups::start().await;
    let store = store(&test);
    let mut chunk = chunk();
    chunk.acknowledged_cursor = 1024 * 1024;
    chunk.sealed_length = 1024;
    store.put_chunk(&chunk).await.unwrap();
    let id = chunk.id.unwrap();
    let mut key = b"/chunk-ranges/".to_vec();
    key.extend_from_slice(&id.high.to_be_bytes());
    key.extend_from_slice(&id.low.to_be_bytes());
    let ranges: Vec<_> = (0..4096)
        .map(|i| RetiredByteRange {
            start: i * 2,
            end: i * 2 + 1,
        })
        .collect();
    // Version-one resource fixture at the maximum interval count.
    let encoded = bincode::serialize(&(
        1_u16,
        chunk.id,
        chunk.writer_epoch,
        chunk.create_ts_ms,
        chunk.sealed_ts_ms,
        chunk.acknowledged_cursor,
        chunk.modify_ts,
        &ranges,
    ))
    .unwrap();
    test.kv.put(0, 1, &key, &encoded, None).await.unwrap();
    assert!(matches!(
        store.retire_chunk_range(&request(&chunk, 8192, 1)).await,
        Err(StoreError::InvalidRange(_))
    ));
    assert_eq!(
        store
            .get_chunk_range_retirement(&id)
            .await
            .unwrap()
            .unwrap()
            .ranges(),
        ranges
    );
    store.retire_chunk_range(&request(&chunk, 1, 1)).await.unwrap();
    let record = store.get_chunk_range_retirement(&id).await.unwrap().unwrap();
    assert_eq!(record.ranges().len(), 4095);
    assert_eq!(record.ranges()[0], RetiredByteRange { start: 0, end: 3 });
    assert_eq!(record.ranges()[1], RetiredByteRange { start: 4, end: 5 });
    assert_eq!(store.get_chunk(&id).await.unwrap(), chunk);
}
