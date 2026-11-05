// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbClientError, ChunkdbRpcTransport};
use crowdb_paxos_kv_client::{GetOutcome, ReadMode, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, Chunk, ChunkState, ChunkType, DeleteChunkRangeRequest, DeleteChunkRequest,
    QueryChunkRequest, SealChunkRequest, Strip, StripType,
};
use crowdb_protocol::key::{BinaryKey, FreeBlockKey};
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires the owned protected Chunkdb container fixture"]
async fn allocate_seal_delete_preserves_physical_placement_and_free_facts() {
    let client = ChunkdbClient::new(
        ServiceRegistryClient::new(crate::common::hardware::kv_client()),
        Arc::new(ChunkdbRpcTransport::new()),
    );
    let chunk = allocate_protected(&client).await;
    let id = chunk.id.unwrap();
    let queried = client
        .query_chunk(QueryChunkRequest { chunk_id: Some(id) })
        .await
        .unwrap()
        .chunk
        .unwrap();
    assert_eq!(queried.id, chunk.id);
    assert_eq!(queried.state, ChunkState::Active as i32);
    let sealed = client
        .seal_chunk(SealChunkRequest {
            chunk_id: Some(id),
            seal_bytes: 100 * 1024 - 17,
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    assert_eq!(sealed.sealed_length, 100);
    assert_eq!(sealed.acknowledged_cursor, 100 * 1024 - 17);
    assert_eq!(sealed.strips.len(), 1);
    assert_eq!(sealed.capacity, 1024);
    assert!(sealed.cleanup_intents.is_empty());
    let segments = match sealed.strips[0].strip.as_ref().unwrap() {
        Strip::MirrorStrip(mirror) => &mirror.segments,
        Strip::EcStrip(_) => panic!("expected mirror placement"),
    };
    let deleted = client
        .delete_chunk(DeleteChunkRequest { chunk_id: Some(id) })
        .await
        .unwrap()
        .chunk
        .unwrap();
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    assert!(deleted.strips.is_empty());
    assert_free_facts(segments).await;
    let again = client
        .delete_chunk(DeleteChunkRequest { chunk_id: Some(id) })
        .await
        .unwrap()
        .chunk
        .unwrap();
    assert_eq!(again.state, ChunkState::Deleted as i32);
    let queried = client
        .query_chunk(QueryChunkRequest { chunk_id: Some(id) })
        .await
        .unwrap()
        .chunk
        .unwrap();
    assert_eq!(queried.state, ChunkState::Deleted as i32);
    verify_deleted_notification(&client, &queried).await;
    verify_range_notification(&client).await;
}

async fn verify_deleted_notification(client: &ChunkdbClient, deleted: &Chunk) {
    let request = DeleteChunkRangeRequest {
        chunk_id: deleted.id,
        chunk_offset: 0,
        chunk_size: 1,
        expected_modify_ts: deleted.modify_ts,
    };
    assert!(matches!(
        client
            .delete_chunk_range(DeleteChunkRangeRequest {
                chunk_size: 0,
                ..request.clone()
            })
            .await,
        Err(ChunkdbClientError::InvalidArgument(_))
    ));
    for _ in 0..2 {
        assert!(
            client
                .delete_chunk_range(request.clone())
                .await
                .unwrap()
                .reclaimed
        );
    }
    assert_eq!(
        client
            .query_chunk(QueryChunkRequest { chunk_id: deleted.id })
            .await
            .unwrap()
            .chunk
            .unwrap(),
        *deleted
    );
}

async fn verify_range_notification(client: &ChunkdbClient) {
    let allocated = client
        .allocate_chunk(AllocateChunkRequest {
            write_granularity: 1,
            strip_count: 1,
            strip_type: StripType::Mirror as i32,
            copy_count: 3,
            chunk_type: ChunkType::S3 as i32,
            writer_epoch: 17,
            writer_lease_ms: 60_000,
            ..Default::default()
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    let id = allocated.id.unwrap();
    let sealed = client
        .seal_chunk(SealChunkRequest {
            chunk_id: Some(id),
            seal_bytes: 8192,
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    let request = DeleteChunkRangeRequest {
        chunk_id: Some(id),
        chunk_offset: 134,
        chunk_size: 234,
        expected_modify_ts: sealed.modify_ts,
    };
    assert!(
        !client
            .delete_chunk_range(request.clone())
            .await
            .unwrap()
            .reclaimed
    );
    assert!(!client.delete_chunk_range(request).await.unwrap().reclaimed);
    assert_eq!(
        client
            .query_chunk(QueryChunkRequest { chunk_id: Some(id) })
            .await
            .unwrap()
            .chunk
            .unwrap(),
        sealed
    );
    let retired = client
        .delete_chunk(DeleteChunkRequest { chunk_id: Some(id) })
        .await
        .unwrap()
        .chunk
        .unwrap();
    assert_eq!(retired.state, ChunkState::Deleted as i32);
}

async fn assert_free_facts(segments: &[crowdb_protocol::diskdb::rpc::Segment]) {
    let kv = crate::common::hardware::kv_client();
    for segment in segments {
        let key = FreeBlockKey {
            disk_id: segment.disk_id.unwrap(),
            zone_index: segment.zone_index,
            unit_offset: segment.unit_offset,
            allocation_ts: segment.allocation_ts,
        }
        .to_bytes();
        assert!(
            matches!(
                kv.get(0, 1, &key, ReadMode::Linearizable, None).await.unwrap(),
                GetOutcome::Found { .. }
            ),
            "layout removal must persist exact allocation free fact: {segment:?}"
        );
    }
}

pub(super) async fn allocate_protected(client: &ChunkdbClient) -> crowdb_protocol::chunkdb::rpc::Chunk {
    let chunk = client
        .allocate_chunk(AllocateChunkRequest {
            write_granularity: 1,
            strip_count: 3,
            strip_type: StripType::Mirror as i32,
            copy_count: 3,
            chunk_type: ChunkType::S3 as i32,
            ..Default::default()
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    assert_eq!(chunk.state, ChunkState::Active as i32);
    assert_eq!(chunk.strips.len(), 3);
    for strip in &chunk.strips {
        let assessment = strip.placement_assessment.as_ref().unwrap();
        assert_eq!(assessment.loss_budget, 2);
        assert_eq!(assessment.max_fragments_per_rack, 1);
        assert_eq!(assessment.max_fragments_per_node, 1);
        assert_eq!(assessment.max_fragments_per_disk, 1);
        assert!(assessment.rack_protected && assessment.node_protected && assessment.disk_protected);
        assert!(!strip.placement_repair_required);
    }
    chunk
}
