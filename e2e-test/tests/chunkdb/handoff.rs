// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_diskdb_client::{DiskdbClient, DiskdbRpcTransport};
use crowdb_kv_client::{CrowdbKvClient, GetOutcome, ReadMode, ServiceRegistryClient};
use crowdb_protocol::chunk_task::relocation_operation_id;
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateReplacementSegmentRequest, ChunkType, QueryChunkRequest, Strip, StripType,
};
use crowdb_protocol::diskdb::rpc::{ExecuteRelocationRequest, RelocationJournalPhase, Segment};
use crowdb_protocol::key::{BinaryKey, FreeBlockKey, RelocationJournalKey};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test]
#[ignore = "requires the owned real relocation storage fixture"]
async fn relocation_journal_copies_publishes_then_frees_exact_source() {
    let (client, io) = super::conversion::clients().await;
    let chunk = client
        .allocate_chunk(AllocateChunkRequest {
            write_granularity: 1024,
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
    let id = chunk.id.unwrap();
    let Strip::MirrorStrip(mirror) = chunk.strips[0].strip.as_ref().unwrap() else {
        panic!("expected mirror")
    };
    let source = mirror.segments[0];
    let target = client
        .allocate_replacement_segment(AllocateReplacementSegmentRequest {
            chunk_id: Some(id),
            old_segment: Some(source),
            surviving_segments: mirror.segments[1..].to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .segment
        .unwrap();
    let unit = u64::from(chunk.strips[0].unit_kb) * 1024;
    let data = Bytes::from(
        (0..1024 * 1024)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect::<Vec<_>>(),
    );
    for segment in &mirror.segments {
        io.write_segment(segment, unit, data.clone()).await.unwrap();
        io.fsync_segment(segment).await.unwrap();
        assert_eq!(io.read_segment(segment, unit).await.unwrap(), data);
    }
    let kv = crate::common::hardware::kv_client();
    assert!(!source_freed(&kv, source).await);
    assert_eq!(crate::common::control::node("pause", 4).await["Paused"], true);
    let diskdb = DiskdbClient::new(
        ServiceRegistryClient::new(crate::common::hardware::kv_client()),
        Arc::new(DiskdbRpcTransport::new()),
    );
    let accepted = diskdb
        .execute_relocation(ExecuteRelocationRequest {
            target_disk_group_id: target.disk_id.unwrap().low / 10,
            source: Some(source),
            target: Some(target),
        })
        .await
        .unwrap();
    let operation = relocation_operation_id(&source).unwrap();
    assert_eq!(accepted.operation_id, Some(operation));
    assert!(!source_freed(&kv, source).await);
    assert_eq!(crate::common::control::node("unpause", 4).await["Paused"], false);
    retired(&kv, source, target, operation).await;
    let published = client
        .query_chunk(QueryChunkRequest { chunk_id: Some(id) })
        .await
        .unwrap()
        .chunk
        .unwrap();
    let Strip::MirrorStrip(after) = published.strips[0].strip.as_ref().unwrap() else {
        panic!("publication must preserve mirror")
    };
    assert_eq!(after.segments.len(), mirror.segments.len());
    assert!(!after.segments.contains(&source));
    assert!(after.segments.contains(&target));
    for healthy in &mirror.segments[1..] {
        assert!(after.segments.contains(healthy));
    }
    let protection = published.strips[0].placement_assessment.as_ref().unwrap();
    assert!(protection.rack_protected && protection.node_protected && protection.disk_protected);
    assert!(published.cleanup_intents.is_empty());
    for segment in &after.segments {
        assert_eq!(io.read_segment(segment, unit).await.unwrap(), data);
    }
    assert!(source_freed(&kv, source).await);
}

async fn source_freed(kv: &CrowdbKvClient, source: Segment) -> bool {
    let key = FreeBlockKey {
        disk_id: source.disk_id.unwrap(),
        zone_index: source.zone_index,
        unit_offset: source.unit_offset,
        allocation_ts: source.allocation_ts,
    }
    .to_bytes();
    matches!(
        kv.get(0, 1, &key, ReadMode::Linearizable, None).await.unwrap(),
        GetOutcome::Found { .. }
    )
}

async fn retired(
    kv: &CrowdbKvClient,
    source: Segment,
    target: Segment,
    operation: crowdb_protocol::common::ChunkId,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let key = RelocationJournalKey {
            disk_id: source.disk_id.unwrap(),
            zone_index: source.zone_index,
            unit_offset: source.unit_offset,
            allocation_ts: source.allocation_ts,
        }
        .to_bytes();
        let GetOutcome::Found { value, .. } = kv.get(0, 1, &key, ReadMode::Linearizable, None).await.unwrap()
        else {
            panic!("accepted relocation journal missing")
        };
        let journal: crowdb_protocol::diskdb::rpc::RelocationJournalValue =
            bincode::deserialize(&value).unwrap();
        assert_eq!(journal.operation_id, Some(operation));
        assert_eq!(journal.source, Some(source));
        assert_eq!(journal.target, Some(target));
        if RelocationJournalPhase::try_from(journal.phase) == Ok(RelocationJournalPhase::SourceFreed) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "relocation did not retire source: {journal:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
