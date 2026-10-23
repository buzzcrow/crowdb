// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod topology;

use crowdb_protocol::chunkdb::rpc::{QueryChunkRequest, ReplaceChunkStripRangeRequest, Strip};
use crowdb_protocol::common::ChunkId;
use std::time::{Duration, Instant};

#[tokio::test]
#[ignore = "requires the owned protected storage repair container fixture"]
async fn four_missing_shards_repair_partial_data_and_parity() {
    let (client, io, converted, expected) = super::conversion::converted_partial().await;
    super::conversion::assert_shards(&io, &converted, expected.clone()).await;
    let id = converted.id.unwrap();
    let mut damaged = converted.strips[0].clone();
    let Strip::EcStrip(before) = damaged.strip.as_ref().unwrap() else {
        panic!("expected EC")
    };
    let missing: Vec<_> = [0, 7, 8, 9].map(|index| before.segments[index]).to_vec();
    damaged.unavailable_segments = missing.clone();
    let marked = client
        .replace_chunk_strip_range(ReplaceChunkStripRangeRequest {
            chunk_id: Some(id),
            expected_modify_ts: converted.modify_ts,
            start_index: 0,
            old_strips: vec![converted.strips[0].clone()],
            replacement_strips: vec![damaged],
            operation_id: Some(ChunkId {
                high: id.high ^ converted.modify_ts,
                low: id.low,
            }),
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    assert_eq!(marked.strips[0].unavailable_segments, missing);
    let deadline = Instant::now() + Duration::from_secs(30);
    let repaired = loop {
        let current = client
            .query_chunk(QueryChunkRequest { chunk_id: Some(id) })
            .await
            .unwrap()
            .chunk
            .unwrap();
        if current.strips[0].unavailable_segments.is_empty() {
            break current;
        }
        assert!(Instant::now() < deadline, "repair did not publish: {current:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(repaired.acknowledged_cursor, converted.acknowledged_cursor);
    assert_eq!(repaired.strips.len(), 1);
    let Strip::EcStrip(after) = repaired.strips[0].strip.as_ref().unwrap() else {
        panic!("repair must preserve EC")
    };
    let Strip::EcStrip(before) = converted.strips[0].strip.as_ref().unwrap() else {
        unreachable!()
    };
    for (index, segment) in after.segments.iter().enumerate() {
        if [0, 7, 8, 9].contains(&index) {
            assert_ne!(*segment, before.segments[index]);
            assert!(!missing.contains(segment));
        } else {
            assert_eq!(*segment, before.segments[index], "healthy shard {index} changed");
        }
    }
    super::conversion::assert_shards(&io, &repaired, expected).await;
    let tasks = super::tasks::store().await;
    let repair_tasks: Vec<_> = tasks
        .list_partition(&id)
        .await
        .unwrap()
        .into_iter()
        .filter(|task| task.kind == crowdb_protocol::chunk_task::TASK_KIND_REPAIR_STRIP)
        .collect();
    assert_eq!(repair_tasks.len(), 1);
    let task = super::tasks::completed(&tasks, id, repair_tasks[0].kind, repair_tasks[0].task_id).await;
    assert_eq!(task.state, crowdb_protocol::chunk_task::ChunkTaskState::Completed);
}
