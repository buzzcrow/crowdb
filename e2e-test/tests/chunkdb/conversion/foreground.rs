// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunkdb::task::TaskStore;
use crowdb_protocol::chunk_task::{ChunkTaskState, TASK_KIND_MIRROR_TO_EC};
use crowdb_protocol::chunkdb::rpc::{PrepareMirrorToEcConversionRequest, SealChunkRequest, Strip};

#[tokio::test]
#[ignore = "requires packaged background recovery of an expired foreground conversion claim"]
async fn expired_client_claim_preserves_dirty_destination_tail_and_completes_once() {
    let (client, io) = super::clients().await;
    let (chunk, expected, length) = super::prepare_partial(&client, &io).await;
    let id = chunk.id.unwrap();
    let sealed = client
        .seal_chunk(SealChunkRequest {
            chunk_id: Some(id),
            seal_bytes: length,
            ..Default::default()
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    let prepared = client
        .prepare_mirror_to_ec_conversion(PrepareMirrorToEcConversionRequest {
            chunk_id: Some(id),
            expected_modify_ts: sealed.modify_ts,
            start_index: 0,
            old_strips: sealed.strips.clone(),
            data_num: 8,
            code_num: 4,
            client_owner: 7001,
            claim_lease_ms: 5000,
        })
        .await
        .unwrap();
    let replacement = prepared.replacement_strip.unwrap();
    let Strip::EcStrip(before) = replacement.strip.as_ref().unwrap() else {
        panic!("prepared replacement must be EC")
    };
    let tasks = crate::chunkdb::tasks::store().await;
    let task_id = prepared.task_id.unwrap();
    let running = tasks
        .get(&id, TASK_KIND_MIRROR_TO_EC, &task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(running.state, ChunkTaskState::Running);
    assert_eq!(running.claim_owner, 7001);
    assert_eq!(running.claim_generation, 1);
    assert_eq!(Some(running.operation_id), prepared.operation_id);
    dirty_replacement(&io, &replacement, expected[0].len()).await;
    let current = tasks
        .get(&id, TASK_KIND_MIRROR_TO_EC, &task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        current.state,
        ChunkTaskState::Running,
        "dirty destinations precede claim expiry"
    );
    assert_eq!(current.claim_owner, 7001);
    let converted = super::wait_converted(&client, &id).await;
    assert_eq!(converted.acknowledged_cursor, length);
    let Strip::EcStrip(after) = converted.strips[0].strip.as_ref().unwrap() else {
        unreachable!()
    };
    assert_eq!(
        after.segments, before.segments,
        "takeover reuses the prepared allocation"
    );
    super::assert_shards_with_tail(&io, &converted, expected, 0xa5).await;
    assert_completed(&tasks, id, task_id, &running).await;
}

async fn dirty_replacement(
    io: &crowdb_chunkdb::conversion::io::ConversionDiskIo,
    replacement: &crowdb_protocol::chunkdb::rpc::ChunkStrip,
    shard: usize,
) {
    let Strip::EcStrip(before) = replacement.strip.as_ref().unwrap() else {
        unreachable!()
    };
    for segment in &before.segments {
        io.write_segment(
            segment,
            u64::from(replacement.unit_kb) * 1024,
            Bytes::from(vec![0xa5; shard]),
        )
        .await
        .unwrap();
        io.fsync_segment(segment).await.unwrap();
        assert!(io
            .read_segment(segment, u64::from(replacement.unit_kb) * 1024)
            .await
            .unwrap()
            .iter()
            .all(|byte| *byte == 0xa5));
    }
}

async fn assert_completed(
    tasks: &TaskStore,
    id: crowdb_protocol::common::ChunkId,
    task_id: crowdb_protocol::common::ChunkId,
    running: &crowdb_protocol::chunk_task::ChunkTaskValue,
) {
    let completed = crate::chunkdb::tasks::completed(tasks, id, TASK_KIND_MIRROR_TO_EC, task_id).await;
    assert_eq!(
        completed.state,
        ChunkTaskState::Completed,
        "{}",
        completed.last_error
    );
    assert!(completed.claim_generation > running.claim_generation);
    assert!(completed.attempt > running.attempt);
    assert_eq!(completed.operation_id, running.operation_id);
    let conversion_tasks: Vec<_> = tasks
        .list_partition(&id)
        .await
        .unwrap()
        .into_iter()
        .filter(|task| task.kind == TASK_KIND_MIRROR_TO_EC)
        .collect();
    assert_eq!(conversion_tasks.len(), 1);
}
