// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Remaining host process-outage timing proof pending the container fault budget.

#[path = "common/e2e_stack.rs"]
mod e2e_stack;

use bytes::Bytes;
use crowdb_chunk_client::{ChunkIoWriter, SmallWritePolicy};
use e2e_stack::{all_binaries_available, E2eStack};
use std::time::Duration;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
use crowdb_protocol::frame::MAX_FRAME_PAYLOAD_BYTES;

fn policy() -> SmallWritePolicy {
    SmallWritePolicy {
        memory_budget: 16 * MIB,
        queue_capacity: 256,
        min_pipelines: 1,
        max_pipelines: 1,
        scale_out_queue_bytes: 16 * MIB,
        scale_out_queue_objects: 256,
        scale_in_delay: Duration::from_millis(50),
        control_interval: Duration::from_millis(1),
        cooldown: Duration::from_millis(1),
        chunk_capacity: 2 * MIB as u64,
        object_limit: MAX_FRAME_PAYLOAD_BYTES,
        mirror_copies: 1,
        ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
    }
}

#[tokio::test]
async fn single_copy_read_reports_diskio_process_failure() {
    if !all_binaries_available() {
        return;
    }
    let mut stack = E2eStack::start(policy()).await;
    let data = Bytes::from_static(b"diskio-read-failure");
    let mut writer = stack.client.prepare_small_write(data.len()).await.unwrap();
    writer.on_data(data.clone()).await.unwrap();
    let location = writer.on_finish().await.unwrap().remove(0);
    stack.client.shutdown_small_writes().await.unwrap();
    assert_eq!(
        stack
            .client
            .read_object(std::slice::from_ref(&location))
            .await
            .unwrap()
            .concat()
            .as_slice(),
        data.as_ref()
    );
    stack.crash_diskio();
    let read = tokio::time::timeout(
        Duration::from_secs(15),
        stack.client.read_object(std::slice::from_ref(&location)),
    )
    .await
    .expect("read did not return after DiskIO exited");
    assert!(
        read.is_err(),
        "single-copy read succeeded after its DiskIO process exited"
    );
}
