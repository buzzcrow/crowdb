// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{hardware::DG_ID, make_chunk_id};
use crowdb_diskdb_client::DiskdbClient;
use crowdb_protocol::diskdb::rpc::{AllocateBlocksRequest, FreeBlocksRequest};
use std::sync::Arc;

pub(super) async fn run_concurrent_benchmark(client: &Arc<DiskdbClient>) {
    let mut tasks = Vec::new();
    for task in 0..4_u64 {
        let client = Arc::clone(client);
        tasks.push(tokio::spawn(async move {
            for cycle in 0..100_u64 {
                let allocation = client
                    .allocate_blocks(AllocateBlocksRequest {
                        disk_group_id: DG_ID,
                        unit_count: 1,
                        count: 1,
                        exclude_disk_ids: Vec::new(),
                        owner_chunk: Some(make_chunk_id(task, cycle)),
                        allow_disk_reuse: false,
                    })
                    .await
                    .expect("concurrent allocation");
                assert_eq!(allocation.segments.len(), 1);
                let freed = client
                    .free_blocks(FreeBlocksRequest {
                        segments: allocation.segments,
                    })
                    .await
                    .expect("concurrent free");
                assert_eq!(freed.freed_count, 1);
                assert!(freed.failures.is_empty());
            }
        }));
    }
    for task in tasks {
        task.await.expect("concurrent allocation/free task");
    }
}
