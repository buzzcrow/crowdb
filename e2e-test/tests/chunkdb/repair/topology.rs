// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv_client::HardwareClient;
use crowdb_protocol::chunkdb::rpc::{AllocateChunkRequest, ChunkType, QueryChunkRequest, Strip, StripType};
use crowdb_protocol::common::HwStatus;
use std::time::{Duration, Instant};

#[tokio::test]
#[ignore = "requires the owned storage topology recovery fixture"]
async fn degraded_ec_recovers_protection_and_exact_bytes_after_node_returns() {
    let (client, io) = super::super::conversion::clients().await;
    let hw = HardwareClient::new(crate::common::hardware::kv_client());
    assert_eq!(crate::common::control::node("kill", 4).await["Running"], false);
    for index in 2..6 {
        hw.set_node_status(100 + index, 10 + index, HwStatus::Offline)
            .await
            .unwrap();
    }
    assert_eq!(crate::common::control::node("start", 4).await["Running"], true);
    ready().await;
    let chunk = client
        .allocate_chunk(AllocateChunkRequest {
            write_granularity: 1024,
            strip_count: 1,
            strip_type: StripType::Ec as i32,
            data_num: 2,
            code_num: 1,
            chunk_type: ChunkType::S3 as i32,
            ..Default::default()
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    let id = chunk.id.unwrap();
    assert!(chunk.strips[0].placement_repair_required);
    assert!(
        !chunk.strips[0]
            .placement_assessment
            .as_ref()
            .unwrap()
            .node_protected
    );
    let Strip::EcStrip(ec) = chunk.strips[0].strip.as_ref().unwrap() else {
        panic!("expected degraded EC")
    };
    let unit = u64::from(chunk.strips[0].unit_kb) * 1024;
    let data = Bytes::from(vec![0x5a; 1024 * 1024]);
    for segment in &ec.segments {
        io.write_segment(segment, unit, data.clone()).await.unwrap();
        io.fsync_segment(segment).await.unwrap();
        assert_eq!(io.read_segment(segment, unit).await.unwrap(), data);
    }
    let tasks = super::super::tasks::store().await;
    let pending = attempted(&tasks, id).await;
    assert_ne!(
        pending.state,
        crowdb_protocol::chunk_task::ChunkTaskState::Completed
    );
    assert!(
        client
            .query_chunk(QueryChunkRequest { chunk_id: Some(id) })
            .await
            .unwrap()
            .chunk
            .unwrap()
            .strips[0]
            .placement_repair_required
    );
    hw.set_node_status(102, 12, HwStatus::Up).await.unwrap();
    let repaired = repaired(&client, id).await;
    let assessment = repaired.strips[0].placement_assessment.as_ref().unwrap();
    assert!(assessment.rack_protected && assessment.node_protected && assessment.disk_protected);
    let Strip::EcStrip(ec) = repaired.strips[0].strip.as_ref().unwrap() else {
        panic!("repair must preserve EC")
    };
    assert_eq!((ec.data_num, ec.code_num, ec.segments.len()), (2, 1, 3));
    for segment in &ec.segments {
        assert_eq!(io.read_segment(segment, unit).await.unwrap(), data);
    }
    super::super::tasks::completed(&tasks, id, pending.kind, pending.task_id).await;
}

async fn ready() {
    let addresses: Vec<String> = serde_json::from_str(&std::env::var("CROWDB_E2E_RPC").unwrap()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) = reqwest::get(format!("http://{}:7301/ready", addresses[4])).await {
            if response.status().is_success() {
                return;
            }
        }
        assert!(Instant::now() < deadline, "restarted Chunkdb is not ready");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn attempted(
    tasks: &crowdb_chunkdb::task::TaskStore,
    id: crowdb_protocol::common::ChunkId,
) -> crowdb_protocol::chunk_task::ChunkTaskValue {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let found = tasks.list_partition(&id).await.unwrap().into_iter().find(|task| {
            task.kind == crowdb_protocol::chunk_task::TASK_KIND_REPAIR_PLACEMENT && task.attempt > 0
        });
        if let Some(task) = found {
            return task;
        }
        assert!(
            Instant::now() < deadline,
            "degraded placement task was not attempted"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn repaired(
    client: &crowdb_chunkdb_client::ChunkdbClient,
    id: crowdb_protocol::common::ChunkId,
) -> crowdb_protocol::chunkdb::rpc::Chunk {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let current = client
            .query_chunk(QueryChunkRequest { chunk_id: Some(id) })
            .await
            .unwrap()
            .chunk
            .unwrap();
        if !current.strips[0].placement_repair_required {
            return current;
        }
        assert!(
            Instant::now() < deadline,
            "node return did not repair placement: {current:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
