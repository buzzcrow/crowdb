// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Actual KV/DDB/DiskIO conversion and repair of byte-sealed partial EC data.

mod common;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use common::cluster::{
    seed_hardware_layout, ChunkdbHarness, DiskdbServer, KvCluster, DATA_GROUP_ID, STORE_ID,
};
use crowdb_chunkdb::conversion::io::ConversionDiskIo;
use crowdb_chunkdb::conversion::{ConversionCoordinator, MirrorToEcTaskHandler};
use crowdb_chunkdb::metrics::ChunkdbMetrics;
use crowdb_chunkdb::repair::{RepairCoordinator, RepairStripTaskHandler};
use crowdb_chunkdb::routing::{default_binding_table, BindingCache};
use crowdb_chunkdb::task::{TaskClaim, TaskExecutor, TaskManager, TaskStore};
use crowdb_common::ec::{encode_parity_from_shards, EcScheme};
use crowdb_common::metrics::MetricsRegistry;
use crowdb_protocol::chunk_task::{ChunkTaskState, TASK_KIND_MIRROR_TO_EC, TASK_KIND_REPAIR_STRIP};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkType, Strip, StripType};
use crowdb_test_harness::diskio::{DiskioGroup0Identity, DiskioProcess, DiskioStartOpts};

fn now_ms() -> u64 {
    u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap()
}

fn start_disks(cluster: &KvCluster) -> Vec<DiskioProcess> {
    (0..6)
        .map(|index| {
            DiskioProcess::start_for_group(
                &DiskioStartOpts {
                    dummy_disk: "mem",
                    kv_seeds: &cluster.mgmt_endpoints,
                    disks: &[],
                    fault_error_rate: 0.0,
                    fault_latency_ms: None,
                    no_o_direct: false,
                },
                DiskioGroup0Identity {
                    instance_id: 8000 + index,
                    rack_id: 100 + index,
                    node_id: 10 + index,
                    disk_group_id: 1000 + index,
                },
            )
        })
        .collect()
}

async fn assert_ec_bytes(io: &ConversionDiskIo, chunk: &Chunk, expected: &[Vec<u8>], stale_tail: bool) {
    let strip = &chunk.strips[0];
    let Some(Strip::EcStrip(ec)) = &strip.strip else {
        panic!("expected EC strip")
    };
    let unit = u64::from(strip.unit_kb) * 1024;
    for (index, segment) in ec.segments.iter().enumerate() {
        let bytes = io.read_segment(segment, unit).await.unwrap();
        assert_eq!(&bytes[..expected[index].len()], expected[index], "shard {index}");
        if stale_tail && expected[index].len() < bytes.len() {
            assert!(bytes[expected[index].len()..].iter().all(|byte| *byte == 0xa5));
        }
    }
}

#[tokio::test]
async fn partial_conversion_and_four_shard_repair_preserve_exact_bytes() {
    let cluster = KvCluster::start().await;
    let groups = seed_hardware_layout(
        &cluster.make_hardware_client(),
        &[
            (100, vec![10]),
            (101, vec![11]),
            (102, vec![12]),
            (103, vec![13]),
            (104, vec![14]),
            (105, vec![15]),
        ],
    )
    .await;
    let _diskdb = DiskdbServer::start_with_disk_groups_and_zones(&cluster, &groups, 4).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let _disks = start_disks(&cluster);
    let io = Arc::new(ConversionDiskIo::deferred(
        crowdb_chunkdb::chunkdb_config::ConversionIoConfig::default(),
    ));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        match io
            .refresh(
                &cluster.make_service_registry_client(),
                &cluster.make_hardware_client(),
            )
            .await
        {
            Ok(()) => break,
            Err(error) => assert!(
                tokio::time::Instant::now() < deadline,
                "DiskIO owners not ready: {error}"
            ),
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let chunk = harness
        .handler
        .allocate_chunk(None, 1024, 8, StripType::Mirror, 0, 0, 3, ChunkType::S3, 0, 0)
        .await
        .unwrap();
    let id = chunk.id.unwrap();
    let shard = usize::try_from(chunk.strips[0].capacity).unwrap() * 1024;
    let payload: Vec<u8> = (0..7 * shard + 17)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    let mut expected: Vec<Vec<u8>> = payload.chunks(shard).map(<[u8]>::to_vec).collect();
    for (strip, data) in chunk.strips.iter().zip(&expected) {
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            panic!("expected mirror")
        };
        let unit = u64::from(strip.unit_kb) * 1024;
        for segment in &mirror.segments {
            io.write_segment(segment, unit, Bytes::from(vec![0xa5; shard]))
                .await
                .unwrap();
            io.write_segment(segment, unit, Bytes::copy_from_slice(data))
                .await
                .unwrap();
            io.fsync_segment(segment).await.unwrap();
        }
    }
    let chunk = harness
        .handler
        .seal_chunk_bytes(&id, payload.len() as u64)
        .await
        .unwrap();
    let bindings = BindingCache::new();
    bindings
        .replace(default_binding_table(STORE_ID, DATA_GROUP_ID))
        .unwrap();
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let mut padded = expected.clone();
    for data in &mut padded {
        data.resize(shard, 0);
    }
    let refs: Vec<_> = padded.iter().map(Vec::as_slice).collect();
    expected.extend(encode_parity_from_shards(EcScheme::new(8, 4), &refs).unwrap());
    let mut registry = MetricsRegistry::new();
    let metrics = ChunkdbMetrics::register(&mut registry);
    let converted = convert(&harness, tasks.clone(), io.clone(), &chunk, metrics.conversion).await;
    assert_eq!(converted.acknowledged_cursor, payload.len() as u64);
    assert_ec_bytes(&io, &converted, &expected, true).await;
    let repaired = repair(&harness, tasks, io.clone(), &converted, metrics.repair).await;
    assert!(repaired.strips[0].unavailable_segments.is_empty());
    assert_eq!(repaired.acknowledged_cursor, payload.len() as u64);
    assert_ec_bytes(&io, &repaired, &expected, false).await;
}

async fn convert(
    harness: &ChunkdbHarness,
    tasks: Arc<TaskStore>,
    io: Arc<ConversionDiskIo>,
    chunk: &Chunk,
    metrics: Arc<crowdb_chunkdb::metrics::ConversionMetrics>,
) -> Chunk {
    let id = chunk.id.unwrap();
    let shard = usize::try_from(chunk.strips[0].capacity).unwrap() * 1024;
    let prepared = ConversionCoordinator::new(harness.handler.clone(), tasks.clone())
        .prepare(
            id,
            chunk.modify_ts,
            0,
            chunk.strips.clone(),
            8,
            4,
            7001,
            30_000,
            now_ms(),
        )
        .await
        .unwrap();
    let strip = &prepared.replacement_strip;
    let Some(Strip::EcStrip(ec)) = &strip.strip else {
        panic!("expected replacement EC")
    };
    for segment in &ec.segments {
        io.write_segment(
            segment,
            u64::from(strip.unit_kb) * 1024,
            Bytes::from(vec![0xa5; shard]),
        )
        .await
        .unwrap();
    }
    let manager = Arc::new(TaskManager::new(tasks.clone(), 7001, 30_000));
    let executor = TaskExecutor::new(
        manager.clone(),
        1,
        vec![Arc::new(MirrorToEcTaskHandler::new(
            harness.handler.clone(),
            tasks.clone(),
            io.clone(),
            metrics,
            50,
            1,
        ))],
    )
    .unwrap();
    let task = tasks
        .get(&id, TASK_KIND_MIRROR_TO_EC, &prepared.task_id)
        .await
        .unwrap()
        .unwrap();
    executor
        .execute(TaskClaim::from_task_for_tests(task))
        .await
        .unwrap();
    assert_eq!(
        tasks
            .get(&id, TASK_KIND_MIRROR_TO_EC, &prepared.task_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        ChunkTaskState::Completed
    );
    harness.handler.query_chunk(&id).await.unwrap()
}

async fn repair(
    harness: &ChunkdbHarness,
    tasks: Arc<TaskStore>,
    io: Arc<ConversionDiskIo>,
    converted: &Chunk,
    metrics: Arc<crowdb_chunkdb::metrics::RepairMetrics>,
) -> Chunk {
    let id = converted.id.unwrap();
    let manager = Arc::new(TaskManager::new(tasks.clone(), 7001, 30_000));
    let mut failed = converted.strips[0].clone();
    let Some(Strip::EcStrip(ec)) = &failed.strip else {
        panic!("expected EC")
    };
    failed.unavailable_segments = [0, 7, 8, 9].map(|index| ec.segments[index]).to_vec();
    let failed = harness.handler.update_chunk_strip(&id, 0, failed).await.unwrap();
    assert_eq!(
        RepairCoordinator::new(harness.handler.clone(), tasks.clone())
            .admit_chunk(&failed, now_ms())
            .await
            .unwrap(),
        1
    );
    let ready = tasks.scan_ready(now_ms(), 16).await.unwrap();
    let index = ready
        .iter()
        .find(|index| index.kind == TASK_KIND_REPAIR_STRIP)
        .unwrap();
    let claim = manager.claim(index, now_ms()).await.unwrap().unwrap();
    let task_id = claim.task.task_id;
    let repair = TaskExecutor::new(
        manager,
        1,
        vec![Arc::new(RepairStripTaskHandler::new(
            harness.handler.clone(),
            Arc::new(TaskManager::new(tasks.clone(), 7001, 30_000)),
            io.clone(),
            64 * 1024 * 1024,
            1,
            true,
            metrics,
        ))],
    )
    .unwrap();
    repair.execute(claim).await.unwrap();
    let finished = tasks
        .get(&id, TASK_KIND_REPAIR_STRIP, &task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        finished.state,
        ChunkTaskState::Completed,
        "{}",
        finished.last_error
    );
    harness.handler.query_chunk(&id).await.unwrap()
}
