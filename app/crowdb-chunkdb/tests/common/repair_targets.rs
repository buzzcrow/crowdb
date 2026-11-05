// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crate::common::cluster::{
    seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster, DATA_GROUP_ID, STORE_ID,
};
use crate::common::diskio_stub::TestDiskio;
use crowdb_chunkdb::conversion::io::ConversionDiskIo;
use crowdb_chunkdb::metrics::ChunkdbMetrics;
use crowdb_chunkdb::repair::{decode_payload, encode_payload, RepairCoordinator, RepairStripTaskHandler};
use crowdb_chunkdb::routing::{default_binding_table, BindingCache};
use crowdb_chunkdb::task::{TaskExecutor, TaskManager, TaskStore};
use crowdb_chunkdb::topology::build_snapshot;
use crowdb_common::metrics::MetricsRegistry;
use crowdb_paxos_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::chunk_task::{ChunkTaskValue, RepairTargetCheckpoint, RepairTargetPhase};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkType, Strip, StripType};
use crowdb_protocol::common::{ChunkId, InstanceValue};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::key::InstanceKey;

pub struct TestRepairTargets {
    pub cluster: KvCluster,
    pub harness: ChunkdbHarness,
    pub tasks: Arc<TaskStore>,
    pub manager: Arc<TaskManager>,
    pub io: Arc<ConversionDiskIo>,
    pub chunk: Chunk,
    pub failed: Segment,
    pub survivor: Segment,
    pub target: Segment,
    pub task: ChunkTaskValue,
    disks: Vec<(u64, Option<TestDiskio>)>,
    _diskdb: DiskdbServer,
}

impl TestRepairTargets {
    pub async fn start() -> Self {
        let cluster = KvCluster::start_pinned().await;
        let hardware = cluster.make_hardware_client();
        seed_hardware(&hardware).await;
        let diskdb = DiskdbServer::start(&cluster).await;
        let registry = cluster.make_service_registry_client();
        let mut disks = Vec::new();
        for group in hardware.list_disk_groups().await.unwrap() {
            let disk =
                TestDiskio::start(&registry, group.dg_id, group.rack_id, group.node_id, group.dg_id).await;
            disks.push((group.dg_id, Some(disk)));
        }
        let harness = ChunkdbHarness::start(&cluster).await;
        let chunk = harness
            .handler
            .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 2, ChunkType::S3, 0, 0)
            .await
            .unwrap();
        let Strip::MirrorStrip(mirror) = chunk.strips[0].strip.as_ref().unwrap() else {
            panic!("expected mirrored chunk");
        };
        let (failed, survivor) = (mirror.segments[0], mirror.segments[1]);
        let id = chunk.id.unwrap();
        let target = harness
            .handler
            .allocate_repair_segment(
                &id,
                &failed,
                &[survivor],
                &[failed.disk_id.unwrap(), survivor.disk_id.unwrap()],
                false,
            )
            .await
            .unwrap();
        let io = Arc::new(ConversionDiskIo::connect(&registry, &hardware).await.unwrap());
        let unit = u64::from(chunk.strips[0].unit_kb) * 1024;
        let data = bytes::Bytes::from(vec![42; usize::try_from(unit).unwrap()]);
        io.write_segment(&survivor, unit, data).await.unwrap();
        io.fsync_segment(&survivor).await.unwrap();
        let marked = mark_failed(&harness, &chunk, failed).await;
        let bindings = BindingCache::new();
        bindings
            .replace(default_binding_table(STORE_ID, DATA_GROUP_ID))
            .unwrap();
        let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
        let task = persist_allocated_target(&harness, &tasks, &marked, target).await;
        let manager = Arc::new(TaskManager::new(Arc::clone(&tasks), 8001, 30_000));
        Self {
            cluster,
            harness,
            tasks,
            manager,
            io,
            chunk: marked,
            failed,
            survivor,
            target,
            task,
            disks,
            _diskdb: diskdb,
        }
    }

    pub async fn stop_target(&mut self, expire_registration: bool) {
        let group = self
            .harness
            .topology
            .snapshot()
            .disk_location(self.target.disk_id.unwrap())
            .unwrap()
            .disk_group_id;
        let key = InstanceKey {
            service: "diskio".into(),
            instance_id: group,
        }
        .to_path();
        if expire_registration {
            let kv = self.cluster.make_crowdb_client();
            let GetOutcome::Found { value, .. } = kv
                .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
                .await
                .unwrap()
            else {
                panic!("missing DiskIO registration");
            };
            let mut instance: InstanceValue = serde_json::from_slice(&value).unwrap();
            instance.last_heartbeat_ms = 0;
            kv.put(
                0,
                0,
                key.as_bytes(),
                &serde_json::to_vec(&instance).unwrap(),
                None,
            )
            .await
            .unwrap();
        }
        self.disks
            .iter_mut()
            .find(|(id, _)| *id == group)
            .unwrap()
            .1
            .take();
        let snapshot = build_snapshot(&self.cluster.make_hardware_client())
            .await
            .unwrap();
        assert_eq!(
            snapshot.healthy_disk_groups().iter().any(|g| g.dg_id == group),
            !expire_registration
        );
        self.harness.topology.replace(snapshot);
    }

    pub async fn execute(&self) -> ChunkTaskValue {
        let ready = self.tasks.scan_ready(now_ms(), 16).await.unwrap();
        let index = ready
            .iter()
            .find(|index| index.task_id == self.task.task_id)
            .unwrap();
        let claim = self.manager.claim(index, now_ms()).await.unwrap().unwrap();
        let metrics = ChunkdbMetrics::register(&mut MetricsRegistry::new()).repair;
        let worker = RepairStripTaskHandler::new(
            Arc::clone(&self.harness.handler),
            Arc::clone(&self.manager),
            Arc::clone(&self.io),
            16 * 1024 * 1024,
            1,
            false,
            metrics,
        );
        TaskExecutor::new(Arc::clone(&self.manager), 1, vec![Arc::new(worker)])
            .unwrap()
            .execute(claim)
            .await
            .unwrap();
        self.tasks
            .get(&self.task.partition_id, self.task.kind, &self.task.task_id)
            .await
            .unwrap()
            .unwrap()
    }

    pub async fn publish_before_checkpoint(&self) -> Chunk {
        let unit = u64::from(self.chunk.strips[0].unit_kb) * 1024;
        let bytes = self.io.read_segment(&self.survivor, unit).await.unwrap();
        self.io.write_segment(&self.target, unit, bytes).await.unwrap();
        self.io.fsync_segment(&self.target).await.unwrap();
        let mut replacement = self.chunk.strips[0].clone();
        let Strip::MirrorStrip(mirror) = replacement.strip.as_mut().unwrap() else {
            unreachable!()
        };
        *mirror
            .segments
            .iter_mut()
            .find(|segment| **segment == self.failed)
            .unwrap() = self.target;
        replacement.unavailable_segments.clear();
        self.harness
            .handler
            .publish_tentative_chunk_strip_range(
                &self.chunk.id.unwrap(),
                self.chunk.modify_ts,
                0,
                &self.chunk.strips,
                &[replacement],
                self.task.operation_id,
            )
            .await
            .unwrap()
    }
}

async fn mark_failed(harness: &ChunkdbHarness, chunk: &Chunk, failed: Segment) -> Chunk {
    let mut marked = chunk.strips[0].clone();
    marked.unavailable_segments.push(failed);
    harness
        .handler
        .replace_chunk_strip_range(
            &chunk.id.unwrap(),
            chunk.modify_ts,
            0,
            &chunk.strips,
            &[marked],
            ChunkId { high: 111, low: 1 },
        )
        .await
        .unwrap()
}

async fn persist_allocated_target(
    harness: &ChunkdbHarness,
    tasks: &Arc<TaskStore>,
    chunk: &Chunk,
    target: Segment,
) -> ChunkTaskValue {
    RepairCoordinator::new(Arc::clone(&harness.handler), Arc::clone(tasks))
        .admit_chunk(chunk, now_ms())
        .await
        .unwrap();
    let ready = tasks.scan_ready(now_ms(), 16).await.unwrap();
    let index = ready
        .iter()
        .find(|index| index.kind == crowdb_protocol::chunk_task::TASK_KIND_REPAIR_STRIP)
        .unwrap();
    let task = tasks
        .get(&chunk.id.unwrap(), index.kind, &index.task_id)
        .await
        .unwrap()
        .unwrap();
    let mut payload = decode_payload(&task.payload).unwrap();
    payload.targets.push(RepairTargetCheckpoint {
        source: payload.failed_segments[0],
        destination: target,
        phase: RepairTargetPhase::Allocated,
    });
    let mut checkpoint = task.clone();
    checkpoint.revision += 1;
    checkpoint.payload = encode_payload(&payload).unwrap();
    tasks.write_transition(Some(&task), &checkpoint).await.unwrap();
    checkpoint
}

fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}
