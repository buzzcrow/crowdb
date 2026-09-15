// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[allow(dead_code)]
mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwap;
use common::cluster::KvCluster;
use crowdb_diskdb::bg_task::BgCtx;
use crowdb_diskdb::ddb_config::{DdbConfig, KeepAliveConfig, StorageDefaults};
use crowdb_diskdb::liveness::keepalive::KeepAlive;
use crowdb_diskdb::liveness::lifecycle::StartupPhase;
use crowdb_diskdb::metrics::RecalcEngine;
use crowdb_diskdb::model::alloc;
use crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer;
use crowdb_diskdb::rebalance::{
    RebalancePlannerTask, RebalanceZonePacer, RelocationIo, RelocationOwner, RelocationSourceFree,
    RelocationWorker,
};
use crowdb_diskdb::recovery::ZoneLoader;
use crowdb_diskdb::scanner::ScanState;
use crowdb_diskdb::service::DiskdbRpcService;
use crowdb_diskdb_client::DiskdbRpcTransport;
use crowdb_kv_client::HardwareClient;
use crowdb_protocol::chunkdb::rpc::{RelocateSegmentHandoffRequest, RelocationHandoffDisposition};
use crowdb_protocol::common::{ChunkId, DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{
    CommitState, DiskGroupValue, DiskType, DiskValue, RelocationJournalPhase, RelocationJournalValue,
    Segment, RELOCATION_JOURNAL_SCHEMA_VERSION,
};
use crowdb_protocol::key::RelocationJournalKey;

const DG_ID: u64 = 100;
const INSTANCE_ID: u64 = 999;
const ZONE_COUNT: u32 = 4;
const UNIT_SIZE: u32 = 1024 * 1024;

struct TestIo {
    copies: AtomicUsize,
}

impl RelocationIo for TestIo {
    fn copy_and_fsync<'a>(
        &'a self,
        _source: Segment,
        _target: Segment,
        unit_size: u32,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        self.copies.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            assert_eq!(unit_size, UNIT_SIZE);
            Ok(())
        })
    }
}

struct TestOwner {
    outcomes: RwLock<Vec<Result<RelocationHandoffDisposition, String>>>,
}

#[derive(Default)]
struct TestSourceFree {
    calls: AtomicUsize,
}

impl RelocationSourceFree for TestSourceFree {
    fn free_source<'a>(
        &'a self,
        _source: Segment,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
struct TestPacer {
    delays: RwLock<Vec<std::time::Duration>>,
}

impl RebalanceZonePacer for TestPacer {
    fn sleep(&self, duration: std::time::Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.delays.write().unwrap().push(duration);
        Box::pin(async {})
    }
}

impl RelocationOwner for TestOwner {
    fn handoff<'a>(
        &'a self,
        _request: RelocateSegmentHandoffRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RelocationHandoffDisposition, String>> + Send + 'a>> {
        Box::pin(async move { self.outcomes.write().unwrap().remove(0) })
    }
}

async fn seed_hardware(hw: &HardwareClient) {
    hw.add_rack(
        1,
        &RackValue {
            status: HwStatus::Up as i32,
            node_ids: vec![10],
        },
    )
    .await
    .unwrap();
    hw.add_node(
        1,
        10,
        &NodeValue {
            status: HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: vec![DG_ID],
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
        },
    )
    .await
    .unwrap();
    let disk_ids = vec![
        DiskId { high: 0, low: 1 },
        DiskId { high: 0, low: 2 },
        DiskId { high: 0, low: 3 },
    ];
    hw.add_disk_group(
        1,
        10,
        DG_ID,
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: disk_ids.clone(),
        },
    )
    .await
    .unwrap();
    for disk_id in disk_ids {
        hw.add_disk(
            1,
            10,
            DG_ID,
            &disk_id,
            &DiskValue {
                disk_type: DiskType::BlockSsd as i32,
                capacity_units: 512,
                zone_size_units: 128,
                unit_size_bytes: UNIT_SIZE,
                zone_count: ZONE_COUNT,
                status: HwStatus::Up as i32,
                device_path: String::new(),
            },
        )
        .await
        .unwrap();
    }
    hw.set_owner(1, 10, DG_ID, INSTANCE_ID, u64::MAX).await.unwrap();
    hw.set_bind(1, 10, DG_ID, 0, 1).await.unwrap();
}

#[tokio::test]
async fn relocation_journal_persists_and_lists_exact_source() {
    let cluster = KvCluster::start().await;
    let kv = cluster.make_ddb_kv_client();
    let owner = ChunkId { high: 80, low: 97 };
    let source = Segment {
        disk_id: Some(DiskId { high: 1, low: 2 }),
        zone_index: 3,
        unit_offset: 4,
        unit_count: 5,
        owner_chunk: Some(owner),
        allocation_ts: 6,
    };
    let target = Segment {
        disk_id: Some(DiskId { high: 7, low: 8 }),
        zone_index: 9,
        unit_offset: 10,
        unit_count: 5,
        owner_chunk: Some(owner),
        allocation_ts: 11,
    };
    let key = RelocationJournalKey {
        disk_id: source.disk_id.unwrap(),
        zone_index: source.zone_index,
        unit_offset: source.unit_offset,
        allocation_ts: source.allocation_ts,
    };
    let value = RelocationJournalValue {
        schema_version: RELOCATION_JOURNAL_SCHEMA_VERSION,
        operation_id: Some(ChunkId { high: 12, low: 13 }),
        owner_chunk: Some(owner),
        source: Some(source),
        target: Some(target),
        target_disk_group_id: 100,
        unit_size: 1024 * 1024,
        phase: RelocationJournalPhase::Copied.into(),
        created_at_ms: 14,
        updated_at_ms: 15,
        last_error: String::new(),
    };

    kv.put_relocation_journal((0, 1), &key, &value).await.unwrap();
    assert_eq!(
        kv.get_relocation_journal((0, 1), &key).await.unwrap(),
        Some(value.clone())
    );
    assert_eq!(
        kv.list_relocation_journals((0, 1)).await.unwrap(),
        vec![(key, value)]
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn relocation_worker_resumes_after_accept_and_frees_source_only_after_publish() {
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let keepalive = KeepAlive::new(
        cluster.make_hardware_client(),
        cluster.make_service_registry_client(),
        Arc::clone(&container),
        KeepAliveConfig {
            interval: std::time::Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: ZONE_COUNT,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ddb_kv_client(cluster.make_ddb_kv_client());
    assert_eq!(keepalive.tick().await.groups_added, 1);
    common::cluster::wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;
    container.set_lifecycle_phase(StartupPhase::Up);
    let dg = container.get_disk_group(DG_ID).unwrap();
    let kv = Arc::new(cluster.make_ddb_kv_client());
    let metrics = crowdb_diskdb::metrics::DiskdbMetrics::disabled();
    let config = Arc::new(ArcSwap::from_pointee(DdbConfig::default()));
    let ctx = BgCtx {
        container,
        kv: Arc::clone(&kv),
        metrics: metrics.clone(),
        config,
    };
    let owner_chunk = ChunkId { high: 80, low: 1 };
    let source = alloc::allocate_blocks(
        &dg,
        128,
        1,
        &[],
        false,
        &owner_chunk,
        UNIT_SIZE,
        &kv,
        100,
        ZONE_COUNT,
        &metrics,
    )
    .await
    .unwrap()
    .remove(0);
    alloc::commit_blocks(&dg, &[source], &kv, &metrics).await.unwrap();
    let io = Arc::new(TestIo {
        copies: AtomicUsize::new(0),
    });
    let owner = Arc::new(TestOwner {
        outcomes: RwLock::new(vec![Ok(RelocationHandoffDisposition::Accepted)]),
    });
    let worker = Arc::new(RelocationWorker::new(owner, io.clone()));
    let pacer = Arc::new(TestPacer::default());
    let planner = RebalancePlannerTask::new(Arc::clone(&worker), Arc::clone(&ctx.config))
        .with_pacer_for_tests(pacer.clone());
    planner.run_once_for_tests(&ctx).await;
    let mut journals = kv.list_relocation_journals((0, 1)).await.unwrap();
    assert_eq!(
        journals.len(),
        1,
        "usage={:?}, delays={:?}",
        dg.aggregate_usage().disks,
        pacer.delays.read().unwrap()
    );
    let (key, mut journal) = journals.remove(0);
    let target = journal.target.unwrap();
    assert_eq!(
        RelocationJournalPhase::try_from(journal.phase),
        Ok(RelocationJournalPhase::Accepted)
    );
    assert_eq!(io.copies.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.rebalance_plan_count.snapshot(), 1);
    assert_eq!(metrics.rebalance_planned_blocks.snapshot(), 128);
    assert_eq!(
        pacer.delays.read().unwrap().as_slice(),
        &[std::time::Duration::from_secs(3)]
    );
    assert!(kv
        .read_zone_records((0, 1), &source.disk_id.unwrap(), source.zone_index)
        .await
        .unwrap()
        .free
        .iter()
        .all(|record| record.key.allocation_ts != source.allocation_ts));
    let usage_before_duplicate = dg.aggregate_usage();
    let (duplicate_key, duplicate) = worker
        .reserve(&ctx, &dg, source, target.disk_id.unwrap())
        .await
        .unwrap();
    assert_eq!(duplicate_key, key);
    assert_eq!(duplicate.target, Some(target));
    assert_eq!(dg.aggregate_usage(), usage_before_duplicate);

    let transient = RelocationWorker::new(
        Arc::new(TestOwner {
            outcomes: RwLock::new(vec![Err("owner route unavailable".into())]),
        }),
        io.clone(),
    );
    assert!(transient.resume(&ctx, &dg, &key, &mut journal).await.is_err());
    assert_eq!(
        RelocationJournalPhase::try_from(journal.phase),
        Ok(RelocationJournalPhase::Accepted)
    );
    assert_eq!(io.copies.load(Ordering::Relaxed), 1);

    let restarted = RelocationWorker::new(
        Arc::new(TestOwner {
            outcomes: RwLock::new(vec![Ok(RelocationHandoffDisposition::Published)]),
        }),
        io.clone(),
    );
    restarted.resume(&ctx, &dg, &key, &mut journal).await.unwrap();
    assert_eq!(
        RelocationJournalPhase::try_from(journal.phase),
        Ok(RelocationJournalPhase::SourceFreed)
    );
    assert_eq!(io.copies.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.rebalance_moves_total.snapshot().count, 1);
    let target_busy = kv
        .get_busy(
            (0, 1),
            &target.disk_id.unwrap(),
            target.zone_index,
            target.unit_offset,
        )
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(target_busy.commit_state, CommitState::Committed as i32);
    assert!(kv
        .read_zone_records((0, 1), &source.disk_id.unwrap(), source.zone_index)
        .await
        .unwrap()
        .free
        .iter()
        .any(|record| record.key.allocation_ts == source.allocation_ts));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn relocation_restart_matrix_resumes_every_durable_phase() {
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let keepalive = KeepAlive::new(
        cluster.make_hardware_client(),
        cluster.make_service_registry_client(),
        Arc::clone(&container),
        KeepAliveConfig {
            interval: std::time::Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: ZONE_COUNT,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ddb_kv_client(cluster.make_ddb_kv_client());
    assert_eq!(keepalive.tick().await.groups_added, 1);
    common::cluster::wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;
    container.set_lifecycle_phase(StartupPhase::Up);
    let dg = container.get_disk_group(DG_ID).unwrap();
    let kv = Arc::new(cluster.make_ddb_kv_client());
    let metrics = crowdb_diskdb::metrics::DiskdbMetrics::disabled();
    let ctx = BgCtx {
        container,
        kv: Arc::clone(&kv),
        metrics: metrics.clone(),
        config: Arc::new(ArcSwap::from_pointee(DdbConfig::default())),
    };

    for (case, initial_phase) in [
        RelocationJournalPhase::Reserved,
        RelocationJournalPhase::Copied,
        RelocationJournalPhase::Accepted,
        RelocationJournalPhase::Published,
        RelocationJournalPhase::TargetConfirmed,
        RelocationJournalPhase::SourceFreed,
    ]
    .into_iter()
    .enumerate()
    {
        let owner_chunk = ChunkId {
            high: 80,
            low: 100 + u64::try_from(case).unwrap(),
        };
        let source = alloc::allocate_blocks(
            &dg,
            1,
            1,
            &[],
            false,
            &owner_chunk,
            UNIT_SIZE,
            &kv,
            100,
            ZONE_COUNT,
            &metrics,
        )
        .await
        .unwrap()
        .remove(0);
        alloc::commit_blocks(&dg, &[source], &kv, &metrics).await.unwrap();
        let target_disk = dg
            .disks
            .read()
            .unwrap()
            .iter()
            .find(|disk| Some(disk.disk_id) != source.disk_id)
            .unwrap()
            .disk_id;
        let setup_worker = RelocationWorker::new(
            Arc::new(TestOwner {
                outcomes: RwLock::new(Vec::new()),
            }),
            Arc::new(TestIo {
                copies: AtomicUsize::new(0),
            }),
        );
        let (key, mut journal) = setup_worker
            .reserve(&ctx, &dg, source, target_disk)
            .await
            .unwrap();
        let target = journal.target.unwrap();
        if matches!(
            initial_phase,
            RelocationJournalPhase::TargetConfirmed | RelocationJournalPhase::SourceFreed
        ) {
            alloc::commit_blocks(&dg, &[target], &kv, &metrics).await.unwrap();
        }
        if initial_phase == RelocationJournalPhase::SourceFreed {
            alloc::free_blocks(&dg, &[source], &kv).await.unwrap();
        }
        journal.phase = initial_phase.into();
        kv.put_relocation_journal(dg.bind(), &key, &journal)
            .await
            .unwrap();

        let io = Arc::new(TestIo {
            copies: AtomicUsize::new(0),
        });
        let owner_outcomes = match initial_phase {
            RelocationJournalPhase::Reserved | RelocationJournalPhase::Copied => {
                vec![Ok(RelocationHandoffDisposition::Accepted)]
            }
            RelocationJournalPhase::Accepted => vec![Ok(RelocationHandoffDisposition::Published)],
            _ => Vec::new(),
        };
        let restarted = RelocationWorker::new(
            Arc::new(TestOwner {
                outcomes: RwLock::new(owner_outcomes),
            }),
            io.clone(),
        );
        let mut recovered = kv.get_relocation_journal(dg.bind(), &key).await.unwrap().unwrap();
        restarted.resume(&ctx, &dg, &key, &mut recovered).await.unwrap();

        let expected_phase = match initial_phase {
            RelocationJournalPhase::Reserved | RelocationJournalPhase::Copied => {
                RelocationJournalPhase::Accepted
            }
            _ => RelocationJournalPhase::SourceFreed,
        };
        assert_eq!(
            RelocationJournalPhase::try_from(recovered.phase),
            Ok(expected_phase)
        );
        assert_eq!(
            io.copies.load(Ordering::Relaxed),
            usize::from(initial_phase == RelocationJournalPhase::Reserved)
        );
        let source_freed = kv
            .read_zone_records(dg.bind(), &source.disk_id.unwrap(), source.zone_index)
            .await
            .unwrap()
            .free
            .iter()
            .any(|record| record.key.allocation_ts == source.allocation_ts);
        assert_eq!(
            source_freed,
            !matches!(
                initial_phase,
                RelocationJournalPhase::Reserved | RelocationJournalPhase::Copied
            )
        );
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn relocation_stale_discards_target_while_rejected_quarantines_it() {
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let keepalive = KeepAlive::new(
        cluster.make_hardware_client(),
        cluster.make_service_registry_client(),
        Arc::clone(&container),
        KeepAliveConfig {
            interval: std::time::Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: ZONE_COUNT,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ddb_kv_client(cluster.make_ddb_kv_client());
    assert_eq!(keepalive.tick().await.groups_added, 1);
    common::cluster::wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;
    container.set_lifecycle_phase(StartupPhase::Up);
    let dg = container.get_disk_group(DG_ID).unwrap();
    let kv = Arc::new(cluster.make_ddb_kv_client());
    let metrics = crowdb_diskdb::metrics::DiskdbMetrics::disabled();
    let ctx = BgCtx {
        container,
        kv: Arc::clone(&kv),
        metrics: metrics.clone(),
        config: Arc::new(ArcSwap::from_pointee(DdbConfig::default())),
    };
    let owner_chunk = ChunkId { high: 80, low: 2 };
    let source = alloc::allocate_blocks(
        &dg,
        8,
        1,
        &[],
        false,
        &owner_chunk,
        UNIT_SIZE,
        &kv,
        100,
        ZONE_COUNT,
        &metrics,
    )
    .await
    .unwrap()
    .remove(0);
    alloc::commit_blocks(&dg, &[source], &kv, &metrics).await.unwrap();
    let target_disk = dg
        .disks
        .read()
        .unwrap()
        .iter()
        .find(|disk| Some(disk.disk_id) != source.disk_id)
        .unwrap()
        .disk_id;
    let io = Arc::new(TestIo {
        copies: AtomicUsize::new(0),
    });
    let stale_worker = RelocationWorker::new(
        Arc::new(TestOwner {
            outcomes: RwLock::new(vec![Ok(RelocationHandoffDisposition::Stale)]),
        }),
        io.clone(),
    );
    let (stale_key, mut stale_journal) = stale_worker
        .reserve(&ctx, &dg, source, target_disk)
        .await
        .unwrap();
    let stale_target = stale_journal.target.unwrap();
    stale_worker
        .resume(&ctx, &dg, &stale_key, &mut stale_journal)
        .await
        .unwrap();
    assert_eq!(
        RelocationJournalPhase::try_from(stale_journal.phase),
        Ok(RelocationJournalPhase::Discarded)
    );
    assert!(kv
        .read_zone_records(dg.bind(), &stale_target.disk_id.unwrap(), stale_target.zone_index,)
        .await
        .unwrap()
        .free
        .iter()
        .any(|record| record.key.allocation_ts == stale_target.allocation_ts));
    assert!(kv
        .read_zone_records(dg.bind(), &source.disk_id.unwrap(), source.zone_index)
        .await
        .unwrap()
        .free
        .iter()
        .all(|record| record.key.allocation_ts != source.allocation_ts));

    let rejected_source = alloc::allocate_blocks(
        &dg,
        8,
        1,
        &[],
        false,
        &owner_chunk,
        UNIT_SIZE,
        &kv,
        100,
        ZONE_COUNT,
        &metrics,
    )
    .await
    .unwrap()
    .remove(0);
    alloc::commit_blocks(&dg, &[rejected_source], &kv, &metrics)
        .await
        .unwrap();
    let rejected_target_disk = dg
        .disks
        .read()
        .unwrap()
        .iter()
        .find(|disk| Some(disk.disk_id) != rejected_source.disk_id)
        .unwrap()
        .disk_id;
    let rejected_worker = RelocationWorker::new(
        Arc::new(TestOwner {
            outcomes: RwLock::new(vec![Ok(RelocationHandoffDisposition::Rejected)]),
        }),
        io,
    );
    let (rejected_key, mut rejected_journal) = rejected_worker
        .reserve(&ctx, &dg, rejected_source, rejected_target_disk)
        .await
        .unwrap();
    let rejected_target = rejected_journal.target.unwrap();
    assert!(rejected_worker
        .resume(&ctx, &dg, &rejected_key, &mut rejected_journal)
        .await
        .is_err());
    assert_eq!(
        RelocationJournalPhase::try_from(rejected_journal.phase),
        Ok(RelocationJournalPhase::Copied)
    );
    assert!(!rejected_journal.last_error.is_empty());
    let rejected_busy = kv
        .get_busy(
            dg.bind(),
            &rejected_target.disk_id.unwrap(),
            rejected_target.zone_index,
            rejected_target.unit_offset,
        )
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(rejected_busy.commit_state, CommitState::Tentative as i32);
    assert!(kv
        .read_zone_records(
            dg.bind(),
            &rejected_source.disk_id.unwrap(),
            rejected_source.zone_index,
        )
        .await
        .unwrap()
        .free
        .iter()
        .all(|record| record.key.allocation_ts != rejected_source.allocation_ts));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn relocation_adopts_local_target_and_finalizes_remote_source() {
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let keepalive = KeepAlive::new(
        cluster.make_hardware_client(),
        cluster.make_service_registry_client(),
        Arc::clone(&container),
        KeepAliveConfig::default(),
    )
    .with_ddb_kv_client(cluster.make_ddb_kv_client());
    assert_eq!(keepalive.tick().await.groups_added, 1);
    common::cluster::wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;
    container.set_lifecycle_phase(StartupPhase::Up);
    let dg = container.get_disk_group(DG_ID).unwrap();
    let kv = Arc::new(cluster.make_ddb_kv_client());
    let metrics = crowdb_diskdb::metrics::DiskdbMetrics::disabled();
    let ctx = BgCtx {
        container,
        kv: Arc::clone(&kv),
        metrics: metrics.clone(),
        config: Arc::new(ArcSwap::from_pointee(DdbConfig::default())),
    };
    let owner_chunk = ChunkId { high: 80, low: 3 };
    let target = alloc::allocate_blocks(
        &dg,
        2,
        1,
        &[],
        false,
        &owner_chunk,
        UNIT_SIZE,
        &kv,
        100,
        ZONE_COUNT,
        &metrics,
    )
    .await
    .unwrap()
    .remove(0);
    let source = Segment {
        disk_id: Some(DiskId { high: 99, low: 100 }),
        zone_index: 7,
        unit_offset: 8,
        unit_count: 2,
        owner_chunk: Some(owner_chunk),
        allocation_ts: 9,
    };
    let source_free = Arc::new(TestSourceFree::default());
    let worker = Arc::new(
        RelocationWorker::new(
            Arc::new(TestOwner {
                outcomes: RwLock::new(vec![Ok(RelocationHandoffDisposition::Published)]),
            }),
            Arc::new(TestIo {
                copies: AtomicUsize::new(0),
            }),
        )
        .with_source_free(source_free.clone()),
    );
    let service = Arc::new(
        DiskdbRpcService::new(
            Arc::clone(&ctx.container),
            Arc::clone(&kv),
            StorageDefaults::default(),
            Arc::new(ZoneLoader::new(Arc::clone(&kv), 4)),
            Arc::new(RecalcEngine::new(Arc::clone(&kv), Arc::clone(&ctx.container))),
            ScanState::new(),
            Arc::new(metrics),
            Arc::clone(&ctx.config),
            tokio::runtime::Handle::current(),
        )
        .with_relocation_worker(worker),
    );
    let port = crowdb_protocol::port::alloc::alloc_test_port(crowdb_protocol::ServicePort::DiskdbRpc);
    let server = Arc::new(crowdb_rpc_ffi::RpcServer::new(None));
    server.listen("127.0.0.1", i32::from(port)).unwrap();
    service.register_handlers(&server);
    server.start();
    let response = DiskdbRpcTransport::new()
        .execute_relocation(
            &format!("http://127.0.0.1:{port}"),
            &crowdb_protocol::diskdb::rpc::ExecuteRelocationRequest {
                target_disk_group_id: DG_ID,
                source: Some(source),
                target: Some(target),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        response.operation_id,
        crowdb_protocol::chunk_task::relocation_operation_id(&source)
    );
    let key = RelocationJournalKey {
        disk_id: source.disk_id.unwrap(),
        zone_index: source.zone_index,
        unit_offset: source.unit_offset,
        allocation_ts: source.allocation_ts,
    };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let journal = loop {
        let journal = kv.get_relocation_journal(dg.bind(), &key).await.unwrap().unwrap();
        if RelocationJournalPhase::try_from(journal.phase) == Ok(RelocationJournalPhase::SourceFreed) {
            break journal;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    assert_eq!(
        RelocationJournalPhase::try_from(journal.phase),
        Ok(RelocationJournalPhase::SourceFreed)
    );
    assert_eq!(source_free.calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        kv.get_busy(
            dg.bind(),
            &target.disk_id.unwrap(),
            target.zone_index,
            target.unit_offset,
        )
        .await
        .unwrap()
        .unwrap()
        .0
        .commit_state,
        CommitState::Committed as i32
    );
}
