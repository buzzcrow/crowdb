// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Full-stack E2E test: real KV cluster + diskdb + chunkdb in-process.
//!
//! Verifies the allocate → append → seal → query → delete lifecycle
//! against a real 3-node crowdb-kv-server cluster with diskdb running
//! in-process as a crowdb-rpc server.

mod common;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::cluster::{
    seed_hardware, seed_hardware_layout_from_disk_group, seed_hardware_layout_with_zones, ChunkdbHarness,
    DiskdbServer, KvCluster, DATA_GROUP_ID, STORE_ID,
};
use crowdb_chunkdb::allocator::StripAllocType;
use crowdb_chunkdb::chunkdb_config::PlacementRebalanceConfig;
use crowdb_chunkdb::conversion::io::ConversionDiskIo;
use crowdb_chunkdb::conversion::{decode_payload, ConversionCoordinator, MirrorToEcTaskHandler};
use crowdb_chunkdb::finalize::FinalizeChunkTaskHandler;
use crowdb_chunkdb::lifecycle::{
    ChunkLockMap, LifecycleError, LifecycleHandler, ReservationFence, ReservationRecovery, ReservationUpdate,
    ReserveGroupSpec,
};
use crowdb_chunkdb::metrics::{ChunkdbMetrics, LifecycleMetrics};
use crowdb_chunkdb::placement_rebalance::PlacementRebalancePlanner;
use crowdb_chunkdb::placement_repair::{PlacementRepairCoordinator, PlacementRepairTaskHandler};
use crowdb_chunkdb::range_guard::{OwnedRange, RangeGuard};
use crowdb_chunkdb::relocation::{RelocationAdmissionError, RelocationCoordinator};
use crowdb_chunkdb::repair::{decode_payload as decode_repair_payload, RepairCoordinator};
use crowdb_chunkdb::routing::{default_binding_table, hash_to_bucket, BindingCache};
use crowdb_chunkdb::selector::{FailureDomainPriority, PlacementConstraints};
use crowdb_chunkdb::service::ChunkdbRpcService;
use crowdb_chunkdb::task::{
    RelocateSegmentTaskHandler, SegmentOwnerResolver, TaskAdmission, TaskClaim, TaskExecutor, TaskHandler,
    TaskManager, TaskOutcome, TaskScanner, TaskStore,
};
use crowdb_chunkdb_client::ChunkdbRpcTransport;
use crowdb_common::metrics::MetricsRegistry;
use crowdb_protocol::chunk_task::{
    ChunkTaskState, ChunkTaskValue, RelocateSegmentTaskDisposition, RelocateSegmentTaskPayload,
    CHUNK_TASK_SCHEMA_VERSION, TASK_KIND_FINALIZE_CHUNK, TASK_KIND_MIRROR_TO_EC, TASK_KIND_RELOCATE_SEGMENT,
    TASK_KIND_REPAIR_PLACEMENT, TASK_KIND_REPAIR_STRIP,
};
use crowdb_protocol::chunkdb::rpc::{
    Chunk, ChunkState, ChunkStrip, ChunkType, EcState, EcStrip, PlacementAssessment,
    QuerySegmentOwnerRequest, RelocateSegmentHandoffRequest, RelocationHandoffDisposition,
    SegmentOwnerDisposition, Strip, StripReservationAction, StripReservationState, StripType,
};
use crowdb_protocol::common::{ChunkId, DiskGroupUsageSummary};
use crowdb_protocol::diskdb::rpc::RelocationJournalPhase;
use crowdb_protocol::{port::alloc as port_alloc, ServicePort};
use crowdb_test_harness::diskio::{DiskioGroup0Identity, DiskioProcess, DiskioStartOpts};

fn max_fragment_count<K: Eq + Hash>(counts: &HashMap<K, u32>) -> u32 {
    *counts.values().max().expect("segment count")
}

fn test_physical_assessment(
    snapshot: &crowdb_chunkdb::topology::TopologySnapshot,
    segments: &[crowdb_protocol::diskdb::rpc::Segment],
    loss_budget: u32,
) -> PlacementAssessment {
    let mut rack_counts = HashMap::new();
    let mut node_counts = HashMap::new();
    let mut disk_counts = HashMap::new();
    for segment in segments {
        let disk_id = segment.disk_id.expect("physical disk");
        let location = snapshot.disk_location(disk_id).expect("disk location");
        *rack_counts.entry(location.rack_id).or_insert(0) += 1;
        *node_counts.entry(location.node_id).or_insert(0) += 1;
        *disk_counts.entry(disk_id).or_insert(0) += 1;
    }
    let max_fragments_per_rack = max_fragment_count(&rack_counts);
    let max_fragments_per_node = max_fragment_count(&node_counts);
    let max_fragments_per_disk = max_fragment_count(&disk_counts);
    PlacementAssessment {
        loss_budget,
        max_fragments_per_rack,
        max_fragments_per_node,
        max_fragments_per_disk,
        rack_protected: max_fragments_per_rack <= loss_budget,
        node_protected: max_fragments_per_node <= loss_budget,
        disk_protected: max_fragments_per_disk <= loss_budget,
        topology_generation: snapshot.generation(),
        usage_fresh: false,
    }
}

async fn assert_conversion_task_pending(
    handler: &Arc<LifecycleHandler>,
    task_store: &Arc<TaskStore>,
    chunk_id: ChunkId,
    task: &ChunkTaskValue,
    replacement: &ChunkStrip,
) {
    assert_eq!(
        task_store
            .list_partition(&chunk_id)
            .await
            .unwrap()
            .into_iter()
            .filter(|t| t.kind != TASK_KIND_FINALIZE_CHUNK)
            .collect::<Vec<_>>(),
        vec![task.clone()]
    );
    assert!(decode_payload(&task.payload).unwrap().replacement_strip.is_some());
    let target = match replacement.strip.as_ref().expect("replacement body") {
        Strip::MirrorStrip(mirror) => mirror.segments[0],
        Strip::EcStrip(ec) => ec.segments[0],
    };
    let owner = SegmentOwnerResolver::new(Arc::clone(handler), Arc::clone(task_store));
    assert_eq!(
        owner.resolve(&chunk_id, &target).await.unwrap(),
        SegmentOwnerDisposition::TaskPending
    );

    let port = port_alloc::alloc_test_port(ServicePort::ChunkdbRpc);
    let endpoint = format!("http://127.0.0.1:{port}");
    let server = Arc::new(crowdb_rpc_ffi::RpcServer::new(None));
    server.listen("127.0.0.1", i32::from(port)).unwrap();
    let mut registry = MetricsRegistry::new();
    let service = Arc::new(
        ChunkdbRpcService::new(
            Arc::clone(handler),
            Arc::new(ChunkdbMetrics::register(&mut registry)),
            tokio::runtime::Handle::current(),
        )
        .with_task_store(Arc::clone(task_store)),
    );
    service.register_handlers(&server);
    server.start();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let transport = ChunkdbRpcTransport::new();
        match transport
            .send_query_segment_owner(
                &endpoint,
                &QuerySegmentOwnerRequest {
                    chunk_id: Some(chunk_id),
                    segment: Some(target),
                },
            )
            .await
        {
            Ok(response) => {
                assert_eq!(
                    SegmentOwnerDisposition::try_from(response.disposition).unwrap(),
                    SegmentOwnerDisposition::TaskPending
                );
                break;
            }
            Err(error) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "owner RPC did not become ready: {error}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

fn start_diskio_groups(
    cluster: &KvCluster,
    groups: &[(u64, u64, u64)],
    instance_base: u64,
) -> Vec<DiskioProcess> {
    groups
        .iter()
        .enumerate()
        .map(|(index, &(disk_group_id, rack_id, node_id))| {
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
                    instance_id: instance_base.saturating_add(u64::try_from(index).unwrap()),
                    rack_id,
                    node_id,
                    disk_group_id,
                },
            )
        })
        .collect()
}

#[tokio::test]
async fn physical_ec_reports_the_two_rack_layout_truthfully() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary not found");
        return;
    }

    for (data_num, code_num) in [(10, 2), (20, 2), (40, 4)] {
        for priority in [FailureDomainPriority::RackFirst, FailureDomainPriority::NodeFirst] {
            let cluster = KvCluster::start().await;
            let disk_groups = seed_hardware_layout_with_zones(
                &cluster.make_hardware_client(),
                &[(100, vec![10, 11, 12, 13]), (101, vec![20, 21])],
                32,
            )
            .await;
            let _diskdb = DiskdbServer::start_with_disk_groups_and_zones(&cluster, &disk_groups, 32).await;
            let harness = ChunkdbHarness::start(&cluster).await;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while harness.topology.snapshot().healthy_disk_groups().len() < disk_groups.len() {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "two-rack topology was not published"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let strip = harness
                .allocator
                .allocate_strip(
                    &harness.topology.snapshot(),
                    &ChunkId {
                        high: u64::try_from(data_num).unwrap(),
                        low: u64::try_from(code_num).unwrap(),
                    },
                    StripAllocType::Ec { data_num, code_num },
                    1,
                    0,
                    &PlacementConstraints::new()
                        .allow_unsafe_ec()
                        .allow_degraded_failure_domains()
                        .with_failure_domain_priority(priority),
                )
                .await
                .expect("degraded EC allocation");
            let assessment = strip.placement_assessment.as_ref().expect("physical assessment");
            assert_eq!(assessment.loss_budget, u32::try_from(code_num).unwrap());

            let segments = match strip.strip.as_ref().expect("EC strip") {
                Strip::EcStrip(ec) => &ec.segments,
                Strip::MirrorStrip(_) => panic!("expected EC strip"),
            };
            let mut racks = HashMap::new();
            let mut nodes = HashMap::new();
            let mut disks = HashMap::new();
            for segment in segments {
                let disk_id = segment.disk_id.expect("physical disk ID");
                let location = harness
                    .topology
                    .snapshot()
                    .disk_location(disk_id)
                    .expect("disk location");
                *racks.entry(location.rack_id).or_insert(0_u32) += 1;
                *nodes.entry(location.node_id).or_insert(0_u32) += 1;
                *disks.entry(disk_id).or_insert(0_u32) += 1;
            }
            assert_eq!(assessment.max_fragments_per_rack, max_fragment_count(&racks));
            assert_eq!(assessment.max_fragments_per_node, max_fragment_count(&nodes));
            assert_eq!(assessment.max_fragments_per_disk, max_fragment_count(&disks));
            assert_eq!(
                assessment.rack_protected,
                max_fragment_count(&racks) <= assessment.loss_budget
            );
            assert_eq!(
                assessment.node_protected,
                max_fragment_count(&nodes) <= assessment.loss_budget
            );
            assert_eq!(
                assessment.disk_protected,
                max_fragment_count(&disks) <= assessment.loss_budget
            );
            assert!(
                !assessment.rack_protected,
                "two racks cannot protect {data_num}+{code_num}"
            );
            assert!(assessment.max_fragments_per_rack > assessment.loss_budget);
        }
    }
}

#[tokio::test]
async fn diskdb_refreshes_new_failure_domains_without_restart() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let mut disk_groups = seed_hardware_layout_with_zones(
        &cluster.make_hardware_client(),
        &[(100, vec![10, 11, 12, 13]), (101, vec![20, 21])],
        32,
    )
    .await;
    let diskdb = DiskdbServer::start_with_disk_groups_and_zones(&cluster, &disk_groups, 32).await;
    let new_disk_groups =
        seed_hardware_layout_from_disk_group(&cluster.make_hardware_client(), &[(102, vec![30])], 32, 2000)
            .await;
    disk_groups.extend_from_slice(&new_disk_groups);
    diskdb
        .refresh_disk_groups(&cluster, &new_disk_groups, &disk_groups, 32)
        .await;
    let harness = ChunkdbHarness::start_with_layout_validity(&cluster, Duration::from_millis(1)).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while harness.topology.snapshot().healthy_disk_groups().len() < disk_groups.len() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "new topology was not published"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn diskio_routes_cover_every_group_in_the_two_rack_fixture() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let disk_groups = seed_hardware_layout_with_zones(
        &cluster.make_hardware_client(),
        &[(100, vec![10, 11, 12, 13]), (101, vec![20, 21])],
        32,
    )
    .await;
    let _diskdb = DiskdbServer::start_with_disk_groups_and_zones(&cluster, &disk_groups, 32).await;
    let diskio = start_diskio_groups(
        &cluster,
        &[
            (1000, 100, 10),
            (1001, 100, 11),
            (1002, 100, 12),
            (1003, 100, 13),
            (1004, 101, 20),
            (1005, 101, 21),
        ],
        2_000,
    );
    let service = cluster.make_service_registry_client();
    let hardware = cluster.make_hardware_client();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if ConversionDiskIo::connect(&service, &hardware).await.is_ok() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "DiskIO routes were not published"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(diskio.len(), disk_groups.len());
}

#[allow(clippy::too_many_lines)]
async fn assert_expanded_topology_converges_ec(data_num: u32, code_num: u32, required_racks: u64) {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let mut disk_groups = seed_hardware_layout_with_zones(
        &cluster.make_hardware_client(),
        &[(100, vec![10, 11, 12, 13]), (101, vec![20, 21])],
        32,
    )
    .await;
    let diskdb = DiskdbServer::start_with_disk_groups_and_zones(&cluster, &disk_groups, 32).await;
    let mut diskio = start_diskio_groups(
        &cluster,
        &[
            (1000, 100, 10),
            (1001, 100, 11),
            (1002, 100, 12),
            (1003, 100, 13),
            (1004, 101, 20),
            (1005, 101, 21),
        ],
        2_000,
    );
    let service = cluster.make_service_registry_client();
    let hardware = cluster.make_hardware_client();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let io = loop {
        if let Ok(io) = ConversionDiskIo::connect(&service, &hardware).await {
            break Arc::new(io);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "DiskIO routes were not published"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let harness = ChunkdbHarness::start(&cluster).await;
    let handler = Arc::new(
        LifecycleHandler::new(
            Arc::clone(&harness.store),
            Arc::clone(&harness.allocator),
            harness.topology.clone(),
        )
        .with_layout_validity(Duration::from_millis(1))
        .with_allow_unsafe_ec(true)
        .with_placement_policy(FailureDomainPriority::RackFirst, true),
    );
    let chunk_id = ChunkId {
        high: 97,
        low: u64::from(data_num),
    };
    let chunk = handler
        .allocate_chunk(
            Some(chunk_id),
            1,
            1,
            StripType::Ec,
            data_num,
            code_num,
            0,
            ChunkType::Repo,
            0,
            0,
        )
        .await
        .unwrap();
    let Some(Strip::EcStrip(ec)) = chunk.strips[0].strip.as_ref() else {
        panic!("expected EC strip");
    };
    for segment in &ec.segments {
        io.write_segment(segment, 1024 * 1024, Bytes::from(vec![0x97; 1024 * 1024]))
            .await
            .expect("seed source data");
    }
    let expansion_layout: Vec<_> = (0..required_racks.saturating_sub(2))
        .map(|offset| (102 + offset, vec![30 + offset]))
        .collect();
    let new_disk_groups =
        seed_hardware_layout_from_disk_group(&cluster.make_hardware_client(), &expansion_layout, 32, 2000)
            .await;
    disk_groups.extend_from_slice(&new_disk_groups);
    diskdb
        .refresh_disk_groups(&cluster, &new_disk_groups, &disk_groups, 32)
        .await;
    let expansion_groups: Vec<_> = (0..required_racks.saturating_sub(2))
        .map(|offset| (2000 + offset, 102 + offset, 30 + offset))
        .collect();
    diskio.extend(start_diskio_groups(&cluster, &expansion_groups, 3_000));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if io.refresh(&service, &hardware).await.is_ok()
            && harness.topology.snapshot().healthy_disk_groups().len() == disk_groups.len()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expanded routes were not published"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    PlacementRepairCoordinator::new(Arc::clone(&handler), Arc::clone(&tasks))
        .admit_chunk(&chunk, 100)
        .await
        .unwrap();
    let mut registry = MetricsRegistry::new();
    let metrics = ChunkdbMetrics::register(&mut registry).placement;
    let manager = Arc::new(TaskManager::new(Arc::clone(&tasks), 97, 30_000));
    let executor = TaskExecutor::new(
        Arc::clone(&manager),
        1,
        vec![Arc::new(PlacementRepairTaskHandler::new(
            Arc::clone(&handler),
            Arc::clone(&manager),
            Arc::clone(&io),
            metrics,
        ))],
    )
    .unwrap();
    for _ in 0..usize::try_from(u64::from(data_num + code_num).saturating_mul(4)).unwrap() {
        let ready = tasks.scan_ready(u64::MAX, 1).await.unwrap();
        let claim = manager.claim(&ready[0], u64::MAX).await.unwrap().unwrap();
        executor.execute(claim).await.unwrap();
        if !handler.query_chunk(&chunk_id).await.unwrap().strips[0].placement_repair_required {
            break;
        }
    }
    let repaired = handler.query_chunk(&chunk_id).await.unwrap();
    let assessment = repaired.strips[0].placement_assessment.as_ref().unwrap();
    let task = tasks.scan_ready(u64::MAX, 1).await.unwrap();
    let task_detail = match task.first() {
        Some(index) => tasks
            .get(&index.partition_id, index.kind, &index.task_id)
            .await
            .unwrap(),
        None => None,
    };
    assert!(
        !repaired.strips[0].placement_repair_required,
        "assessment={assessment:?}, next_task={task:?}, task_detail={task_detail:?}"
    );
    assert!(assessment.rack_protected && assessment.node_protected && assessment.disk_protected);

    let snapshot = handler.topology_snapshot();
    let segments = match repaired.strips[0].strip.as_ref().unwrap() {
        Strip::EcStrip(ec) => &ec.segments,
        Strip::MirrorStrip(_) => unreachable!(),
    };
    let occupied_disks: HashSet<_> = segments.iter().filter_map(|segment| segment.disk_id).collect();
    let current = test_physical_assessment(&snapshot, segments, code_num);
    let mut source_groups = HashSet::new();
    let (source_dg, target_dg) = disk_groups
        .iter()
        .copied()
        .find_map(|target_dg| {
            let target_disk = snapshot
                .disk_group(target_dg)?
                .value
                .disk_ids
                .iter()
                .copied()
                .find(|disk_id| !occupied_disks.contains(disk_id))?;
            source_groups.clear();
            segments.iter().find_map(|source| {
                let source_dg = snapshot.disk_location(source.disk_id?).unwrap().disk_group_id;
                if source_dg == target_dg || !source_groups.insert(source_dg) {
                    return None;
                }
                let source = segments.iter().find(|candidate| {
                    candidate.disk_id.is_some_and(|disk_id| {
                        snapshot.disk_location(disk_id).unwrap().disk_group_id == source_dg
                    })
                })?;
                let mut replacement = segments.clone();
                let position = replacement.iter().position(|candidate| candidate == source)?;
                replacement[position].disk_id = Some(target_disk);
                let next = test_physical_assessment(&snapshot, &replacement, code_num);
                (next.rack_protected == current.rack_protected
                    && next.node_protected == current.node_protected
                    && next.disk_protected == current.disk_protected
                    && next.max_fragments_per_rack <= current.max_fragments_per_rack
                    && next.max_fragments_per_node <= current.max_fragments_per_node
                    && next.max_fragments_per_disk <= current.max_fragments_per_disk)
                    .then_some((source_dg, target_dg))
            })
        })
        .expect("one cross-domain move preserves every physical protection bound");
    let capacity = 1_000_000_000_000u64;
    let summaries: Vec<_> = disk_groups
        .iter()
        .copied()
        .map(|disk_group_id| {
            let used_bytes = if disk_group_id == source_dg {
                capacity * 8 / 10
            } else if disk_group_id == target_dg {
                capacity / 10
            } else {
                capacity / 2
            };
            DiskGroupUsageSummary {
                disk_group_id,
                capacity_bytes: capacity,
                used_bytes,
                free_bytes: capacity - used_bytes,
                disk_count: 3,
                allocatable_disk_count: 3,
                allocatable_capacity_bytes: capacity,
                allocatable_used_bytes: used_bytes,
                allocatable_free_bytes: capacity - used_bytes,
                sampled_at_ms: 1,
            }
        })
        .collect();
    service
        .register_diskdb(
            common::cluster::INSTANCE_ID,
            &diskdb.rpc_endpoint,
            &disk_groups,
            &summaries,
        )
        .await
        .unwrap();
    let refreshed = crowdb_chunkdb::topology::build_snapshot(&hardware).await.unwrap();
    harness.topology.replace(refreshed);
    harness
        .pool
        .update_disk_id_lookup(&harness.topology.snapshot().disk_groups());

    let coordinator = Arc::new(RelocationCoordinator::new(Arc::clone(&manager)));
    diskdb.set_relocation_owner(coordinator);
    let planner = PlacementRebalancePlanner::new(
        Arc::clone(&handler),
        Arc::clone(&harness.pool),
        PlacementRebalanceConfig {
            enabled: true,
            scan_interval_secs: 1,
            imbalance_threshold_pct: 20,
            hysteresis_secs: 0,
            min_target_free_bytes: 0,
            max_moves_per_cycle: 1,
        },
    );
    assert_eq!(planner.run_once(1_000).await.unwrap(), 1);
    let kv = cluster.make_ddb_kv_client();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let accepted = loop {
        let journals = kv
            .list_relocation_journals((STORE_ID, DATA_GROUP_ID))
            .await
            .unwrap();
        if let Some((_, journal)) = journals.iter().find(|(_, journal)| {
            journal.source.is_some_and(|source| {
                snapshot
                    .disk_location(source.disk_id.unwrap())
                    .is_some_and(|location| location.disk_group_id == source_dg)
            })
        }) {
            if RelocationJournalPhase::try_from(journal.phase) == Ok(RelocationJournalPhase::Accepted) {
                break journal.clone();
            }
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let relocation_executor = TaskExecutor::new(
        Arc::clone(&manager),
        1,
        vec![Arc::new(RelocateSegmentTaskHandler::new(
            Arc::clone(&handler),
            Arc::clone(&manager),
        ))],
    )
    .unwrap();
    for _ in 0..4 {
        let ready = tasks.scan_ready(u64::MAX, 1).await.unwrap();
        if ready.is_empty() {
            break;
        }
        let claim = manager.claim(&ready[0], u64::MAX).await.unwrap().unwrap();
        relocation_executor.execute(claim).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    harness
        .pool
        .execute_relocation(crowdb_protocol::diskdb::rpc::ExecuteRelocationRequest {
            target_disk_group_id: target_dg,
            source: accepted.source,
            target: accepted.target,
        })
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let journals = kv
            .list_relocation_journals((STORE_ID, DATA_GROUP_ID))
            .await
            .unwrap();
        if journals.iter().any(|(_, journal)| {
            journal.operation_id == accepted.operation_id
                && RelocationJournalPhase::try_from(journal.phase) == Ok(RelocationJournalPhase::SourceFreed)
        }) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let rebalanced = handler.query_chunk(&chunk_id).await.unwrap();
    let assessment = rebalanced.strips[0].placement_assessment.as_ref().unwrap();
    assert!(assessment.rack_protected && assessment.node_protected && assessment.disk_protected);
    assert!(!diskio.is_empty());
}

#[tokio::test]
async fn expanded_topology_converges_degraded_ec_matrix() {
    for (data_num, code_num, racks) in [(10, 2, 6), (20, 2, 11), (40, 4, 11)] {
        assert_expanded_topology_converges_ec(data_num, code_num, racks).await;
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn degraded_ec_markers_recreate_one_task_per_large_strip_after_admission_gap() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary not found");
        return;
    }

    let cluster = KvCluster::start().await;
    let disk_groups = seed_hardware_layout_with_zones(
        &cluster.make_hardware_client(),
        &[(100, vec![10, 11, 12, 13]), (101, vec![20, 21])],
        32,
    )
    .await;
    let _diskdb = DiskdbServer::start_with_disk_groups_and_zones(&cluster, &disk_groups, 32).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let handler = Arc::new(
        LifecycleHandler::new(
            Arc::clone(&harness.store),
            Arc::clone(&harness.allocator),
            harness.topology.clone(),
        )
        .with_allow_unsafe_ec(true)
        .with_placement_policy(FailureDomainPriority::RackFirst, true),
    );

    let mut chunks = Vec::new();
    for (index, (data_num, code_num)) in [(10, 2), (20, 2), (40, 4)].into_iter().enumerate() {
        let chunk = handler
            .allocate_chunk(
                Some(ChunkId {
                    high: 970,
                    low: u64::try_from(index).unwrap(),
                }),
                1,
                1,
                StripType::Ec,
                data_num,
                code_num,
                0,
                ChunkType::Repo,
                0,
                0,
            )
            .await
            .expect("degraded EC allocation persists before task admission");
        assert!(chunk.strips[0].placement_repair_required);
        chunks.push(chunk);
    }

    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let restarted = PlacementRepairCoordinator::new(Arc::clone(&handler), Arc::clone(&tasks));
    assert_eq!(restarted.scan_batch(256, 100).await.unwrap(), 3);

    let ready = tasks.scan_ready(100, 16).await.unwrap();
    assert_eq!(ready.len(), 3);
    for chunk in &chunks {
        let chunk_id = chunk.id.expect("chunk ID");
        assert_eq!(
            ready.iter().filter(|task| task.partition_id == chunk_id).count(),
            1,
            "one repaired task for the marked strip"
        );
    }

    let second_restart = PlacementRepairCoordinator::new(Arc::clone(&handler), Arc::clone(&tasks));
    assert_eq!(second_restart.scan_batch(256, 101).await.unwrap(), 0);
    assert_eq!(tasks.scan_ready(101, 16).await.unwrap().len(), 3);

    let largest_chunk_id = chunks
        .last()
        .and_then(|chunk| chunk.id)
        .expect("largest chunk ID");
    let index = ready
        .iter()
        .find(|task| task.partition_id == largest_chunk_id)
        .copied()
        .expect("40+4 placement task");
    let task = tasks
        .get(&largest_chunk_id, TASK_KIND_REPAIR_PLACEMENT, &index.task_id)
        .await
        .unwrap()
        .expect("stored placement task");
    let mut registry = MetricsRegistry::new();
    let metrics = ChunkdbMetrics::register(&mut registry).placement;
    let manager = Arc::new(TaskManager::new(Arc::clone(&tasks), 97, 30_000));
    let executor = TaskExecutor::new(
        Arc::clone(&manager),
        1,
        vec![Arc::new(PlacementRepairTaskHandler::new(
            Arc::clone(&handler),
            Arc::clone(&manager),
            Arc::new(ConversionDiskIo::empty_for_tests()),
            Arc::clone(&metrics),
        ))],
    )
    .unwrap();
    let claim = manager
        .claim(&index, 102)
        .await
        .unwrap()
        .expect("claim placement task");
    executor.execute(claim).await.unwrap();
    let waiting = tasks
        .get(&largest_chunk_id, TASK_KIND_REPAIR_PLACEMENT, &task.task_id)
        .await
        .unwrap()
        .expect("waiting placement task");
    assert_eq!(waiting.state, ChunkTaskState::RetryWait);
    assert_eq!(waiting.last_error_code, 40);
    assert_eq!(metrics.snapshot().repair_waiting, 1);
    assert_eq!(metrics.snapshot().repair_failures, 0);
}

fn task_value() -> ChunkTaskValue {
    ChunkTaskValue {
        schema_version: CHUNK_TASK_SCHEMA_VERSION,
        task_id: ChunkId { high: 3, low: 4 },
        partition_id: ChunkId { high: 1, low: 2 },
        kind: TASK_KIND_MIRROR_TO_EC,
        kind_version: 1,
        state: ChunkTaskState::Pending,
        priority: 10,
        revision: 1,
        operation_id: ChunkId { high: 5, low: 6 },
        source_revision: 7,
        created_at_ms: 100,
        updated_at_ms: 100,
        eligible_at_ms: 100,
        attempt: 0,
        max_attempts: 3,
        estimated_queue_bytes: 20 * 1024 * 1024,
        claim_owner: 0,
        claim_generation: 0,
        claim_deadline_ms: 0,
        last_error_code: 0,
        last_error: String::new(),
        payload: vec![1, 2, 3],
    }
}

struct CompleteTaskHandler;

#[tokio::test]
async fn active_chunk_creates_one_deadline_indexed_finalizer() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::Repo,
            17,
            60_000,
        )
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = TaskStore::new(cluster.make_crowdb_client(), bindings);

    let task = tasks
        .get(&chunk_id, TASK_KIND_FINALIZE_CHUNK, &chunk_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.source_revision, 17);
    assert_eq!(task.state, ChunkTaskState::Pending);
    assert_eq!(
        tasks
            .scan_finalize_due(task.eligible_at_ms, 8)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(tasks.scan_ready(task.eligible_at_ms, 8).await.unwrap().is_empty());
}

#[tokio::test]
async fn finalizer_reclaims_an_empty_active_chunk() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::Repo,
            19,
            60_000,
        )
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let mut task = tasks
        .get(&chunk_id, TASK_KIND_FINALIZE_CHUNK, &chunk_id)
        .await
        .unwrap()
        .unwrap();
    task.eligible_at_ms = 0;
    let finalizer = FinalizeChunkTaskHandler::new(
        Arc::clone(&harness.handler),
        Arc::new(ConversionDiskIo::empty_for_tests()),
    );
    assert!(matches!(finalizer.execute(&task).await, TaskOutcome::Complete));
    assert_eq!(
        harness.handler.query_chunk(&chunk_id).await.unwrap().state,
        ChunkState::Deleted as i32
    );
}

#[tokio::test]
async fn finalizer_waits_for_pre_expiry_requests_before_scanning() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::Repo,
            23,
            60_000,
        )
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = TaskStore::new(cluster.make_crowdb_client(), bindings);
    let task = tasks
        .get(&chunk_id, TASK_KIND_FINALIZE_CHUNK, &chunk_id)
        .await
        .unwrap()
        .unwrap();
    let finalizer = FinalizeChunkTaskHandler::new(
        Arc::clone(&harness.handler),
        Arc::new(ConversionDiskIo::empty_for_tests()),
    );
    assert!(matches!(
        finalizer.execute(&task).await,
        TaskOutcome::Retry { delay_ms, .. } if delay_ms > 0
    ));
    assert_eq!(
        harness.handler.query_chunk(&chunk_id).await.unwrap().state,
        ChunkState::Active as i32
    );
}

impl TaskHandler for CompleteTaskHandler {
    fn kind(&self) -> u16 {
        TASK_KIND_MIRROR_TO_EC
    }

    fn supports_version(&self, version: u16) -> bool {
        version == 1
    }

    fn execute<'a>(&'a self, _task: &'a ChunkTaskValue) -> crowdb_chunkdb::task::executor::TaskFuture<'a> {
        Box::pin(async { TaskOutcome::Complete })
    }
}

#[tokio::test]
async fn task_survives_claim_expiry_takeover_and_completion() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }

    let cluster = KvCluster::start().await;
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let store = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let first_manager = TaskManager::new(Arc::clone(&store), 41, 100);
    let task = task_value();

    assert!(matches!(
        first_manager.admit(task.clone()).await.unwrap(),
        TaskAdmission::Created(_)
    ));
    assert!(matches!(
        first_manager.admit(task.clone()).await.unwrap(),
        TaskAdmission::Existing(_)
    ));

    let ready = store.scan_ready(100, 16).await.unwrap();
    assert_eq!(ready.len(), 1);
    let first_claim = first_manager.claim(&ready[0], 100).await.unwrap().unwrap();
    assert_eq!(first_claim.task.claim_generation, 1);
    assert_eq!(first_claim.task.attempt, 1);
    assert!(store.scan_ready(100, 16).await.unwrap().is_empty());
    assert!(store.scan_expired_leases(199, 16).await.unwrap().is_empty());

    drop(first_manager);
    let restarted_manager = Arc::new(TaskManager::new(Arc::clone(&store), 42, 100));
    let executor = Arc::new(
        TaskExecutor::new(
            Arc::clone(&restarted_manager),
            2,
            vec![Arc::new(CompleteTaskHandler)],
        )
        .unwrap(),
    );
    let scanner = TaskScanner::new(
        Arc::clone(&store),
        Arc::clone(&restarted_manager),
        executor,
        16,
        Duration::from_secs(30),
    );
    let summary = scanner.run_once(200).await.unwrap();
    assert_eq!(summary.expired_claims_requeued, 1);
    assert_eq!(summary.ready_indexes_seen, 1);
    assert_eq!(summary.tasks_claimed, 1);
    assert_eq!(summary.tasks_completed_or_requeued, 1);
    assert_eq!(summary.dispatch_errors, 0);

    assert!(store.scan_ready(210, 16).await.unwrap().is_empty());
    assert!(store.scan_expired_leases(u64::MAX, 16).await.unwrap().is_empty());
    let stored = store
        .get(&task.partition_id, task.kind, &task.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, ChunkTaskState::Completed);
    assert_eq!(stored.revision, 5);
    assert_eq!(stored.claim_generation, 2);
    assert_eq!(stored.attempt, 2);

    for id in 10..13 {
        let mut queued = task_value();
        queued.task_id.low = id;
        queued.operation_id.low = id;
        restarted_manager.admit(queued).await.unwrap();
    }
    let bounded = scanner.run_once(300).await.unwrap();
    assert_eq!(bounded.ready_indexes_seen, 3);
    assert_eq!(bounded.tasks_claimed, 2);
    assert_eq!(bounded.tasks_completed_or_requeued, 2);
    assert_eq!(store.scan_ready(301, 16).await.unwrap().len(), 1);
    let drained = scanner.run_once(301).await.unwrap();
    assert_eq!(drained.tasks_claimed, 1);
    assert!(store.scan_ready(302, 16).await.unwrap().is_empty());
}

#[tokio::test]
async fn unavailable_strip_survives_crash_gap_and_is_admitted_as_repair_task() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let old = chunk.strips[0].clone();
    let Some(Strip::MirrorStrip(mirror)) = &old.strip else {
        panic!("expected mirror strip");
    };
    let failed = mirror.segments[0];
    let mut marked = old.clone();
    marked.unavailable_segments.push(failed);
    let marked_chunk = harness
        .handler
        .replace_chunk_strip_range(
            &chunk_id,
            chunk.modify_ts,
            0,
            std::slice::from_ref(&old),
            std::slice::from_ref(&marked),
            ChunkId { high: 111, low: 1 },
        )
        .await
        .unwrap();

    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let restarted = RepairCoordinator::new(Arc::clone(&harness.handler), Arc::clone(&tasks));
    assert_eq!(restarted.admit_chunk(&marked_chunk, 100).await.unwrap(), 1);
    assert_eq!(restarted.scan_batch(256, 101).await.unwrap(), 0);

    let ready = tasks.scan_ready(101, 16).await.unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].kind, TASK_KIND_REPAIR_STRIP);
    let task = tasks
        .get(&chunk_id, ready[0].kind, &ready[0].task_id)
        .await
        .unwrap()
        .unwrap();
    let payload = decode_repair_payload(&task.payload).unwrap();
    assert_eq!(payload.chunk_id, chunk_id);
    assert_eq!(payload.strip_sequence, old.strip_sequence);
    assert_eq!(payload.failed_segments, vec![failed]);
    assert_eq!(task.source_revision, marked_chunk.modify_ts);
    assert_eq!(
        task.estimated_queue_bytes,
        u64::from(failed.unit_count) * u64::from(old.unit_kb) * 1024 * 4
    );

    // A terminal task must not suppress repair while its durable failure
    // marker still exists.
    let mut completed = task.clone();
    completed.state = ChunkTaskState::Completed;
    completed.revision = completed.revision.saturating_add(1);
    tasks.write_transition(Some(&task), &completed).await.unwrap();
    assert_eq!(restarted.admit_chunk(&marked_chunk, 102).await.unwrap(), 1);
    let revived = tasks
        .get(&chunk_id, completed.kind, &completed.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(revived.state, ChunkTaskState::Pending);
    assert_eq!(revived.revision, completed.revision.saturating_add(1));
    assert_eq!(revived.attempt, 0);
}

#[tokio::test]
async fn degraded_ec_strip_is_admitted_as_a_persistent_placement_task() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let harness = ChunkdbHarness::start(&cluster).await;
    let coordinator = PlacementRepairCoordinator::new(Arc::clone(&harness.handler), Arc::clone(&tasks));
    let chunk_id = ChunkId { high: 41, low: 42 };
    let strip = ChunkStrip {
        strip_sequence: 7,
        strip_type: StripType::Ec as i32,
        strip: Some(Strip::EcStrip(EcStrip {
            data_num: 2,
            code_num: 1,
            ec_state: EcState::Parity as i32,
            segments: Vec::new(),
        })),
        placement_assessment: Some(PlacementAssessment {
            loss_budget: 1,
            max_fragments_per_rack: 2,
            max_fragments_per_node: 1,
            max_fragments_per_disk: 1,
            rack_protected: false,
            node_protected: true,
            disk_protected: true,
            topology_generation: 1,
            usage_fresh: true,
        }),
        placement_repair_required: true,
        ..ChunkStrip::default()
    };
    let chunk = Chunk {
        id: Some(chunk_id),
        modify_ts: 9,
        strips: vec![strip],
        ..Chunk::default()
    };

    assert_eq!(coordinator.admit_chunk(&chunk, 100).await.unwrap(), 1);
    assert_eq!(coordinator.admit_chunk(&chunk, 101).await.unwrap(), 0);
    let ready = tasks.scan_ready(101, 16).await.unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].kind, TASK_KIND_REPAIR_PLACEMENT);
    let task = tasks
        .get(&chunk_id, TASK_KIND_REPAIR_PLACEMENT, &ready[0].task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.source_revision, 9);
    assert_eq!(task.max_attempts, u32::MAX);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn chunkdb_full_stack_allocate_seal_delete() {
    // Skip if crowdb-kv-server binary is not built.
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }

    // 1. Start the kv cluster.
    let cluster = KvCluster::start().await;
    eprintln!("kv cluster started");

    // 2. Seed hardware metadata.
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    eprintln!("hardware seeded");

    // 3. Start diskdb in-process.
    let diskdb = DiskdbServer::start(&cluster).await;
    eprintln!("diskdb started: {}", diskdb.rpc_endpoint);

    // 4. Wire chunkdb handler.
    let harness = ChunkdbHarness::start(&cluster).await;
    eprintln!("chunkdb harness ready");

    // 5. Allocate a chunk with three prefetched strips (1 MiB each,
    // 3 mirror copies).
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1, // 1 unit per strip
            3, // 3 strips
            StripType::Mirror,
            0,
            0,
            3, // 3 mirror copies
            ChunkType::Repo,
            0,
            0,
        )
        .await
        .expect("allocate_chunk");
    assert_eq!(chunk.state, ChunkState::Active as i32);
    assert!(!chunk.strips.is_empty(), "chunk should have strips");
    for strip in &chunk.strips {
        let assessment = strip
            .placement_assessment
            .as_ref()
            .expect("allocated strips persist a physical placement assessment");
        assert_eq!(assessment.loss_budget, 2);
        assert_eq!(assessment.max_fragments_per_rack, 1);
        assert_eq!(assessment.max_fragments_per_node, 1);
        assert_eq!(assessment.max_fragments_per_disk, 1);
        assert!(assessment.rack_protected);
        assert!(assessment.node_protected);
        assert!(assessment.disk_protected);
        assert!(!strip.placement_repair_required);
    }
    eprintln!("chunk allocated: {} strips", chunk.strips.len());

    // 6. Query the chunk.
    let chunk_id = chunk.id.as_ref().expect("chunk has id");
    let queried = harness.handler.query_chunk(chunk_id).await.expect("query_chunk");
    assert_eq!(queried.id, chunk.id);
    assert_eq!(queried.state, ChunkState::Active as i32);
    eprintln!("chunk queried");

    // 7. Seal the chunk.
    let sealed = harness
        .handler
        .seal_chunk(chunk_id, 100)
        .await
        .expect("seal_chunk");
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    assert_eq!(sealed.sealed_length, 100);
    assert_eq!(sealed.strips.len(), 1);
    assert_eq!(sealed.capacity, 1024);
    assert!(sealed.cleanup_intents.is_empty());
    eprintln!("chunk sealed");

    // 8. Delete the chunk.
    let deleted = harness
        .handler
        .delete_chunk(chunk_id)
        .await
        .expect("delete_chunk");
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    eprintln!("chunk deleted");

    // 9. Delete again → should return the same idempotent tombstone.
    let deleted_again = harness
        .handler
        .delete_chunk(chunk_id)
        .await
        .expect("idempotent delete");
    assert_eq!(deleted_again.state, ChunkState::Deleted as i32);
    eprintln!("second delete returned Deleted (idempotent)");

    // 10. Query after delete → should return the deleted chunk (state=Deleted).
    let queried_after = harness
        .handler
        .query_chunk(chunk_id)
        .await
        .expect("query after delete");
    assert_eq!(queried_after.state, ChunkState::Deleted as i32);
    eprintln!("chunk queried after delete (state=Deleted)");
}

#[tokio::test]
async fn chunkdb_fenced_range_replacement_is_idempotent_and_preserves_geometry() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 2, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let old = chunk.strips[0].clone();
    let Some(Strip::MirrorStrip(mut mirror)) = old.strip.clone() else {
        panic!("expected mirror strip");
    };
    let failed = mirror.segments[0];
    let replacement = harness
        .handler
        .allocate_replacement_segment(
            &chunk_id,
            &failed,
            &mirror.segments[1..],
            &[failed.disk_id.unwrap()],
        )
        .await
        .unwrap();
    assert_ne!(replacement.disk_id, failed.disk_id);
    mirror.segments[0] = replacement;
    let mut installed = old.clone();
    installed.strip = Some(Strip::MirrorStrip(mirror));
    let operation_id = ChunkId { high: 9, low: 7 };
    let updated = harness
        .handler
        .replace_chunk_strip_range(
            &chunk_id,
            chunk.modify_ts,
            0,
            std::slice::from_ref(&old),
            std::slice::from_ref(&installed),
            operation_id,
        )
        .await
        .unwrap();
    assert_eq!(updated.capacity, chunk.capacity);
    assert_eq!(updated.next_strip_sequence, chunk.next_strip_sequence);
    assert_eq!(updated.strips[0], installed);
    assert_eq!(updated.cleanup_intents.len(), 1);
    assert_eq!(updated.cleanup_intents[0].retired_segments, vec![failed]);

    let retried = harness
        .handler
        .replace_chunk_strip_range(
            &chunk_id,
            chunk.modify_ts,
            0,
            std::slice::from_ref(&old),
            std::slice::from_ref(&installed),
            operation_id,
        )
        .await
        .unwrap();
    assert_eq!(retried, updated);
    assert_stale_replacement_conflicts(&harness, chunk_id, &chunk, &installed).await;

    let mut consolidated = harness
        .allocator
        .allocate_strip(
            &harness.topology.snapshot(),
            &chunk_id,
            StripAllocType::Mirror { copy_count: 3 },
            2,
            updated.strips[0].strip_sequence,
            &PlacementConstraints::new(),
        )
        .await
        .unwrap();
    consolidated.chunk_offset = updated.strips[0].chunk_offset;
    let consolidated = harness
        .handler
        .replace_chunk_strip_range(
            &chunk_id,
            updated.modify_ts,
            0,
            &updated.strips,
            std::slice::from_ref(&consolidated),
            ChunkId { high: 11, low: 12 },
        )
        .await
        .unwrap();
    assert_eq!(consolidated.strips.len(), 1);
    assert_eq!(consolidated.capacity, chunk.capacity);
    assert_eq!(consolidated.next_strip_sequence, chunk.next_strip_sequence);
    assert_eq!(consolidated.cleanup_intents.len(), 2);
}

async fn assert_stale_replacement_conflicts(
    harness: &ChunkdbHarness,
    chunk_id: ChunkId,
    original: &crowdb_protocol::chunkdb::rpc::Chunk,
    installed: &crowdb_protocol::chunkdb::rpc::ChunkStrip,
) {
    let conflict = harness
        .handler
        .replace_chunk_strip_range(
            &chunk_id,
            original.modify_ts,
            0,
            std::slice::from_ref(&original.strips[1]),
            std::slice::from_ref(installed),
            ChunkId { high: 10, low: 8 },
        )
        .await;
    assert!(matches!(conflict, Err(LifecycleError::StateConflict)));
}

#[tokio::test]
async fn mirror_range_is_atomically_replaced_by_tentative_ec_strip() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start_with_layout_validity(&cluster, Duration::from_millis(1)).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 8, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate mirror range");
    let chunk_id = chunk.id.expect("chunk id");
    let chunk = harness
        .handler
        .seal_chunk(&chunk_id, chunk.capacity)
        .await
        .expect("seal mirror range");

    let task_bindings = BindingCache::new();
    task_bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let task_store = Arc::new(TaskStore::new(cluster.make_crowdb_client(), task_bindings));
    let coordinator = ConversionCoordinator::new(Arc::clone(&harness.handler), Arc::clone(&task_store));
    let prepared = coordinator
        .prepare(
            chunk_id,
            chunk.modify_ts,
            0,
            chunk.strips.clone(),
            8,
            4,
            7001,
            30_000,
            100,
        )
        .await
        .expect("prepare durable conversion task");
    let replacement = prepared.replacement_strip;
    let Some(Strip::EcStrip(ec)) = &replacement.strip else {
        panic!("expected EC replacement");
    };
    assert_eq!((ec.data_num, ec.code_num, ec.segments.len()), (8, 4, 12));
    assert_eq!(replacement.capacity, chunk.capacity);
    assert_eq!(replacement.chunk_offset, 0);

    let converted = coordinator
        .complete(chunk_id, prepared.task_id, 7001, 101)
        .await
        .expect("publish durable EC replacement");
    let mut durable_replacement = replacement.clone();
    let Some(Strip::EcStrip(ec)) = &mut durable_replacement.strip else {
        unreachable!();
    };
    ec.ec_state = crowdb_protocol::chunkdb::rpc::EcState::Parity as i32;
    assert_eq!(converted.strips, vec![durable_replacement.clone()]);
    assert_eq!(converted.last_strip_replacement, Some(prepared.operation_id));
    assert_eq!(converted.cleanup_intents.len(), 1);
    assert_eq!(converted.cleanup_intents[0].retired_segments.len(), 24);

    let retry = coordinator
        .complete(chunk_id, prepared.task_id, 7001, 102)
        .await
        .expect("idempotent task completion retry");
    assert_eq!(retry, converted);

    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(harness.handler.reconcile_pending_chunks().await.unwrap(), 1);
    let reclaimed = harness.handler.query_chunk(&chunk_id).await.unwrap();
    assert_eq!(reclaimed.strips, vec![durable_replacement]);
    assert!(reclaimed.cleanup_intents.is_empty());
}

#[tokio::test]
async fn deletion_during_conversion_clears_task_ownership_before_tentative_cleanup() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 8, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate mirror range");
    let chunk_id = chunk.id.expect("chunk id");
    let chunk = harness
        .handler
        .seal_chunk(&chunk_id, chunk.capacity)
        .await
        .expect("seal mirror range");

    let task_bindings = BindingCache::new();
    task_bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let task_store = Arc::new(TaskStore::new(cluster.make_crowdb_client(), task_bindings));
    let coordinator = ConversionCoordinator::new(Arc::clone(&harness.handler), Arc::clone(&task_store));
    let prepared = coordinator
        .prepare(
            chunk_id,
            chunk.modify_ts,
            0,
            chunk.strips,
            8,
            4,
            7002,
            30_000,
            100,
        )
        .await
        .expect("prepare conversion");
    let task = task_store
        .get(&chunk_id, TASK_KIND_MIRROR_TO_EC, &prepared.task_id)
        .await
        .unwrap()
        .expect("durable conversion task");
    assert_conversion_task_pending(
        &harness.handler,
        &task_store,
        chunk_id,
        &task,
        &prepared.replacement_strip,
    )
    .await;

    harness
        .handler
        .delete_chunk(&chunk_id)
        .await
        .expect("delete chunk");
    let io = Arc::new(ConversionDiskIo::empty_for_tests());
    let mut registry = MetricsRegistry::new();
    let metrics = ChunkdbMetrics::register(&mut registry).conversion;
    let manager = Arc::new(TaskManager::new(Arc::clone(&task_store), 7002, 30_000));
    let executor = TaskExecutor::new(
        manager,
        1,
        vec![Arc::new(MirrorToEcTaskHandler::new(
            Arc::clone(&harness.handler),
            Arc::clone(&task_store),
            io,
            metrics,
            50,
            1,
        ))],
    )
    .unwrap();
    executor.execute(TaskClaim { task }).await.unwrap();

    let failed = task_store
        .get(&chunk_id, TASK_KIND_MIRROR_TO_EC, &prepared.task_id)
        .await
        .unwrap()
        .expect("terminal conversion task");
    assert_eq!(failed.state, ChunkTaskState::Failed);
    assert!(decode_payload(&failed.payload)
        .unwrap()
        .replacement_strip
        .is_none());
    harness
        .handler
        .discard_conversion_strip(&chunk_id, &prepared.replacement_strip)
        .await
        .expect("reclaim unreferenced tentative replacement");
    let deleted = harness.handler.query_chunk(&chunk_id).await.unwrap();
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    assert!(deleted.strips.is_empty());
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn relocation_handoff_claims_publishes_and_defers_source_free() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start_with_layout_validity(&cluster, Duration::from_millis(1)).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let Strip::MirrorStrip(mirror) = chunk.strips[0].strip.as_ref().unwrap() else {
        unreachable!();
    };
    let source = mirror.segments[0];
    let target = harness
        .handler
        .allocate_replacement_segment(&chunk_id, &source, &mirror.segments[1..], &[])
        .await
        .unwrap();
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let manager = Arc::new(TaskManager::new(Arc::clone(&tasks), 8000, 30_000));
    let coordinator = Arc::new(RelocationCoordinator::new(Arc::clone(&manager)));
    let operation_id = crowdb_protocol::chunk_task::relocation_operation_id(&source).unwrap();
    let request = RelocateSegmentHandoffRequest {
        operation_id: Some(operation_id),
        chunk_id: Some(chunk_id),
        source: Some(source),
        target: Some(target),
    };
    let mut invalid_identity = request.clone();
    invalid_identity.operation_id = Some(ChunkId {
        high: operation_id.high,
        low: operation_id.low ^ 1,
    });
    assert!(matches!(
        coordinator.admit(&invalid_identity, 1).await,
        Err(RelocationAdmissionError::InvalidGeometry)
    ));
    assert!(tasks
        .list_partition(&chunk_id)
        .await
        .unwrap()
        .into_iter()
        .all(|t| t.kind == TASK_KIND_FINALIZE_CHUNK));
    let port = port_alloc::alloc_test_port(ServicePort::ChunkdbRpc);
    let endpoint = format!("http://127.0.0.1:{port}");
    let server = Arc::new(crowdb_rpc_ffi::RpcServer::new(None));
    server.listen("127.0.0.1", i32::from(port)).unwrap();
    let mut registry = MetricsRegistry::new();
    let service = Arc::new(
        ChunkdbRpcService::new(
            Arc::clone(&harness.handler),
            Arc::new(ChunkdbMetrics::register(&mut registry)),
            tokio::runtime::Handle::current(),
        )
        .with_task_store(Arc::clone(&tasks))
        .with_relocation(Arc::clone(&coordinator)),
    );
    service.register_handlers(&server);
    server.start();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match ChunkdbRpcTransport::new()
            .send_relocate_segment_handoff(&endpoint, &request)
            .await
        {
            Ok(response) => {
                assert_eq!(
                    RelocationHandoffDisposition::try_from(response.disposition).unwrap(),
                    RelocationHandoffDisposition::Accepted
                );
                break;
            }
            Err(error) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "relocation RPC did not become ready: {error}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    let duplicate = ChunkdbRpcTransport::new()
        .send_relocate_segment_handoff(&endpoint, &request)
        .await
        .unwrap();
    assert_eq!(
        RelocationHandoffDisposition::try_from(duplicate.disposition).unwrap(),
        RelocationHandoffDisposition::Accepted
    );
    assert_eq!(
        tasks
            .list_partition(&chunk_id)
            .await
            .unwrap()
            .into_iter()
            .filter(|t| t.kind != TASK_KIND_FINALIZE_CHUNK)
            .count(),
        1
    );
    let owner = SegmentOwnerResolver::new(Arc::clone(&harness.handler), Arc::clone(&tasks));
    assert_eq!(
        owner.resolve(&chunk_id, &target).await.unwrap(),
        SegmentOwnerDisposition::TaskPending
    );
    let executor = TaskExecutor::new(
        Arc::clone(&manager),
        1,
        vec![Arc::new(RelocateSegmentTaskHandler::new(
            Arc::clone(&harness.handler),
            Arc::clone(&manager),
        ))],
    )
    .unwrap();
    for _ in 0..4 {
        let ready = tasks.scan_ready(u64::MAX, 1).await.unwrap();
        if ready.is_empty() {
            break;
        }
        let claim = manager.claim(&ready[0], u64::MAX).await.unwrap().unwrap();
        executor.execute(claim).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let response = ChunkdbRpcTransport::new()
        .send_relocate_segment_handoff(&endpoint, &request)
        .await
        .unwrap();
    assert_eq!(
        RelocationHandoffDisposition::try_from(response.disposition).unwrap(),
        RelocationHandoffDisposition::Published
    );
    let published = harness.handler.query_chunk(&chunk_id).await.unwrap();
    let Strip::MirrorStrip(published_mirror) = published.strips[0].strip.as_ref().unwrap() else {
        unreachable!();
    };
    assert!(!published_mirror.segments.contains(&source));
    assert!(published_mirror.segments.contains(&target));
    assert!(published.cleanup_intents.is_empty());

    let kv = cluster.make_ddb_kv_client();
    let source_records = kv
        .read_zone_records(
            (STORE_ID, DATA_GROUP_ID),
            &source.disk_id.unwrap(),
            source.zone_index,
        )
        .await
        .unwrap();
    assert!(!source_records.free.iter().any(|record| {
        record.key.unit_offset == source.unit_offset && record.key.allocation_ts == source.allocation_ts
    }));
}

#[tokio::test]
async fn relocation_rejects_a_target_that_weakens_physical_placement() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let Strip::MirrorStrip(mirror) = chunk.strips[0].strip.as_ref().unwrap() else {
        unreachable!();
    };
    let source = mirror.segments[0];
    let mut target = mirror.segments[1];
    target.allocation_ts = target.allocation_ts.saturating_add(1);
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let manager = Arc::new(TaskManager::new(Arc::clone(&tasks), 8001, 30_000));
    let coordinator = RelocationCoordinator::new(Arc::clone(&manager));
    let operation_id = crowdb_protocol::chunk_task::relocation_operation_id(&source).unwrap();
    assert_eq!(
        coordinator
            .admit(
                &RelocateSegmentHandoffRequest {
                    operation_id: Some(operation_id),
                    chunk_id: Some(chunk_id),
                    source: Some(source),
                    target: Some(target),
                },
                1,
            )
            .await
            .unwrap(),
        RelocationHandoffDisposition::Accepted
    );
    let executor = TaskExecutor::new(
        Arc::clone(&manager),
        1,
        vec![Arc::new(RelocateSegmentTaskHandler::new(
            Arc::clone(&harness.handler),
            Arc::clone(&manager),
        ))],
    )
    .unwrap();
    let ready = tasks.scan_ready(u64::MAX, 1).await.unwrap();
    let claim = manager.claim(&ready[0], u64::MAX).await.unwrap().unwrap();
    executor.execute(claim).await.unwrap();

    let stored = tasks
        .get(&chunk_id, TASK_KIND_RELOCATE_SEGMENT, &operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, ChunkTaskState::Completed);
    let payload: RelocateSegmentTaskPayload = serde_json::from_slice(&stored.payload).unwrap();
    assert_eq!(payload.disposition, RelocateSegmentTaskDisposition::Rejected);
    assert_eq!(harness.handler.query_chunk(&chunk_id).await.unwrap(), chunk);
}

#[tokio::test]
async fn relocation_marks_deleted_owner_stale_without_publishing_target() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let Strip::MirrorStrip(mirror) = chunk.strips[0].strip.as_ref().unwrap() else {
        unreachable!();
    };
    let source = mirror.segments[0];
    let target = harness
        .handler
        .allocate_replacement_segment(&chunk_id, &source, &mirror.segments[1..], &[])
        .await
        .unwrap();
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let manager = Arc::new(TaskManager::new(Arc::clone(&tasks), 8002, 30_000));
    let coordinator = RelocationCoordinator::new(Arc::clone(&manager));
    let operation_id = crowdb_protocol::chunk_task::relocation_operation_id(&source).unwrap();
    coordinator
        .admit(
            &RelocateSegmentHandoffRequest {
                operation_id: Some(operation_id),
                chunk_id: Some(chunk_id),
                source: Some(source),
                target: Some(target),
            },
            1,
        )
        .await
        .unwrap();
    harness.handler.delete_chunk(&chunk_id).await.unwrap();
    let executor = TaskExecutor::new(
        Arc::clone(&manager),
        1,
        vec![Arc::new(RelocateSegmentTaskHandler::new(
            Arc::clone(&harness.handler),
            Arc::clone(&manager),
        ))],
    )
    .unwrap();
    let ready = tasks.scan_ready(u64::MAX, 1).await.unwrap();
    let claim = manager.claim(&ready[0], u64::MAX).await.unwrap().unwrap();
    executor.execute(claim).await.unwrap();

    let stored = tasks
        .get(&chunk_id, TASK_KIND_RELOCATE_SEGMENT, &operation_id)
        .await
        .unwrap()
        .unwrap();
    let payload: RelocateSegmentTaskPayload = serde_json::from_slice(&stored.payload).unwrap();
    assert_eq!(payload.disposition, RelocateSegmentTaskDisposition::Stale);
    let owner = SegmentOwnerResolver::new(Arc::clone(&harness.handler), tasks);
    assert_eq!(
        owner.resolve(&chunk_id, &target).await.unwrap(),
        SegmentOwnerDisposition::Absent
    );
    let deleted = harness.handler.query_chunk(&chunk_id).await.unwrap();
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    assert!(deleted.strips.is_empty());
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn cross_domain_rebalance_hands_one_safe_move_to_target_diskdb() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary is unavailable");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start_with_layout_validity(&cluster, Duration::from_millis(1)).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let Strip::MirrorStrip(mirror) = chunk.strips[0].strip.as_ref().unwrap() else {
        unreachable!();
    };
    let snapshot = harness.topology.snapshot();
    let occupied: HashSet<_> = mirror
        .segments
        .iter()
        .filter_map(|segment| segment.disk_id)
        .filter_map(|disk| snapshot.disk_location(disk))
        .map(|location| location.disk_group_id)
        .collect();
    let source = mirror.segments[0];
    let source_dg = snapshot
        .disk_location(source.disk_id.unwrap())
        .unwrap()
        .disk_group_id;
    let target_dg = common::cluster::seeded_dg_ids()
        .into_iter()
        .find(|disk_group_id| !occupied.contains(disk_group_id))
        .unwrap();
    let capacity = 1_000_000_000_000u64;
    let summaries: Vec<_> = common::cluster::seeded_dg_ids()
        .into_iter()
        .map(|disk_group_id| {
            let used_bytes = if disk_group_id == source_dg {
                capacity * 8 / 10
            } else if disk_group_id == target_dg {
                capacity / 10
            } else {
                capacity / 2
            };
            DiskGroupUsageSummary {
                disk_group_id,
                capacity_bytes: capacity,
                used_bytes,
                free_bytes: capacity - used_bytes,
                disk_count: 3,
                allocatable_disk_count: 3,
                allocatable_capacity_bytes: capacity,
                allocatable_used_bytes: used_bytes,
                allocatable_free_bytes: capacity - used_bytes,
                sampled_at_ms: 1,
            }
        })
        .collect();
    cluster
        .make_service_registry_client()
        .register_diskdb(
            common::cluster::INSTANCE_ID,
            &diskdb.rpc_endpoint,
            &common::cluster::seeded_dg_ids(),
            &summaries,
        )
        .await
        .unwrap();
    let refreshed = crowdb_chunkdb::topology::build_snapshot(&cluster.make_hardware_client())
        .await
        .unwrap();
    harness.topology.replace(refreshed);
    harness
        .pool
        .update_disk_id_lookup(&harness.topology.snapshot().disk_groups());
    let planner = PlacementRebalancePlanner::new(
        Arc::clone(&harness.handler),
        Arc::clone(&harness.pool),
        PlacementRebalanceConfig {
            enabled: true,
            scan_interval_secs: 1,
            imbalance_threshold_pct: 20,
            hysteresis_secs: 10,
            min_target_free_bytes: 0,
            max_moves_per_cycle: 1,
        },
    );
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let manager = Arc::new(TaskManager::new(Arc::clone(&tasks), 8003, 30_000));
    let coordinator = Arc::new(RelocationCoordinator::new(Arc::clone(&manager)));
    diskdb.set_relocation_owner(coordinator);
    assert_eq!(planner.run_once(1_000).await.unwrap(), 0);
    assert_eq!(planner.run_once(10_999).await.unwrap(), 0);
    assert_eq!(planner.run_once(11_000).await.unwrap(), 1);
    let kv = cluster.make_ddb_kv_client();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let accepted = loop {
        let journals = kv
            .list_relocation_journals((STORE_ID, DATA_GROUP_ID))
            .await
            .unwrap();
        if let Some((_, journal)) = journals
            .iter()
            .find(|(_, journal)| journal.source == Some(source))
        {
            assert_eq!(journal.target_disk_group_id, target_dg);
            if RelocationJournalPhase::try_from(journal.phase) == Ok(RelocationJournalPhase::Accepted) {
                break journal.clone();
            }
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let executor = TaskExecutor::new(
        Arc::clone(&manager),
        1,
        vec![Arc::new(RelocateSegmentTaskHandler::new(
            Arc::clone(&harness.handler),
            Arc::clone(&manager),
        ))],
    )
    .unwrap();
    for _ in 0..4 {
        let ready = tasks.scan_ready(u64::MAX, 1).await.unwrap();
        if ready.is_empty() {
            break;
        }
        let claim = manager.claim(&ready[0], u64::MAX).await.unwrap().unwrap();
        executor.execute(claim).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    harness
        .pool
        .execute_relocation(crowdb_protocol::diskdb::rpc::ExecuteRelocationRequest {
            target_disk_group_id: target_dg,
            source: accepted.source,
            target: accepted.target,
        })
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let journals = kv
            .list_relocation_journals((STORE_ID, DATA_GROUP_ID))
            .await
            .unwrap();
        if journals.iter().any(|(_, journal)| {
            journal.source == Some(source)
                && RelocationJournalPhase::try_from(journal.phase) == Ok(RelocationJournalPhase::SourceFreed)
        }) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let source_records = kv
        .read_zone_records(
            (STORE_ID, DATA_GROUP_ID),
            &source.disk_id.unwrap(),
            source.zone_index,
        )
        .await
        .unwrap();
    assert!(source_records.free.iter().any(|record| {
        record.key.unit_offset == source.unit_offset && record.key.allocation_ts == source.allocation_ts
    }));
    let published = harness.handler.query_chunk(&chunk_id).await.unwrap();
    let Strip::MirrorStrip(published_mirror) = published.strips[0].strip.as_ref().unwrap() else {
        unreachable!();
    };
    assert!(!published_mirror.segments.contains(&source));
    assert!(published_mirror.segments.contains(&accepted.target.unwrap()));
    let assessment = published.strips[0].placement_assessment.as_ref().unwrap();
    assert!(assessment.rack_protected && assessment.node_protected && assessment.disk_protected);

    let journal_count = kv
        .list_relocation_journals((STORE_ID, DATA_GROUP_ID))
        .await
        .unwrap()
        .len();
    let balanced_summaries: Vec<_> = common::cluster::seeded_dg_ids()
        .into_iter()
        .map(|disk_group_id| DiskGroupUsageSummary {
            disk_group_id,
            capacity_bytes: capacity,
            used_bytes: capacity / 2,
            free_bytes: capacity / 2,
            disk_count: 3,
            allocatable_disk_count: 3,
            allocatable_capacity_bytes: capacity,
            allocatable_used_bytes: capacity / 2,
            allocatable_free_bytes: capacity / 2,
            sampled_at_ms: 2,
        })
        .collect();
    cluster
        .make_service_registry_client()
        .register_diskdb(
            common::cluster::INSTANCE_ID,
            &diskdb.rpc_endpoint,
            &common::cluster::seeded_dg_ids(),
            &balanced_summaries,
        )
        .await
        .unwrap();
    let refreshed = crowdb_chunkdb::topology::build_snapshot(&cluster.make_hardware_client())
        .await
        .unwrap();
    harness.topology.replace(refreshed);
    assert_eq!(planner.run_once(12_000).await.unwrap(), 0);
    assert_eq!(
        kv.list_relocation_journals((STORE_ID, DATA_GROUP_ID))
            .await
            .unwrap()
            .len(),
        journal_count
    );
}

#[tokio::test]
async fn chunkdb_restart_reconciles_expired_replacement_cleanup_intent() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: crowdb-kv-server binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start_with_layout_validity(&cluster, Duration::from_millis(1)).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let old = chunk.strips[0].clone();
    let Some(Strip::MirrorStrip(mut mirror)) = old.strip.clone() else {
        panic!("expected mirror strip");
    };
    let failed = mirror.segments[0];
    let replacement = harness
        .handler
        .allocate_replacement_segment(
            &chunk_id,
            &failed,
            &mirror.segments[1..],
            &[failed.disk_id.unwrap()],
        )
        .await
        .unwrap();
    mirror.segments[0] = replacement;
    let mut installed = old.clone();
    installed.strip = Some(Strip::MirrorStrip(mirror));
    harness
        .handler
        .replace_chunk_strip_range(
            &chunk_id,
            chunk.modify_ts,
            0,
            std::slice::from_ref(&old),
            std::slice::from_ref(&installed),
            ChunkId { high: 31, low: 41 },
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;

    let restarted = LifecycleHandler::new(
        Arc::clone(&harness.store),
        Arc::clone(&harness.allocator),
        harness.topology.clone(),
    )
    .with_layout_validity(Duration::from_millis(1))
    .with_locks(Arc::new(ChunkLockMap::new(
        10_000,
        Arc::new(LifecycleMetrics::new()),
        Duration::from_secs(60),
    )));
    assert_eq!(restarted.reconcile_pending_chunks().await.unwrap(), 1);
    let reconciled = restarted.query_chunk(&chunk_id).await.unwrap();
    assert_eq!(reconciled.strips[0], installed);
    assert!(reconciled.cleanup_intents.is_empty());
    assert_eq!(
        reconciled.last_strip_replacement,
        Some(ChunkId { high: 31, low: 41 })
    );
}

#[tokio::test]
async fn chunkdb_lock_serializes_concurrent_append() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk.
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Two concurrent appends on the same chunk — both should succeed
    // (serialized by the per-chunk lock, not corrupted).
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let id1 = chunk_id;
    let id2 = chunk_id;
    let t1 = tokio::spawn(async move {
        h1.append_chunk(&id1, 1, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .expect("append 1")
    });
    let t2 = tokio::spawn(async move {
        h2.append_chunk(&id2, 1, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .expect("append 2")
    });
    let r1 = t1.await.expect("task 1");
    let r2 = t2.await.expect("task 2");
    // One request appends; the other observes a stale revision and receives
    // the refreshed parent without allocating a duplicate strip.
    let outcomes = [&r1, &r2];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.strips.len() == 1)
            .count(),
        1
    );
    assert_eq!(
        outcomes.iter().filter(|outcome| outcome.chunk.is_some()).count(),
        1
    );
    let current = harness.handler.query_chunk(&chunk_id).await.expect("query chunk");
    assert_eq!(current.strips.len(), 2);
    assert_eq!(current.modify_ts, 2);
}

#[tokio::test]
async fn chunkdb_lock_no_deadlock_different_chunks() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate two chunks.
    let chunk_a = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate A");
    let chunk_b = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate B");
    let id_a = *chunk_a.id.as_ref().expect("chunk A id");
    let id_b = *chunk_b.id.as_ref().expect("chunk B id");

    // Concurrent append on different chunks — no deadlock, both succeed quickly.
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let t1 = tokio::spawn(async move {
        h1.append_chunk(&id_a, 1, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .expect("append A")
    });
    let t2 = tokio::spawn(async move {
        h2.append_chunk(&id_b, 1, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .expect("append B")
    });
    // If there's a deadlock, this timeout will fire.
    let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (t1.await.expect("task A"), t2.await.expect("task B"))
    })
    .await
    .expect("no deadlock — both appends completed within 10s");
    assert_eq!(r1.strips.len(), 1, "chunk A should return one appended strip");
    assert_eq!(r2.strips.len(), 1, "chunk B should return one appended strip");
    assert_eq!(r1.modify_ts, 2);
    assert_eq!(r2.modify_ts, 2);
    eprintln!("no deadlock: both chunks appended independently");
}

#[tokio::test]
async fn chunkdb_cache_hit_on_second_query() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk (populates cache via populate_cache for auto-gen ID).
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Append (should be a cache hit — no store round-trip for get_chunk).
    let appended = harness
        .handler
        .append_chunk(&chunk_id, 1, 4, StripType::Mirror, 0, 0, 3, 1)
        .await
        .expect("append_chunk");
    assert_eq!(appended.strips.len(), 4, "should return the appended batch");
    assert!(
        appended
            .strips
            .windows(2)
            .all(|pair| pair[1].strip_sequence == pair[0].strip_sequence + 1
                && pair[1].chunk_offset == pair[0].chunk_offset + pair[0].capacity),
        "batch must retain sequence and logical-offset order"
    );
    assert_eq!(appended.modify_ts, 2);

    // Seal (should also be a cache hit after append refreshed the cache).
    let sealed = harness
        .handler
        .seal_chunk(&chunk_id, 100)
        .await
        .expect("seal_chunk");
    assert_eq!(sealed.state, ChunkState::Sealed as i32);

    // Check metrics — cache should have hits.
    if let Some(locks) = harness.handler.locks() {
        let snap = locks.metrics_snapshot();
        assert!(
            snap.cache_hit_count > 0 || snap.cache_miss_count > 0,
            "cache metrics should be non-zero (hits={}, misses={})",
            snap.cache_hit_count,
            snap.cache_miss_count
        );
        eprintln!(
            "cache metrics: hits={}, misses={}, size={}",
            snap.cache_hit_count, snap.cache_miss_count, snap.cache_size
        );
    }
    eprintln!("cache hit on second query verified");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn reserved_strips_stay_hidden_until_idempotent_confirmation() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let writer_epoch = 71;
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::Repo,
            writer_epoch,
            30_000,
        )
        .await
        .expect("allocate chunk");
    let chunk_id = chunk.id.expect("chunk id");
    let group_id = ChunkId { high: 91, low: 92 };
    let mut fence = ReservationFence {
        expected_modify_ts: chunk.modify_ts,
        writer_epoch,
        lease_generation: 1,
        lease_ms: 30_000,
    };
    let reserved = harness
        .handler
        .reserve_strip_group(
            &chunk_id,
            &group_id,
            fence,
            ReserveGroupSpec {
                strip_size: 1,
                strip_count: 2,
                copy_count: 3,
                conversion_data_num: 0,
                conversion_code_num: 0,
            },
        )
        .await
        .expect("reserve strips");
    let group = reserved.group.expect("reservation group");
    fence.expected_modify_ts = reserved.chunk.modify_ts;
    assert_eq!(reserved.chunk.strips.len(), 1);
    assert_eq!(group.strips.len(), 2);
    assert!(group
        .states
        .iter()
        .all(|state| *state == StripReservationState::Reserved as i32));
    let first = &group.strips[0];
    let cursor = u64::from(first.chunk_offset + first.capacity) * 1024;
    let consumed = harness
        .handler
        .mutate_strip_reservation(
            &chunk_id,
            &group_id,
            fence,
            ReservationUpdate {
                strip_sequence: first.strip_sequence,
                action: StripReservationAction::Consume,
                acknowledged_cursor: cursor,
                closed_strip_sequence: None,
            },
        )
        .await
        .expect("consume reservation");
    assert_eq!(consumed.chunk.strips.len(), 1, "consume must remain invisible");
    let update = ReservationUpdate {
        strip_sequence: first.strip_sequence,
        action: StripReservationAction::Confirm,
        acknowledged_cursor: cursor,
        closed_strip_sequence: Some(first.strip_sequence),
    };
    let confirmed = harness
        .handler
        .mutate_strip_reservation(&chunk_id, &group_id, fence, update)
        .await
        .expect("confirm reservation");
    assert_eq!(confirmed.chunk.strips.len(), 2);
    let repeated = harness
        .handler
        .mutate_strip_reservation(&chunk_id, &group_id, fence, update)
        .await
        .expect("repeat confirmation");
    assert_eq!(
        repeated.chunk.strips.len(),
        2,
        "retry must not duplicate the strip"
    );

    let cancel = ReservationUpdate {
        strip_sequence: group.strips[1].strip_sequence,
        action: StripReservationAction::Cancel,
        acknowledged_cursor: cursor,
        closed_strip_sequence: None,
    };
    harness
        .handler
        .mutate_strip_reservation(&chunk_id, &group_id, fence, cancel)
        .await
        .expect("cancel reservation");
    harness
        .handler
        .mutate_strip_reservation(&chunk_id, &group_id, fence, cancel)
        .await
        .expect("repeat cancellation");
    harness
        .handler
        .seal_chunk(&chunk_id, confirmed.chunk.capacity)
        .await
        .expect("seal and remove terminal reservations");
}

async fn assert_legacy_consumed_reservation_is_retained(
    harness: &ChunkdbHarness,
    chunk_id: &ChunkId,
    group_id: &ChunkId,
    planned_cursor: u64,
) {
    let mut legacy = harness
        .store
        .get_reservation_group(chunk_id, group_id)
        .await
        .unwrap()
        .unwrap();
    legacy.planned_cursors[0] = 0;
    harness.store.put_reservation_group(&legacy).await.unwrap();
    let outcome = harness
        .handler
        .recover_expired_reservation_group(chunk_id, group_id, u64::MAX)
        .await
        .unwrap();
    assert_eq!(outcome, ReservationRecovery::Reconciled);
    let mut retained = harness
        .store
        .get_reservation_group(chunk_id, group_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.states[0], StripReservationState::Consumed as i32);
    retained.planned_cursors[0] = planned_cursor;
    harness.store.put_reservation_group(&retained).await.unwrap();
}

async fn assert_consumed_reservation_waits_for_reuse_grace(
    harness: &ChunkdbHarness,
    chunk_id: &ChunkId,
    group_id: &ChunkId,
) {
    let retained = harness
        .store
        .get_reservation_group(chunk_id, group_id)
        .await
        .unwrap()
        .unwrap();
    let outcome = harness
        .handler
        .recover_expired_reservation_group(
            chunk_id,
            group_id,
            retained
                .lease_deadline_ms
                .saturating_add(harness.handler.layout_validity_ms().saturating_sub(1)),
        )
        .await
        .unwrap();
    assert_eq!(outcome, ReservationRecovery::Reconciled);
    let retained = harness
        .store
        .get_reservation_group(chunk_id, group_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.states[0], StripReservationState::Consumed as i32);
}

#[tokio::test]
async fn expired_consumed_reservation_waits_for_reuse_grace() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let writer_epoch = 72;
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::Repo,
            writer_epoch,
            30_000,
        )
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let group_id = ChunkId { high: 93, low: 94 };
    let fence = ReservationFence {
        expected_modify_ts: chunk.modify_ts,
        writer_epoch,
        lease_generation: 1,
        lease_ms: 30_000,
    };
    let reserved = harness
        .handler
        .reserve_strip_group(
            &chunk_id,
            &group_id,
            fence,
            ReserveGroupSpec {
                strip_size: 1,
                strip_count: 2,
                copy_count: 3,
                conversion_data_num: 0,
                conversion_code_num: 0,
            },
        )
        .await
        .unwrap();
    let group = reserved.group.unwrap();
    let first = group.strips[0].clone();
    let planned_cursor = u64::from(first.chunk_offset + first.capacity) * 1024;
    harness
        .handler
        .mutate_strip_reservation(
            &chunk_id,
            &group_id,
            ReservationFence {
                expected_modify_ts: reserved.chunk.modify_ts,
                ..fence
            },
            ReservationUpdate {
                strip_sequence: first.strip_sequence,
                action: StripReservationAction::Consume,
                acknowledged_cursor: planned_cursor,
                closed_strip_sequence: Some(first.strip_sequence),
            },
        )
        .await
        .unwrap();

    assert_legacy_consumed_reservation_is_retained(&harness, &chunk_id, &group_id, planned_cursor).await;

    assert_consumed_reservation_waits_for_reuse_grace(&harness, &chunk_id, &group_id).await;

    let outcome = harness
        .handler
        .recover_expired_reservation_group(&chunk_id, &group_id, u64::MAX)
        .await
        .unwrap();
    assert_eq!(outcome, ReservationRecovery::Reconciled);
    let recovered = harness.handler.query_chunk(&chunk_id).await.unwrap();
    assert_eq!(recovered.acknowledged_cursor, 0);
    assert_eq!(recovered.strips.len(), 1);
    assert!(harness
        .handler
        .scan_reservation_groups(16)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn reservation_admission_rejects_overcommit_and_rebuilds_durable_usage() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let writer_epoch = 73;
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::Repo,
            writer_epoch,
            30_000,
        )
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let fence = ReservationFence {
        expected_modify_ts: chunk.modify_ts,
        writer_epoch,
        lease_generation: 1,
        lease_ms: 30_000,
    };
    let spec = ReserveGroupSpec {
        strip_size: 1,
        strip_count: 2,
        copy_count: 3,
        conversion_data_num: 0,
        conversion_code_num: 0,
    };

    harness.handler.update_reservation_limits(5, u64::MAX);
    let error = harness
        .handler
        .reserve_strip_group(&chunk_id, &ChunkId { high: 95, low: 1 }, fence, spec)
        .await
        .unwrap_err();
    assert!(matches!(error, LifecycleError::ReservationLimit));
    assert_eq!(
        harness.handler.rebuild_reservation_admission().await.unwrap(),
        (0, 0)
    );

    harness.handler.update_reservation_limits(6, u64::MAX);
    harness
        .handler
        .reserve_strip_group(&chunk_id, &ChunkId { high: 95, low: 2 }, fence, spec)
        .await
        .unwrap();
    let (blocks, bytes) = harness.handler.rebuild_reservation_admission().await.unwrap();
    assert_eq!(blocks, 6);
    assert!(bytes > 0);
}

#[tokio::test]
async fn completed_conversion_reservation_is_taken_over_as_a_durable_task() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let writer_epoch = 74;
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1024,
            0,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::Repo,
            writer_epoch,
            30_000,
        )
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let group_id = ChunkId { high: 96, low: 1 };
    let mut fence = ReservationFence {
        expected_modify_ts: chunk.modify_ts,
        writer_epoch,
        lease_generation: 1,
        lease_ms: 30_000,
    };
    let reserved = harness
        .handler
        .reserve_strip_group(
            &chunk_id,
            &group_id,
            fence,
            ReserveGroupSpec {
                strip_size: 1,
                strip_count: 8,
                copy_count: 3,
                conversion_data_num: 8,
                conversion_code_num: 4,
            },
        )
        .await
        .unwrap();
    fence.expected_modify_ts = reserved.chunk.modify_ts;
    for strip in &reserved.group.unwrap().strips {
        let cursor = u64::from(strip.chunk_offset + strip.capacity) * 1024;
        harness
            .handler
            .mutate_strip_reservation(
                &chunk_id,
                &group_id,
                fence,
                ReservationUpdate {
                    strip_sequence: strip.strip_sequence,
                    action: StripReservationAction::Consume,
                    acknowledged_cursor: cursor,
                    closed_strip_sequence: Some(strip.strip_sequence),
                },
            )
            .await
            .unwrap();
        let confirmed = harness
            .handler
            .mutate_strip_reservation(
                &chunk_id,
                &group_id,
                fence,
                ReservationUpdate {
                    strip_sequence: strip.strip_sequence,
                    action: StripReservationAction::Confirm,
                    acknowledged_cursor: cursor,
                    closed_strip_sequence: Some(strip.strip_sequence),
                },
            )
            .await
            .unwrap();
        fence.expected_modify_ts = confirmed.chunk.modify_ts;
    }

    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let coordinator = ConversionCoordinator::new(Arc::clone(&harness.handler), Arc::clone(&tasks));
    assert_eq!(coordinator.reconcile_reservations(16, u64::MAX).await.unwrap(), 1);
    assert!(harness
        .handler
        .scan_reservation_groups(16)
        .await
        .unwrap()
        .is_empty());
    let ready = tasks.scan_ready(u64::MAX, 16).await.unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].kind, TASK_KIND_MIRROR_TO_EC);
    assert_eq!(ready[0].partition_id, chunk_id);
}

#[tokio::test]
async fn generated_chunk_ids_stay_with_the_serving_range_owner() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let range_guard = Arc::new(RangeGuard::new(false));
    range_guard.replace(vec![OwnedRange {
        start: 0,
        end: 32_767,
        sub_range_index: 0,
    }]);
    let handler = LifecycleHandler::new(
        Arc::clone(&harness.store),
        Arc::clone(&harness.allocator),
        harness.topology.clone(),
    )
    .with_range_guard(range_guard);

    for _ in 0..16 {
        let chunk = handler
            .allocate_chunk(None, 1, 0, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
            .await
            .unwrap();
        assert!(hash_to_bucket(&chunk.id.unwrap()) <= 32_767);
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn conversion_reservation_allocates_joint_plan_and_cleans_every_early_tail() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    for tail in 1_u32..=7 {
        let writer_epoch = 100 + u64::from(tail);
        let chunk = harness
            .handler
            .allocate_chunk(
                None,
                1024,
                0,
                StripType::Mirror,
                0,
                0,
                3,
                ChunkType::Repo,
                writer_epoch,
                30_000,
            )
            .await
            .expect("allocate empty chunk");
        let chunk_id = chunk.id.expect("chunk id");
        let group_id = ChunkId {
            high: 200,
            low: u64::from(tail),
        };
        let mut fence = ReservationFence {
            expected_modify_ts: chunk.modify_ts,
            writer_epoch,
            lease_generation: 1,
            lease_ms: 30_000,
        };
        let reserved = harness
            .handler
            .reserve_strip_group(
                &chunk_id,
                &group_id,
                fence,
                ReserveGroupSpec {
                    strip_size: 1,
                    strip_count: 8,
                    copy_count: 3,
                    conversion_data_num: 8,
                    conversion_code_num: 4,
                },
            )
            .await
            .unwrap_or_else(|error| panic!("reserve conversion group for tail {tail}: {error}"));
        let group = reserved.group.expect("conversion group");
        fence.expected_modify_ts = reserved.chunk.modify_ts;
        assert!(reserved.chunk.strips.is_empty());
        assert_eq!(group.strips.len(), 8);
        assert_eq!(group.parity_segments.len(), 4);
        assert_eq!(group.preferred_survivors.len(), 8);
        assert!(group.strips.iter().all(|strip| match strip.strip.as_ref() {
            Some(Strip::MirrorStrip(mirror)) => mirror.segments.len() == 3,
            _ => false,
        }));
        let mut cursor = 0;
        for strip in group.strips.iter().take(tail as usize) {
            let planned_cursor = u64::from(strip.chunk_offset + strip.capacity) * 1024;
            harness
                .handler
                .mutate_strip_reservation(
                    &chunk_id,
                    &group_id,
                    fence,
                    ReservationUpdate {
                        strip_sequence: strip.strip_sequence,
                        action: StripReservationAction::Consume,
                        acknowledged_cursor: planned_cursor,
                        closed_strip_sequence: None,
                    },
                )
                .await
                .expect("consume tail strip");
            cursor = planned_cursor;
            let confirmed = harness
                .handler
                .mutate_strip_reservation(
                    &chunk_id,
                    &group_id,
                    fence,
                    ReservationUpdate {
                        strip_sequence: strip.strip_sequence,
                        action: StripReservationAction::Confirm,
                        acknowledged_cursor: cursor,
                        closed_strip_sequence: Some(strip.strip_sequence),
                    },
                )
                .await
                .expect("confirm tail strip");
            fence.expected_modify_ts = confirmed.chunk.modify_ts;
        }
        let sealed = harness
            .handler
            .seal_chunk(&chunk_id, u32::try_from(cursor / 1024).unwrap())
            .await
            .expect("seal early conversion tail");
        assert_eq!(sealed.strips.len(), tail as usize);
        assert!(sealed
            .strips
            .iter()
            .all(|strip| matches!(strip.strip, Some(Strip::MirrorStrip(_)))));
    }
}

#[tokio::test]
async fn chunkdb_lock_serializes_concurrent_seal() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk (Active).
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Two concurrent seals on the same chunk — the lock serializes
    // them: exactly one succeeds (Sealed), the other sees Sealed
    // state and fails with InvalidStateTransition.
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let id1 = chunk_id;
    let id2 = chunk_id;
    let t1 = tokio::spawn(async move { h1.seal_chunk(&id1, 100).await });
    let t2 = tokio::spawn(async move { h2.seal_chunk(&id2, 200).await });
    let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (t1.await.expect("task 1"), t2.await.expect("task 2"))
    })
    .await
    .expect("no deadlock — both seals completed within 10s");

    let ok_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    assert_eq!(ok_count, 1, "exactly one seal should succeed, got {ok_count}");
    let sealed = if let Ok(c) = &r1 { c } else { r2.as_ref().unwrap() };
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    eprintln!("concurrent seal serialized: one Ok, one Err (InvalidStateTransition)");
}

#[tokio::test]
async fn chunkdb_lock_serializes_concurrent_delete() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk (Active).
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Two concurrent deletes on the same chunk — the lock serializes
    // them and both return the same idempotent Deleted result.
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let id1 = chunk_id;
    let id2 = chunk_id;
    let t1 = tokio::spawn(async move { h1.delete_chunk(&id1).await });
    let t2 = tokio::spawn(async move { h2.delete_chunk(&id2).await });
    let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (t1.await.expect("task 1"), t2.await.expect("task 2"))
    })
    .await
    .expect("no deadlock — both deletes completed within 10s");

    assert_eq!(r1.expect("first delete").state, ChunkState::Deleted as i32);
    assert_eq!(r2.expect("second delete").state, ChunkState::Deleted as i32);
    eprintln!("concurrent delete serialized: both return Deleted");
}

#[tokio::test]
async fn chunkdb_lock_serializes_concurrent_append_delete() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk (Active).
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 0, 0)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Concurrent append + delete on the same chunk — the lock
    // serializes them. Delete always wins (Active → Deleted). If
    // delete runs first, append sees Deleted → InvalidStateTransition.
    // If append runs first, append succeeds then delete → Deleted.
    // Invariant: delete always succeeds; final state is Deleted.
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let id1 = chunk_id;
    let id2 = chunk_id;
    let t1 = tokio::spawn(async move { h1.append_chunk(&id1, 1, 1, StripType::Mirror, 0, 0, 3, 1).await });
    let t2 = tokio::spawn(async move { h2.delete_chunk(&id2).await });
    let (append_res, delete_res) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (t1.await.expect("append task"), t2.await.expect("delete task"))
    })
    .await
    .expect("no deadlock — both completed within 10s");

    let deleted = delete_res.expect("delete always succeeds on Active chunk");
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    // Append may succeed (if it ran first) or fail with
    // InvalidStateTransition (if delete ran first) — both are valid.
    match &append_res {
        Ok(outcome) => {
            assert_eq!(outcome.modify_ts, 2);
            assert_eq!(outcome.strips.len(), 1);
            assert!(outcome.chunk.is_none());
        }
        Err(LifecycleError::InvalidStateTransition(_)) => {
            eprintln!("delete ran first → append rejected (InvalidStateTransition)");
        }
        Err(e) => panic!("append failed with unexpected error: {e:?}"),
    }
    // Final state from the store must be Deleted.
    let final_chunk = harness.handler.query_chunk(&chunk_id).await.expect("query final");
    assert_eq!(final_chunk.state, ChunkState::Deleted as i32);
    eprintln!("concurrent append+delete serialized: final state Deleted");
}

#[tokio::test]
async fn chunkdb_shared_writer_cursor_is_fenced_and_orphan_is_sealed() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let chunk = harness
        .handler
        .allocate_chunk(None, 1024, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo, 99, 20)
        .await
        .expect("allocate shared chunk");
    let chunk_id = chunk.id.expect("chunk id");
    let advanced = harness
        .handler
        .advance_chunk_write(&chunk_id, 99, chunk.modify_ts, 1024 * 1024, Some(0), 20)
        .await
        .expect("advance cursor");
    assert_eq!(advanced.acknowledged_cursor, 1024 * 1024);
    assert_eq!(advanced.closed_strip_sequence, Some(0));
    let renewed = harness
        .handler
        .advance_chunk_write(
            &chunk_id,
            99,
            advanced.modify_ts,
            advanced.acknowledged_cursor,
            None,
            20,
        )
        .await
        .expect("renew liveness without advancing cursor");
    assert_eq!(renewed.modify_ts, advanced.modify_ts);
    assert_eq!(renewed.acknowledged_cursor, advanced.acknowledged_cursor);
    assert!(matches!(
        harness
            .handler
            .advance_chunk_write(
                &chunk_id,
                100,
                advanced.modify_ts,
                1024 * 1024 + 4096,
                Some(0),
                20
            )
            .await,
        Err(LifecycleError::StateConflict)
    ));
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(harness.handler.seal_expired_writer_chunks().await.unwrap(), 1);
    let sealed = harness.handler.query_chunk(&chunk_id).await.unwrap();
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    assert_eq!(sealed.sealed_length, 1024);
    assert_eq!(sealed.acknowledged_cursor, 1024 * 1024);
    assert_eq!(sealed.closed_strip_sequence, Some(0));
}
