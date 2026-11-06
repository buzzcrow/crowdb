// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Metadata and internal service fixtures for Chunkdb component proofs.
//!
//! Direct handler tests select pinned in-memory metadata.
//! Internal Diskdb service wiring supplies component allocation dependencies;
//! public packaged-server flows belong to the container acceptance crate.

#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_chunkdb::allocator::{ChunkAllocator, DiskdbClientPool};
use crowdb_chunkdb::lifecycle::LifecycleHandler;
use crowdb_chunkdb::relocation::RelocationCoordinator;
use crowdb_chunkdb::routing::{default_binding_table, BindingCache};
use crowdb_chunkdb::storage::ChunkStore;
use crowdb_chunkdb::topology::{refresh::run_refresh_loop, TopologyCache};
use crowdb_common::metrics::MetricsRegistry;
use crowdb_diskdb::ddb_config::{DdbConfig, KeepAliveConfig, StorageDefaults};
use crowdb_diskdb::ddb_kv_client::DdbKvClient;
use crowdb_diskdb::liveness::keepalive::KeepAlive;
use crowdb_diskdb::liveness::lifecycle::StartupPhase;
use crowdb_diskdb::metrics::DiskdbMetrics;
use crowdb_diskdb::metrics::RecalcEngine;
use crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer;
use crowdb_diskdb::rebalance::{RelocationIo, RelocationOwner, RelocationWorker};
use crowdb_diskdb::recovery::ZoneLoader;
use crowdb_diskdb::scanner::ScanState;
use crowdb_diskdb::service::DiskdbRpcService;
use crowdb_diskdb_client::DiskdbRpcTransport;
use crowdb_paxos_kv::cluster::PaxosKvServer;
use crowdb_paxos_kv_client::{ClientConfig, HardwareClient, PaxosKvClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{RelocateSegmentHandoffRequest, RelocationHandoffDisposition};
use crowdb_protocol::common::{DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue, Segment};

pub struct KvCluster {
    memory_store: Arc<crowdb_paxos_kv::cluster::PxKvStore>,
    endpoint: String,
}

impl KvCluster {
    /// Pinned memory dependency for component handler tests.
    pub async fn start_pinned() -> Self {
        use crowdb_paxos_kv::cluster::group::PxGroup;
        use crowdb_paxos_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        for id in [0, 1] {
            store.add_group(PxGroup::new(
                id,
                PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
            ));
        }
        store.start().await.expect("start pinned memory dependency");
        let endpoint = store.listen_addr().unwrap().to_string();
        Self {
            memory_store: store,
            endpoint,
        }
    }

    pub fn make_crowdb_client(&self) -> Arc<PaxosKvClient> {
        let kv = PaxosKvClient::new(ClientConfig::new(vec![]));
        kv.seed_leader(0, 0, self.endpoint.clone());
        kv.seed_leader(0, 1, self.endpoint.clone());
        Arc::new(kv)
    }

    pub fn make_hardware_client(&self) -> HardwareClient {
        HardwareClient::from_shared(self.make_crowdb_client())
    }

    pub fn make_service_registry_client(&self) -> ServiceRegistryClient {
        ServiceRegistryClient::from_shared(self.make_crowdb_client())
    }

    pub fn make_ddb_kv_client(&self) -> Arc<DdbKvClient> {
        Arc::new(DdbKvClient::from_shared(self.make_crowdb_client()))
    }
}

impl Drop for KvCluster {
    fn drop(&mut self) {
        self.memory_store.stop();
    }
}

// ── hardware seeding ────────────────────────────────────────────

pub const STORE_ID: u64 = 0;
pub const DATA_GROUP_ID: u64 = 1;
pub const INSTANCE_ID: u64 = 999;
pub const ZONE_SIZE_UNITS: u64 = 128;
pub const UNIT_SIZE_BYTES: u32 = 1024 * 1024;
pub const ZONE_COUNT: u32 = 4;

pub fn make_disk_id(low: u64) -> DiskId {
    DiskId { high: 0, low }
}

/// Seed 4 racks × 1 node × 1 disk-group (3 disks each) — enough for
/// mirror 3-copy placement and a safe 8+4 EC strip on distinct disks.
pub async fn seed_hardware(hw: &HardwareClient) {
    let layout = [(100, vec![10]), (101, vec![11]), (102, vec![12]), (103, vec![13])];
    seed_hardware_layout(hw, &layout).await;
}

/// Seed an explicit rack/node layout and return its disk-group IDs.
pub async fn seed_hardware_layout(hw: &HardwareClient, layout: &[(u64, Vec<u64>)]) -> Vec<u64> {
    seed_hardware_layout_with_zones(hw, layout, ZONE_COUNT).await
}

/// Seed an explicit layout with enough zones for large-fragment test shapes.
pub async fn seed_hardware_layout_with_zones(
    hw: &HardwareClient,
    layout: &[(u64, Vec<u64>)],
    zone_count: u32,
) -> Vec<u64> {
    seed_hardware_layout_from_disk_group(hw, layout, zone_count, 1000).await
}

/// Seed an additional layout using disk-group IDs beginning at `first_dg_id`.
pub async fn seed_hardware_layout_from_disk_group(
    hw: &HardwareClient,
    layout: &[(u64, Vec<u64>)],
    zone_count: u32,
    first_dg_id: u64,
) -> Vec<u64> {
    let capacity_units = ZONE_SIZE_UNITS.saturating_mul(u64::from(zone_count));
    let lease_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
        + 3_600_000;

    let mut disk_groups = Vec::new();
    for (rack_id, node_ids) in layout {
        let rack_first_dg_id =
            first_dg_id.saturating_add(u64::try_from(disk_groups.len()).unwrap_or(u64::MAX));

        hw.add_rack(
            *rack_id,
            &RackValue {
                status: HwStatus::Up as i32,
                node_ids: node_ids.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("add rack");

        for (node_offset, node_id) in node_ids.iter().enumerate() {
            let dg_id = rack_first_dg_id.saturating_add(u64::try_from(node_offset).unwrap_or(u64::MAX));
            hw.add_node(
                *rack_id,
                *node_id,
                &NodeValue {
                    status: HwStatus::Up as i32,
                    last_used_dg_id: 0,
                    disk_group_ids: vec![dg_id],
                    status_changed_at_ms: 0,
                    temp_failure_since_ms: None,
                    ..Default::default()
                },
            )
            .await
            .expect("add node");

            let disk_ids = vec![
                make_disk_id(dg_id * 10 + 1),
                make_disk_id(dg_id * 10 + 2),
                make_disk_id(dg_id * 10 + 3),
            ];
            hw.add_disk_group(
                *rack_id,
                *node_id,
                dg_id,
                &DiskGroupValue {
                    status: HwStatus::Up as i32,
                    disk_ids: disk_ids.clone(),
                    name: String::new(),
                },
            )
            .await
            .expect("add disk-group");

            for did in &disk_ids {
                hw.add_disk(
                    *rack_id,
                    *node_id,
                    dg_id,
                    did,
                    &DiskValue {
                        disk_type: DiskType::BlockSsd as i32,
                        capacity_units,
                        zone_size_units: ZONE_SIZE_UNITS,
                        unit_size_bytes: UNIT_SIZE_BYTES,
                        zone_count,
                        status: HwStatus::Up as i32,
                        device_path: String::new(),
                    },
                )
                .await
                .expect("add disk");
            }

            hw.set_owner(*rack_id, *node_id, dg_id, INSTANCE_ID, lease_ms)
                .await
                .expect("set owner");
            hw.set_bind(*rack_id, *node_id, dg_id, STORE_ID, DATA_GROUP_ID)
                .await
                .expect("set bind");
            disk_groups.push(dg_id);
        }
    }
    disk_groups
}

/// Disk-group IDs seeded by `seed_hardware`.
pub fn seeded_dg_ids() -> Vec<u64> {
    (0..4u64).map(|i| 1000 + i).collect()
}

// ── diskdb crowdb-rpc server (in-process) ─────────────────────────────

#[allow(dead_code)]
pub struct DiskdbServer {
    pub container: Arc<DdbDiskGroupContainer>,
    pub rpc_endpoint: String,
    relocation_owner: Arc<HarnessRelocationOwner>,
    _serve_handle: Option<tokio::task::JoinHandle<()>>,
}

struct HarnessRelocationOwner {
    coordinator: arc_swap::ArcSwapOption<RelocationCoordinator>,
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

impl RelocationOwner for HarnessRelocationOwner {
    fn handoff<'a>(
        &'a self,
        request: RelocateSegmentHandoffRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RelocationHandoffDisposition, String>> + Send + 'a>> {
        Box::pin(async move {
            let coordinator = self
                .coordinator
                .load_full()
                .ok_or_else(|| "test relocation owner is not attached".to_string())?;
            coordinator
                .admit(&request, unix_time_ms())
                .await
                .map_err(|error| error.to_string())
        })
    }
}

struct HarnessRelocationIo;

impl RelocationIo for HarnessRelocationIo {
    fn copy_and_fsync<'a>(
        &'a self,
        _source: Segment,
        _target: Segment,
        _unit_size: u32,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

impl DiskdbServer {
    pub fn set_relocation_owner(&self, coordinator: Arc<RelocationCoordinator>) {
        self.relocation_owner.coordinator.store(Some(coordinator));
    }

    /// Start diskdb in-process: run one keepalive tick to populate
    /// state, wait for zones, then start the crowdb-rpc server on a free
    /// port and register in the service registry.
    pub async fn start(cluster: &KvCluster) -> Self {
        Self::start_with_disk_groups_and_zones(cluster, &seeded_dg_ids(), ZONE_COUNT).await
    }

    /// Start `DiskDB` for an explicit layout with its declared zone count.
    pub async fn start_with_disk_groups_and_zones(
        cluster: &KvCluster,
        disk_group_ids: &[u64],
        zone_count: u32,
    ) -> Self {
        crowdb_rpc_ffi::init_test_logging();
        let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
        let svc = cluster.make_service_registry_client();
        let hw = cluster.make_hardware_client();
        // Keepalive takes an owned DdbKvClient; the RPC service takes Arc.
        let dg_kv_owned = DdbKvClient::from_shared(cluster.make_crowdb_client());

        let keepalive_cfg = KeepAliveConfig {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: zone_count,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        };
        let keepalive =
            KeepAlive::new(hw, svc, Arc::clone(&container), keepalive_cfg).with_ddb_kv_client(dg_kv_owned);

        let outcome = keepalive.tick().await;
        eprintln!(
            "diskdb tick: groups_added={}, disks_added={}",
            outcome.groups_added, outcome.disks_added
        );
        assert_eq!(
            outcome.groups_added,
            disk_group_ids.len(),
            "expected every seeded disk-group"
        );
        assert_eq!(
            outcome.disks_added,
            disk_group_ids.len().saturating_mul(3),
            "expected three disks per seeded disk-group"
        );

        for dg_id in disk_group_ids {
            wait_for_disks_ready(&container, *dg_id, 3, zone_count).await;
        }

        let mut registry = MetricsRegistry::new();
        let metrics = Arc::new(DiskdbMetrics::register(&mut registry));
        let rt_handle = tokio::runtime::Handle::current();
        let relocation_owner = Arc::new(HarnessRelocationOwner {
            coordinator: arc_swap::ArcSwapOption::empty(),
        });
        let rpc_service = Arc::new(
            DiskdbRpcService::new(
                Arc::clone(&container),
                cluster.make_ddb_kv_client(),
                StorageDefaults::default(),
                Arc::new(ZoneLoader::new(cluster.make_ddb_kv_client(), 4)),
                Arc::new(RecalcEngine::new(
                    cluster.make_ddb_kv_client(),
                    Arc::clone(&container),
                )),
                ScanState::new(),
                metrics,
                Arc::new(arc_swap::ArcSwap::from_pointee(DdbConfig::default())),
                rt_handle,
            )
            .with_relocation_worker(Arc::new(RelocationWorker::new(
                relocation_owner.clone(),
                Arc::new(HarnessRelocationIo),
            ))),
        );
        let rpc_server = Arc::new(crowdb_rpc_ffi::RpcServer::new(None));
        rpc_server.listen("127.0.0.1", 0).expect("rpc server listen");
        let port = rpc_server.port();
        rpc_service.register_handlers(&rpc_server);
        rpc_server.start();

        container.set_lifecycle_phase(StartupPhase::Up);

        let rpc_endpoint = format!("http://127.0.0.1:{port}");

        // Register in service registry.
        let svc = cluster.make_service_registry_client();
        svc.register_diskdb(INSTANCE_ID, &rpc_endpoint, disk_group_ids, &[])
            .await
            .expect("register diskdb");

        // Probe the RPC server until it responds — the worker threads
        // spawned by start() may not have entered their event loops yet,
        // especially on slow CI runners. A fixed sleep is unreliable;
        // instead, make a lightweight get_disk_group_info call with
        // fresh transports (to avoid stale connection cache) until it
        // succeeds or the deadline expires.
        wait_for_rpc_ready(&rpc_endpoint, disk_group_ids[0]).await;

        Self {
            container,
            rpc_endpoint,
            relocation_owner,
            _serve_handle: None,
        }
    }

    /// Load newly assigned disk-groups into this running `DiskDB` instance and
    /// republish its complete ownership list.
    pub async fn refresh_disk_groups(
        &self,
        cluster: &KvCluster,
        new_disk_group_ids: &[u64],
        all_disk_group_ids: &[u64],
        zone_count: u32,
    ) {
        let keepalive = KeepAlive::new(
            cluster.make_hardware_client(),
            cluster.make_service_registry_client(),
            Arc::clone(&self.container),
            KeepAliveConfig {
                interval: Duration::from_secs(10),
                miss_threshold: 3,
                zone_rotate_count: zone_count,
                cas_retry_limit: 100,
                temp_failure_timeout_secs: 900,
            },
        )
        .with_ddb_kv_client(DdbKvClient::from_shared(cluster.make_crowdb_client()));
        let outcome = keepalive.tick().await;
        assert_eq!(outcome.groups_added, new_disk_group_ids.len());
        for disk_group_id in new_disk_group_ids {
            wait_for_disks_ready(&self.container, *disk_group_id, 3, zone_count).await;
        }
        cluster
            .make_service_registry_client()
            .register_diskdb(INSTANCE_ID, &self.rpc_endpoint, all_disk_group_ids, &[])
            .await
            .expect("refresh DiskDB ownership");
    }
}

/// Probe the diskdb RPC server until it responds. The worker threads
/// spawned by `RpcServer::start()` may not have entered their event
/// loops yet on slow runners. Each retry uses a fresh transport to
/// avoid stale connections from a timed-out attempt.
async fn wait_for_rpc_ready(rpc_endpoint: &str, dg_id: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let transport = DiskdbRpcTransport::new();
        match transport.get_disk_group_info(rpc_endpoint, dg_id).await {
            Ok(_) => return,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "diskdb RPC server not ready at {rpc_endpoint} within 10s: {e}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

pub async fn wait_for_disks_ready(
    container: &DdbDiskGroupContainer,
    dg_id: u64,
    expected_disks: usize,
    expected_zones: u32,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(dg) = container.get_disk_group(dg_id) {
            let disks = dg.disk_snapshot();
            let all_ready = disks.len() == expected_disks
                && disks.iter().all(|d| {
                    d.effective_status() == HwStatus::Up
                        && u32::try_from(d.zones.load().len()).unwrap_or(0) == expected_zones
                });
            if all_ready {
                return;
            }
        }
        assert!(Instant::now() <= deadline, "disks not ready after 10s");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until the topology cache contains the seeded healthy disk-groups.
async fn wait_for_topology_ready(topology: &TopologyCache, expected_disk_groups: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let snap = topology.snapshot();
        if snap.healthy_disk_groups().len() >= expected_disk_groups {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "topology not ready after 10s: racks={}, disk_groups={}, healthy_disk_groups={}",
            snap.rack_ids().len(),
            snap.disk_group_count(),
            snap.healthy_disk_groups().len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ── chunkdb handler ─────────────────────────────────────────────

#[allow(dead_code)]
pub struct ChunkdbHarness {
    pub handler: Arc<LifecycleHandler>,
    pub store: Arc<ChunkStore>,
    pub allocator: Arc<ChunkAllocator>,
    pub pool: Arc<DiskdbClientPool>,
    pub topology: TopologyCache,
    _refresh_handle: tokio::task::JoinHandle<()>,
}

impl ChunkdbHarness {
    /// Wire the chunkdb lifecycle handler against the running kv
    /// cluster + diskdb server.
    pub async fn start(cluster: &KvCluster) -> Self {
        Self::start_with_layout_validity(cluster, Duration::from_secs(30)).await
    }

    pub async fn start_with_layout_validity(cluster: &KvCluster, layout_validity: Duration) -> Self {
        Self::start_with_disk_group_count(cluster, layout_validity, seeded_dg_ids().len()).await
    }

    pub async fn start_with_disk_group_count(
        cluster: &KvCluster,
        layout_validity: Duration,
        expected_disk_groups: usize,
    ) -> Self {
        let kv = cluster.make_crowdb_client();

        // Topology cache + refresh loop.
        let topology = TopologyCache::new();
        let hw = cluster.make_hardware_client();
        let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let refresh_cache = topology.clone();
        let refresh_handle = tokio::spawn(async move {
            run_refresh_loop(refresh_cache, hw, Duration::from_secs(5), stop_rx).await;
        });

        wait_for_topology_ready(&topology, expected_disk_groups).await;

        // Binding cache — all buckets to store 0, group 1.
        let bindings = BindingCache::new();
        bindings
            .replace(default_binding_table(STORE_ID, DATA_GROUP_ID))
            .unwrap();
        let store = Arc::new(ChunkStore::new(Arc::clone(&kv), bindings));

        // Diskdb client pool.
        let svc = cluster.make_service_registry_client();
        let pool = Arc::new(DiskdbClientPool::new(svc));
        pool.refresh_endpoints().await.expect("refresh endpoints");
        let allocator = Arc::new(ChunkAllocator::new(Arc::clone(&pool)));

        let handler = Arc::new(
            LifecycleHandler::new(Arc::clone(&store), Arc::clone(&allocator), topology.clone())
                .with_layout_validity(layout_validity)
                .with_locks(Arc::new(crowdb_chunkdb::lifecycle::ChunkLockMap::new(
                    10_000,
                    Arc::new(crowdb_chunkdb::metrics::LifecycleMetrics::new()),
                    std::time::Duration::from_secs(60),
                ))),
        );

        Self {
            handler,
            store,
            allocator,
            pool,
            topology,
            _refresh_handle: refresh_handle,
        }
    }
}
