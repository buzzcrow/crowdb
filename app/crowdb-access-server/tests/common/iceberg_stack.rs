use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_access_iceberg::catalog::RoutedCatalogStore;
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_diskio_client::{DiskId as DiskIoDiskId, TestWireDiskioClient};
use crowdb_protocol::common::{DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use crowdb_rpc_ffi::RpcServer;
use crowdb_test_harness::chunk_kv::ChunkKvProcess;
use crowdb_test_harness::chunkdb::{ChunkdbPlacementMode, ChunkdbProcess, ChunkdbStartOptions};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskdb::DiskdbProcess;
use crowdb_test_harness::diskio::{DiskArg, DiskioGroup0Identity, DiskioProcess, DiskioStartOpts};

pub struct TestIcebergStack {
    pub chunk_kv: ChunkKvProcess,
    _chunkdb: ChunkdbProcess,
    _diskio: DiskioProcess,
    _diskdb: DiskdbProcess,
    _rpc: Arc<RpcServer>,
    pub cluster: KvCluster,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

impl TestIcebergStack {
    pub async fn start() -> Self {
        let mut cluster = KvCluster::start().await;
        seed(&cluster).await;
        let identity = DiskioGroup0Identity {
            instance_id: 999,
            rack_id: 1,
            node_id: 10,
            disk_group_id: 100,
        };
        let path = cluster
            .runtime_mut()
            .service_dir("diskio", "iceberg")
            .unwrap()
            .join("data/disk.dat");
        let capacity = 16_384_u64 * 1024 * 1024;
        std::fs::File::create(&path).unwrap().set_len(capacity).unwrap();
        let disk = DiskArg {
            id_high: 0,
            id_low: 1,
            path: path.to_string_lossy().into_owned(),
            zone_capacity: capacity.try_into().unwrap(),
        };
        let seeds = cluster.mgmt_endpoints.clone();
        let started = now_ms();
        let diskdb = DiskdbProcess::start_for_instance_in(cluster.runtime_mut(), &seeds, 999, Some(16_384));
        diskdb.wait_for_ready().await;
        diskdb
            .wait_for_registry_ready(&cluster.make_service_registry_client(), 100, started)
            .await;
        let rpc = Arc::new(RpcServer::new(None));
        rpc.listen("127.0.0.1", 0).unwrap();
        rpc.start();
        let diskio = DiskioProcess::start_for_group_in(
            cluster.runtime_mut(),
            &DiskioStartOpts {
                dummy_disk: "null",
                kv_seeds: &seeds,
                disks: &[disk],
                fault_error_rate: 0.0,
                fault_latency_ms: None,
                no_o_direct: true,
            },
            identity,
        );
        let connection = rpc.connect("127.0.0.1", diskio.port).unwrap();
        let client = TestWireDiskioClient::new();
        client.attach(&connection);
        diskio
            .wait_for_disk(&client, &rpc, &connection, DiskIoDiskId::new(0, 1))
            .await;
        cluster
            .make_service_registry_client()
            .heartbeat_diskio_at(999, &format!("127.0.0.1:{}", diskio.port), 1, 10, &[100], &[])
            .await
            .unwrap();
        let chunkdb = ChunkdbProcess::start_with_options_in(
            cluster.runtime_mut(),
            &seeds,
            ChunkdbStartOptions {
                placement_mode: ChunkdbPlacementMode::UnsafeColocated,
                repair_allow_unsafe_placement: true,
                ..ChunkdbStartOptions::default()
            },
        );
        chunkdb.wait_for_ready().await;
        chunkdb
            .wait_for_registry_ready(&cluster.make_service_registry_client())
            .await;
        let mut chunk_kv = ChunkKvProcess::start_in(cluster.runtime_mut(), &seeds);
        chunk_kv.wait_for_ready().await;
        Self {
            chunk_kv,
            _chunkdb: chunkdb,
            _diskio: diskio,
            _diskdb: diskdb,
            _rpc: rpc,
            cluster,
        }
    }

    pub async fn store(&self) -> Arc<RoutedCatalogStore> {
        let config = ClientConfig::default();
        let source = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(Arc::new(
            crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(
                self.cluster.mgmt_endpoints.clone(),
            )),
        )));
        let transport = Arc::new(ChunkKvRpcTransport::new(config.max_owner_connections, 1, 2));
        let client = Arc::new(ChunkKvClient::new(config, source, transport).unwrap());
        client.refresh_catalog().await.unwrap();
        Arc::new(RoutedCatalogStore::new(client))
    }
}

async fn seed(cluster: &KvCluster) {
    let hardware = cluster.make_hardware_client();
    hardware
        .add_rack(
            1,
            &RackValue {
                status: HwStatus::Up as i32,
                node_ids: vec![10],
            },
        )
        .await
        .unwrap();
    hardware
        .add_node(
            1,
            10,
            &NodeValue {
                status: HwStatus::Up as i32,
                last_used_dg_id: 100,
                disk_group_ids: vec![100],
                status_changed_at_ms: 0,
                temp_failure_since_ms: None,
            },
        )
        .await
        .unwrap();
    let disk = DiskId { high: 0, low: 1 };
    hardware
        .add_disk_group(
            1,
            10,
            100,
            &DiskGroupValue {
                status: HwStatus::Up as i32,
                disk_ids: vec![disk],
            },
        )
        .await
        .unwrap();
    hardware
        .add_disk(
            1,
            10,
            100,
            &disk,
            &DiskValue {
                disk_type: DiskType::BlockSsd as i32,
                capacity_units: 16_384,
                zone_size_units: 16_384,
                unit_size_bytes: 1024 * 1024,
                zone_count: 1,
                status: HwStatus::Up as i32,
                device_path: String::new(),
            },
        )
        .await
        .unwrap();
    hardware
        .set_owner(1, 10, 100, 999, now_ms() + 3_600_000)
        .await
        .unwrap();
    hardware.set_bind(1, 10, 100, 0, 1).await.unwrap();
}
