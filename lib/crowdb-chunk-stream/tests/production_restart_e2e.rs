// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, ChunkReadPolicy, SmallWritePolicy};
use crowdb_chunk_stream::{
    ProductionStreamRuntime, StreamBinding, StreamBindingState, StreamConfig, StreamName, StreamRegistry,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient};
use crowdb_protocol::common::{DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use crowdb_test_harness::chunkdb::{self as chunkdb_harness, ChunkdbProcess, ChunkdbStartOptions};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskdb::{self as diskdb_harness, DiskdbProcess};
use crowdb_test_harness::diskio::{self as diskio_harness, DiskArg, DiskioProcess, DiskioStartOpts};
use crowdb_test_harness::hardware::INSTANCE_ID;
use crowdb_test_harness::test_dirs::TestDir;

const TEST_UNIT_BYTES: u32 = 1024 * 1024;
const TEST_ZONE_SIZE_UNITS: u64 = 16 * 1024;
const TEST_CAPACITY_UNITS: u64 = TEST_ZONE_SIZE_UNITS;

fn all_binaries_available() -> bool {
    let available = (std::env::var("CROWDB_KV_SERVER_BIN").is_ok()
        || crowdb_test_harness::cluster::crowdb_kv_server_bin().is_some())
        && diskdb_harness::crowdb_diskdb_bin().is_some()
        && diskio_harness::crowdb_diskio_bin().is_some()
        && chunkdb_harness::crowdb_chunkdb_bin().is_some();
    if !available {
        eprintln!("skipping real-process stream restart test: required binaries are unavailable");
    }
    available
}

fn create_disks(root: &TestDir) -> Vec<DiskArg> {
    let capacity = TEST_CAPACITY_UNITS * u64::from(TEST_UNIT_BYTES);
    (1..=3)
        .map(|id| {
            let path = root.path().join(format!("disk-{id}.dat"));
            let file = std::fs::File::create(&path).expect("create block-disk file");
            file.set_len(capacity).expect("size block-disk file");
            DiskArg {
                id_high: 0,
                id_low: id,
                path: path.to_string_lossy().into_owned(),
                zone_capacity: i64::try_from(capacity).expect("test disk capacity fits i64"),
            }
        })
        .collect()
}

fn start_diskio(disks: &[DiskArg]) -> DiskioProcess {
    DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "null",
        kv_seeds: &[],
        disks,
        fault_error_rate: 0.0,
        no_o_direct: true,
    })
}

async fn register_diskio(cluster: &KvCluster, diskio: &DiskioProcess) {
    cluster
        .make_service_registry_client()
        .heartbeat_diskio(
            INSTANCE_ID,
            &format!("127.0.0.1:{}", diskio.port),
            &[100, 101, 102],
            &[],
        )
        .await
        .expect("register block-backed diskio");
}

async fn seed_restart_hardware(hardware: &HardwareClient) {
    let lease_deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
        + 3_600_000;
    for index in 0..3_u64 {
        let rack_id = index + 1;
        let node_id = index + 10;
        let disk_group_id = index + 100;
        let disk_id = DiskId {
            high: 0,
            low: index + 1,
        };
        hardware
            .add_rack(
                rack_id,
                &RackValue {
                    status: HwStatus::Up as i32,
                    node_ids: vec![node_id],
                },
            )
            .await
            .expect("add rack");
        hardware
            .add_node(
                rack_id,
                node_id,
                &NodeValue {
                    status: HwStatus::Up as i32,
                    last_used_dg_id: 0,
                    disk_group_ids: vec![disk_group_id],
                    status_changed_at_ms: 0,
                    temp_failure_since_ms: None,
                },
            )
            .await
            .expect("add node");
        hardware
            .add_disk_group(
                rack_id,
                node_id,
                disk_group_id,
                &DiskGroupValue {
                    status: HwStatus::Up as i32,
                    disk_ids: vec![disk_id],
                },
            )
            .await
            .expect("add disk group");
        hardware
            .add_disk(
                rack_id,
                node_id,
                disk_group_id,
                &disk_id,
                &DiskValue {
                    disk_type: DiskType::BlockSsd as i32,
                    capacity_units: TEST_CAPACITY_UNITS,
                    zone_size_units: TEST_ZONE_SIZE_UNITS,
                    unit_size_bytes: TEST_UNIT_BYTES,
                    zone_count: 1,
                    status: HwStatus::Up as i32,
                    device_path: String::new(),
                },
            )
            .await
            .expect("add disk");
        hardware
            .set_owner(rack_id, node_id, disk_group_id, INSTANCE_ID, lease_deadline)
            .await
            .expect("set disk group owner");
        hardware
            .set_bind(rack_id, node_id, disk_group_id, 0, 1)
            .await
            .expect("bind disk group");
    }
}

async fn connect_chunk_io(cluster: &KvCluster) -> ChunkIoClient {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match ChunkIoClient::connect(ChunkIoClientConfig {
            management_seeds: cluster.mgmt_endpoints.clone(),
            diskio_connections_per_endpoint: 2,
            diskio_rpc_workers: 1,
            small_write: SmallWritePolicy::default(),
        })
        .await
        {
            Ok(client) => return client,
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for chunk services: {error}");
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(error) => panic!("chunk services unavailable after restart: {error}"),
        }
    }
}

fn start_chunkdb(cluster: &KvCluster) -> ChunkdbProcess {
    ChunkdbProcess::start_with_options(
        &cluster.mgmt_endpoints,
        ChunkdbStartOptions {
            allow_unsafe_ec: true,
            repair_allow_unsafe_placement: true,
            ..ChunkdbStartOptions::default()
        },
    )
}

fn kv_client(cluster: &KvCluster) -> Arc<CrowdbKvClient> {
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
        cluster.mgmt_endpoints.clone(),
    )));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    kv.seed_leader(0, 1, cluster.group1_leader_endpoint.clone());
    kv
}

#[tokio::test]
async fn production_stream_recovers_exact_bytes_after_service_restarts() {
    if !all_binaries_available() {
        return;
    }

    let disk_root = TestDir::new("chunk-stream-restart").expect("create test disk root");
    let disks = create_disks(&disk_root);
    let mut cluster = KvCluster::start().await;
    seed_restart_hardware(&cluster.make_hardware_client()).await;
    let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, false);
    diskdb.wait_for_ready().await;
    let mut diskio = start_diskio(&disks);
    register_diskio(&cluster, &diskio).await;
    let mut chunkdb = start_chunkdb(&cluster);
    chunkdb.wait_for_ready().await;

    let first_io = connect_chunk_io(&cluster).await;
    let first_runtime = ProductionStreamRuntime::new(
        kv_client(&cluster),
        &first_io,
        30_000,
        ChunkReadPolicy::default(),
        StreamConfig::default(),
    )
    .expect("assemble initial runtime");
    let stream_name = StreamName {
        high: u64::from(std::process::id()),
        low: 141,
    };
    first_runtime
        .registry()
        .create(StreamBinding {
            stream_name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("restart-e2e".into()),
        })
        .await
        .expect("publish stream binding");
    let first_stream = first_runtime
        .create_registered(stream_name, 0, 1)
        .await
        .expect("create stream");
    let first = first_stream
        .append(&[Bytes::from_static(b"before-restart|")])
        .await
        .expect("append before restart");
    let abandoned_chunk = first.chunk_id.expect("first append chunk identity");
    assert_eq!(
        first_stream.read_at(0, 15).await.expect("read before restart"),
        Bytes::from_static(b"before-restart|")
    );
    drop(first_stream);
    drop(first_runtime);
    drop(first_io);

    chunkdb.crash();
    diskio.child.kill().expect("kill diskio");
    diskio.child.wait().expect("reap diskio");
    cluster.crash_and_restart().await;

    diskio = start_diskio(&disks);
    register_diskio(&cluster, &diskio).await;
    chunkdb = start_chunkdb(&cluster);
    chunkdb.wait_for_ready().await;
    let second_io = connect_chunk_io(&cluster).await;
    let second_runtime = ProductionStreamRuntime::new(
        kv_client(&cluster),
        &second_io,
        30_000,
        ChunkReadPolicy::default(),
        StreamConfig::default(),
    )
    .expect("assemble recovered runtime");
    let second_stream = second_runtime
        .open(stream_name, 0, 2)
        .await
        .expect("recover authoritative stream head");
    let second = second_stream
        .append(&[Bytes::from_static(b"after-restart")])
        .await
        .expect("append after restart");
    assert_ne!(
        second.chunk_id,
        Some(abandoned_chunk),
        "takeover must isolate the abandoned writer chunk"
    );
    assert_eq!(second.begin, 15);
    assert_eq!(
        second_stream.read_at(0, 28).await.expect("read recovered stream"),
        Bytes::from_static(b"before-restart|after-restart")
    );
}
