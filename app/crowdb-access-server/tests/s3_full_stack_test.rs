// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![cfg(feature = "s3-e2e")]

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_chunkdb_client::ChunkdbClientError;
use crowdb_diskio_client::{DiskId as DiskIoDiskId, TestWireDiskioClient};
use crowdb_kv_client::HardwareClient;
use crowdb_protocol::chunkdb::rpc::DeleteChunkRangeRequest;
use crowdb_protocol::common::{ChunkId, DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use crowdb_rpc_ffi::RpcServer;
use crowdb_test_harness::chunk_kv::ChunkKvProcess;
use crowdb_test_harness::chunkdb::{
    make_client as make_chunkdb_client, ChunkdbPlacementMode, ChunkdbProcess, ChunkdbStartOptions,
};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskdb::DiskdbProcess;
use crowdb_test_harness::diskio::{DiskioGroup0Identity, DiskioProcess, DiskioStartOpts};

const MASTER_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";

struct AccessServerProcess {
    child: Child,
    log_path: PathBuf,
}

impl AccessServerProcess {
    fn log_content(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

impl Drop for AccessServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boto3_runs_against_a_self_hosted_complete_storage_stack() {
    let access_binary = binary("crowdb-access-server", "CROWDB_ACCESS_SERVER_BIN");
    let cluster = KvCluster::start().await;
    let identities = seed_compact_hardware(&cluster.make_hardware_client()).await;

    let diskdb = DiskdbProcess::start_for_instance(&cluster.mgmt_endpoints, 999, Some(1_536));
    diskdb.wait_for_ready().await;
    let rpc = Arc::new(RpcServer::new(None));
    rpc.listen("127.0.0.1", 0)
        .expect("listen for diskio readiness client");
    rpc.start();
    std::thread::sleep(Duration::from_millis(50));
    let mut diskios = Vec::new();
    for (index, identity) in identities.into_iter().enumerate() {
        let diskio = DiskioProcess::start_for_group(
            &DiskioStartOpts {
                dummy_disk: "mem",
                kv_seeds: &cluster.mgmt_endpoints,
                disks: &[],
                fault_error_rate: 0.0,
                fault_latency_ms: None,
                no_o_direct: false,
            },
            identity,
        );
        let connection = rpc
            .connect("127.0.0.1", diskio.port)
            .expect("connect diskio readiness client");
        let diskio_client = TestWireDiskioClient::new();
        diskio_client.attach(&connection);
        diskio
            .wait_for_disk(
                &diskio_client,
                &rpc,
                &connection,
                DiskIoDiskId::new(0, 1 + index as u64),
            )
            .await;
        diskios.push(diskio);
    }

    let chunkdb = ChunkdbProcess::start_with_options(
        &cluster.mgmt_endpoints,
        ChunkdbStartOptions {
            placement_mode: ChunkdbPlacementMode::UnsafeColocated,
            repair_allow_unsafe_placement: true,
            ..ChunkdbStartOptions::default()
        },
    );
    chunkdb.wait_for_ready().await;
    let chunkdb_client = make_chunkdb_client(cluster.make_service_registry_client());
    let range_delete = chunkdb_client
        .delete_chunk_range(DeleteChunkRangeRequest {
            chunk_id: Some(ChunkId { high: 1, low: 1 }),
            chunk_offset: 0,
            chunk_size: 1,
        })
        .await;
    assert!(matches!(range_delete, Err(ChunkdbClientError::Unimplemented(_))));
    let mut chunk_kv = ChunkKvProcess::start(&cluster.mgmt_endpoints);
    chunk_kv.wait_for_ready().await;

    let seeds = cluster.mgmt_endpoints.join(",");
    let issued = Command::new(&access_binary)
        .args(["issue-user", "boto3-e2e"])
        .env("CROWDB_MANAGEMENT_SEEDS", &seeds)
        .env("CROWDB_S3_MASTER_KEY", MASTER_KEY)
        .output()
        .expect("run S3 user-token issuer");
    assert!(
        issued.status.success(),
        "token issuer failed:\n{}",
        String::from_utf8_lossy(&issued.stderr)
    );
    let issued = String::from_utf8(issued.stdout).expect("token issuer output is UTF-8");
    let access_key = output_value(&issued, "AWS_ACCESS_KEY_ID");
    let secret_key = output_value(&issued, "AWS_SECRET_ACCESS_KEY");

    let port = reserve_ephemeral_port();
    let listen = format!("127.0.0.1:{port}");
    let log_path = crowdb_test_harness::test_dirs::test_log_dir()
        .join(format!("crowdb-access-s3-e2e-{}.log", std::process::id()));
    let log = std::fs::File::create(&log_path).expect("create access-server log");
    let log_error = log.try_clone().expect("clone access-server log");
    let child = Command::new(&access_binary)
        .env("CROWDB_S3_LISTEN", &listen)
        .env("CROWDB_MANAGEMENT_SEEDS", &seeds)
        .env("CROWDB_S3_TENANT", "boto3-e2e")
        .env("CROWDB_S3_MASTER_KEY", MASTER_KEY)
        .env("CROWDB_S3_REGION", "us-east-1")
        .env("CROWDB_S3_SMALL_OBJECT_LIMIT", "0")
        .env("CROWDB_S3_EC_DATA", "2")
        .env("CROWDB_S3_EC_CODE", "1")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_error))
        .spawn()
        .expect("start crowdb-access-server");
    let mut access_server = AccessServerProcess { child, log_path };
    wait_for_tcp(&mut access_server, &listen).await;

    let endpoint = format!("http://{listen}");
    let python_binary = std::env::var_os("CROWDB_S3_E2E_PYTHON").unwrap_or_else(|| "python".into());
    let python = Command::new(python_binary)
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/s3_e2e/basic.py"))
        .env("CROWDB_S3_E2E_ENDPOINT", endpoint)
        .env("CROWDB_S3_E2E_REGION", "us-east-1")
        .env("CROWDB_S3_E2E_ACCESS_KEY", access_key)
        .env("CROWDB_S3_E2E_SECRET_KEY", secret_key)
        .output()
        .expect("run boto3 compatibility suite");
    assert!(
        python.status.success(),
        "boto3 suite failed:\nstdout:\n{}\nstderr:\n{}\naccess server:\n{}\nchunk-kv:\n{}",
        String::from_utf8_lossy(&python.stdout),
        String::from_utf8_lossy(&python.stderr),
        access_server.log_content(),
        chunk_kv.log_content(),
    );

    drop(access_server);
    drop(chunk_kv);
    drop(chunkdb);
    drop(diskios);
    drop(diskdb);
    rpc.stop();
}

async fn seed_compact_hardware(hardware: &HardwareClient) -> Vec<DiskioGroup0Identity> {
    const RACK_ID: u64 = 1;
    const UNIT_BYTES: u32 = 1024 * 1024;
    const ZONE_UNITS: u64 = 1_536;
    let node_ids = vec![10];
    hardware
        .add_rack(
            RACK_ID,
            &RackValue {
                status: HwStatus::Up as i32,
                node_ids: node_ids.clone(),
            },
        )
        .await
        .expect("add compact rack");
    let lease_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
        + 3_600_000;
    let mut identities = Vec::new();
    for (index, node_id) in node_ids.into_iter().enumerate() {
        let index = index as u64;
        let disk_group_id = 100 + index;
        let instance_id = 999 + index;
        let disk_id = DiskId {
            high: 0,
            low: 1 + index,
        };
        hardware
            .add_node(
                RACK_ID,
                node_id,
                &NodeValue {
                    status: HwStatus::Up as i32,
                    last_used_dg_id: disk_group_id,
                    disk_group_ids: vec![disk_group_id],
                    status_changed_at_ms: 0,
                    temp_failure_since_ms: None,
                },
            )
            .await
            .expect("add compact node");
        hardware
            .add_disk_group(
                RACK_ID,
                node_id,
                disk_group_id,
                &DiskGroupValue {
                    status: HwStatus::Up as i32,
                    disk_ids: vec![disk_id],
                },
            )
            .await
            .expect("add compact disk group");
        hardware
            .add_disk(
                RACK_ID,
                node_id,
                disk_group_id,
                &disk_id,
                &DiskValue {
                    disk_type: DiskType::BlockSsd as i32,
                    capacity_units: ZONE_UNITS,
                    zone_size_units: ZONE_UNITS,
                    unit_size_bytes: UNIT_BYTES,
                    zone_count: 1,
                    status: HwStatus::Up as i32,
                    device_path: String::new(),
                },
            )
            .await
            .expect("add compact disk");
        hardware
            .set_owner(RACK_ID, node_id, disk_group_id, instance_id, lease_ms)
            .await
            .expect("set compact disk owner");
        hardware
            .set_bind(RACK_ID, node_id, disk_group_id, 0, 1)
            .await
            .expect("bind compact disk group");
        identities.push(DiskioGroup0Identity {
            instance_id,
            rack_id: RACK_ID,
            node_id,
            disk_group_id,
        });
    }
    identities
}

fn binary(name: &str, environment: &str) -> PathBuf {
    if let Some(path) = std::env::var_os(environment).map(PathBuf::from) {
        assert!(path.exists(), "{environment} does not exist: {}", path.display());
        return path;
    }
    let executable = std::env::current_exe().expect("current test executable");
    let debug = executable
        .parent()
        .and_then(Path::parent)
        .expect("target debug directory");
    let candidate = debug.join(name);
    assert!(
        candidate.exists(),
        "required binary does not exist: {}",
        candidate.display()
    );
    candidate
}

fn output_value<'a>(output: &'a str, name: &str) -> &'a str {
    output
        .lines()
        .find_map(|line| line.strip_prefix(name).and_then(|value| value.strip_prefix('=')))
        .unwrap_or_else(|| panic!("issuer omitted {name}: {output}"))
}

fn reserve_ephemeral_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve access-server port")
        .local_addr()
        .expect("reserved address")
        .port()
}

async fn wait_for_tcp(process: &mut AccessServerProcess, address: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if TcpStream::connect(address).is_ok() {
            return;
        }
        assert!(
            process
                .child
                .try_wait()
                .expect("query access-server status")
                .is_none(),
            "crowdb-access-server exited before readiness:\n{}",
            process.log_content()
        );
        assert!(
            Instant::now() <= deadline,
            "crowdb-access-server did not listen within 60s:\n{}",
            process.log_content()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
