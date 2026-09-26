// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoClient, ChunkIoClientConfig, LargeWritePolicy, SmallWritePolicy,
};
use crowdb_chunkdb_client::ChunkdbClientError;
use crowdb_common::ec::EcScheme;
use crowdb_diskio_client::{DiskId as DiskIoDiskId, TestWireDiskioClient};
use crowdb_kv_client::HardwareClient;
use crowdb_protocol::chunkdb::rpc::DeleteChunkRangeRequest;
use crowdb_protocol::common::{ChunkId, DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use crowdb_protocol::ServicePort;
use crowdb_rpc_ffi::RpcServer;
use crowdb_test_harness::chunk_kv::ChunkKvProcess;
use crowdb_test_harness::chunkdb::{
    make_client as make_chunkdb_client, ChunkdbPlacementMode, ChunkdbProcess, ChunkdbStartOptions,
};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskdb::DiskdbProcess;
use crowdb_test_harness::diskio::{DiskArg, DiskioGroup0Identity, DiskioProcess, DiskioStartOpts};
use crowdb_test_harness::test_dirs::TestRuntime;
use hyper::body::Bytes;
use serde_json::json;

const MASTER_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const TEST_COUNT: usize = 17;
const BOTO3_CASES: &[&str] = &[
    "test_signed_raw_http_wire_contract",
    "test_independent_frontends_share_one_namespace",
    "test_slow_signed_upload_releases_native_buffers",
    "test_truncated_signed_upload_does_not_publish_and_releases_credit",
    "test_concurrent_overwrite_delete_and_get_are_portable",
    "test_slow_response_reader_keeps_full_object_consistent",
    "test_basic_bucket_object_matrix",
    "test_fragmentation_and_storage_boundaries",
];

struct AccessServerProcess {
    child: Child,
    log_path: PathBuf,
}

struct FullStackSetup {
    access_binary: PathBuf,
    cluster: KvCluster,
    identity: DiskioGroup0Identity,
    disk_arg: DiskArg,
    diskdb: DiskdbProcess,
    rpc: Arc<RpcServer>,
    diskio: Option<DiskioProcess>,
    chunkdb_options: ChunkdbStartOptions,
    chunkdb: Option<ChunkdbProcess>,
    chunk_kv: ChunkKvProcess,
    seeds: String,
    access_key: String,
    secret_key: String,
    access_server: Option<AccessServerProcess>,
    listen: String,
    second_access_server: AccessServerProcess,
    second_listen: String,
    restarted_access: Option<AccessServerProcess>,
}

struct Boto3CaseContext<'a> {
    listen: &'a str,
    second_listen: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
    access_server: &'a AccessServerProcess,
    chunk_kv: &'a ChunkKvProcess,
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

struct TestCase {
    passed: bool,
}

impl TestCase {
    fn start(name: &str) -> Self {
        print!("test {name} ... ");
        std::io::stdout().flush().expect("flush test name");
        Self { passed: false }
    }

    fn pass(mut self) {
        self.passed = true;
        println!("ok");
    }
}

impl Drop for TestCase {
    fn drop(&mut self) {
        if !self.passed {
            eprintln!("FAILED");
        }
    }
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build S3 E2E runtime")
        .block_on(run_suite());
}

async fn run_suite() {
    let mut stack = start_full_stack().await;
    println!("\nrunning {TEST_COUNT} tests");
    stack.run_boto3_cases();
    stack.run_restart_cases().await;
    stack.run_benchmarks().await;
    stack.cleanup();
    println!("\ntest result: ok. {TEST_COUNT} passed; 0 failed\n");
}

impl FullStackSetup {
    fn run_boto3_cases(&self) {
        let context = Boto3CaseContext {
            listen: &self.listen,
            second_listen: &self.second_listen,
            access_key: &self.access_key,
            secret_key: &self.secret_key,
            access_server: self.access_server.as_ref().expect("primary access server"),
            chunk_kv: &self.chunk_kv,
        };
        for method in BOTO3_CASES {
            let case = TestCase::start(&format!("boto3::{method}"));
            run_boto3_case(method, &context);
            case.pass();
        }
        let case = TestCase::start("boto3::lost_put_reply_is_idempotent");
        run_restart_phase("lost-reply", &self.listen, &self.access_key, &self.secret_key);
        assert_native_write_metrics(&self.listen);
        case.pass();
    }

    async fn run_restart_cases(&mut self) {
        run_restart_phase("prepare", &self.listen, &self.access_key, &self.secret_key);
        self.restart_group0().await;
        self.restart_access_server().await;
        self.restart_chunkdb().await;
        self.restart_diskdb().await;
        self.restart_diskio().await;
        self.restart_chunk_kv().await;
    }

    async fn restart_group0(&mut self) {
        let case = TestCase::start("restart::group0_recovers_objects");
        self.cluster.crash_and_restart().await;
        self.verify_restart("verify-after-group0-restart", &self.second_listen);
        case.pass();
    }

    async fn restart_access_server(&mut self) {
        let case = TestCase::start("restart::access_server_recovers_objects");
        drop(self.access_server.take().expect("primary access server"));
        self.verify_restart("verify", &self.second_listen);
        let (mut process, listen) = start_access_server(
            self.cluster.runtime_mut(),
            &self.access_binary,
            &self.seeds,
            "primary",
        );
        wait_for_tcp(&mut process, &listen).await;
        self.verify_restart("verify", &listen);
        self.restarted_access = Some(process);
        case.pass();
    }

    async fn restart_chunkdb(&mut self) {
        let case = TestCase::start("restart::chunkdb_recovers_objects");
        let previous = self.chunkdb.take().expect("chunkdb process");
        drop(previous);
        let seeds = self.cluster.mgmt_endpoints.clone();
        let chunkdb =
            ChunkdbProcess::start_with_options_in(self.cluster.runtime_mut(), &seeds, self.chunkdb_options);
        chunkdb.wait_for_ready().await;
        self.verify_restart("verify-after-chunkdb-restart", &self.second_listen);
        self.chunkdb = Some(chunkdb);
        case.pass();
    }

    async fn restart_diskdb(&mut self) {
        let case = TestCase::start("restart::diskdb_recovers_objects");
        verify_diskdb_restart(
            &mut self.diskdb,
            &self.cluster.mgmt_endpoints,
            self.identity.disk_group_id,
            &self.second_listen,
            &self.access_key,
            &self.secret_key,
        )
        .await;
        self.cluster
            .runtime_mut()
            .record_process(self.diskdb.child.id())
            .expect("record restarted DiskDB process");
        case.pass();
    }

    async fn restart_diskio(&mut self) {
        let case = TestCase::start("restart::diskio_recovers_objects");
        drop(self.diskio.take().expect("diskio process"));
        self.diskio =
            Some(start_durable_diskio(&mut self.cluster, &self.rpc, self.identity, &self.disk_arg).await);
        self.verify_restart("verify-after-diskio-restart", &self.second_listen);
        case.pass();
    }

    async fn restart_chunk_kv(&mut self) {
        let case = TestCase::start("restart::chunk_kv_recovers_objects");
        self.chunk_kv.restart_in(self.cluster.runtime_mut()).await;
        self.verify_restart("verify-after-chunk-kv-restart", &self.second_listen);
        case.pass();
    }

    async fn run_benchmarks(&self) {
        let case = TestCase::start("benchmark::direct_chunk_path");
        run_direct_chunk_benchmark(
            &self.cluster.mgmt_endpoints,
            &self.cluster.runtime().artifacts_dir(),
        )
        .await;
        case.pass();

        let case = TestCase::start("benchmark::s3_request_path");
        run_benchmark(
            &self.second_listen,
            self.second_access_server.child.id(),
            &self.access_key,
            &self.secret_key,
            &self.cluster.runtime().artifacts_dir(),
        );
        case.pass();
    }

    fn verify_restart(&self, phase: &str, listen: &str) {
        run_restart_phase(phase, listen, &self.access_key, &self.secret_key);
    }

    fn cleanup(mut self) {
        self.verify_restart("cleanup", &self.second_listen);
        drop(self.restarted_access.take());
        self.rpc.stop();
    }
}

async fn start_full_stack() -> FullStackSetup {
    let access_binary = binary("crowdb-access-server", "CROWDB_ACCESS_SERVER_BIN");
    let mut cluster = KvCluster::start().await;
    let identities = seed_compact_hardware(&cluster.make_hardware_client()).await;
    let identity = identities[0];
    let disk_path = cluster
        .runtime_mut()
        .service_dir("diskio", &format!("instance-{}", identity.instance_id))
        .expect("create durable DiskIO directory")
        .join("data")
        .join("disk.dat");
    let capacity = 16_384_u64 * 1024 * 1024;
    std::fs::File::create(&disk_path)
        .expect("create block disk")
        .set_len(capacity)
        .expect("size sparse block disk");
    let disk_arg = DiskArg {
        id_high: 0,
        id_low: 1,
        path: disk_path.to_string_lossy().into_owned(),
        zone_capacity: i64::try_from(capacity).expect("disk zone fits i64"),
    };

    let diskdb_started_at = unix_time_ms();
    let seeds = cluster.mgmt_endpoints.clone();
    let diskdb = DiskdbProcess::start_for_instance_in(cluster.runtime_mut(), &seeds, 999, Some(16_384));
    diskdb.wait_for_ready().await;
    diskdb
        .wait_for_registry_ready(
            &cluster.make_service_registry_client(),
            identity.disk_group_id,
            diskdb_started_at,
        )
        .await;
    let rpc = Arc::new(RpcServer::new(None));
    rpc.listen("127.0.0.1", 0)
        .expect("listen for diskio readiness client");
    rpc.start();
    std::thread::sleep(Duration::from_millis(50));
    let diskio = start_durable_diskio(&mut cluster, &rpc, identity, &disk_arg).await;

    let chunkdb_options = ChunkdbStartOptions {
        placement_mode: ChunkdbPlacementMode::UnsafeColocated,
        repair_allow_unsafe_placement: true,
        ..ChunkdbStartOptions::default()
    };
    let chunkdb = ChunkdbProcess::start_with_options_in(cluster.runtime_mut(), &seeds, chunkdb_options);
    chunkdb.wait_for_ready().await;
    chunkdb
        .wait_for_registry_ready(&cluster.make_service_registry_client())
        .await;
    assert_range_delete_contract(&cluster).await;
    let mut chunk_kv = ChunkKvProcess::start_in(cluster.runtime_mut(), &seeds);
    chunk_kv.wait_for_ready().await;

    let seeds = cluster.mgmt_endpoints.join(",");
    let (access_key, secret_key) = issue_credentials(&access_binary, &seeds);
    let (mut access_server, listen) =
        start_access_server(cluster.runtime_mut(), &access_binary, &seeds, "primary");
    wait_for_tcp(&mut access_server, &listen).await;
    let (mut second_access_server, second_listen) =
        start_access_server(cluster.runtime_mut(), &access_binary, &seeds, "secondary");
    wait_for_tcp(&mut second_access_server, &second_listen).await;
    assert_access_ready(&listen);
    FullStackSetup {
        access_binary,
        cluster,
        identity,
        disk_arg,
        diskdb,
        rpc,
        diskio: Some(diskio),
        chunkdb_options,
        chunkdb: Some(chunkdb),
        chunk_kv,
        seeds,
        access_key,
        secret_key,
        access_server: Some(access_server),
        listen,
        second_access_server,
        second_listen,
        restarted_access: None,
    }
}

async fn start_durable_diskio(
    cluster: &mut KvCluster,
    rpc: &Arc<RpcServer>,
    identity: DiskioGroup0Identity,
    disk: &DiskArg,
) -> DiskioProcess {
    let seeds = cluster.mgmt_endpoints.clone();
    let diskio = DiskioProcess::start_for_group_in(
        cluster.runtime_mut(),
        &DiskioStartOpts {
            dummy_disk: "null",
            kv_seeds: &seeds,
            disks: std::slice::from_ref(disk),
            fault_error_rate: 0.0,
            fault_latency_ms: None,
            no_o_direct: true,
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
            rpc,
            &connection,
            DiskIoDiskId::new(0, disk.id_low),
        )
        .await;
    cluster
        .make_service_registry_client()
        .heartbeat_diskio_at(
            identity.instance_id,
            &format!("127.0.0.1:{}", diskio.port),
            identity.rack_id,
            identity.node_id,
            &[identity.disk_group_id],
            &[],
        )
        .await
        .expect("register durable diskio endpoint");
    diskio
}
async fn assert_range_delete_contract(cluster: &KvCluster) {
    let chunkdb_client = make_chunkdb_client(cluster.make_service_registry_client());
    let range_delete = chunkdb_client
        .delete_chunk_range(DeleteChunkRangeRequest {
            chunk_id: Some(ChunkId { high: 1, low: 1 }),
            chunk_offset: 0,
            chunk_size: 1,
        })
        .await;
    assert!(matches!(range_delete, Err(ChunkdbClientError::Unimplemented(_))));
}

async fn verify_diskdb_restart(
    process: &mut DiskdbProcess,
    seeds: &[String],
    disk_group_id: u64,
    listen: &str,
    access_key: &str,
    secret_key: &str,
) {
    let diskdb_started_at = unix_time_ms();
    process.restart().await;
    let service_registry = crowdb_kv_client::ServiceRegistryClient::new(
        crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(seeds.to_vec())),
    );
    process
        .wait_for_registry_ready(&service_registry, disk_group_id, diskdb_started_at)
        .await;
    run_restart_phase("verify-after-diskdb-restart", listen, access_key, secret_key);
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn assert_access_ready(listen: &str) {
    let ready = http_get(listen, "/_crowdb/health/ready");
    assert!(
        ready.starts_with("HTTP/1.1 200"),
        "access server not ready: {ready}"
    );
}

fn assert_native_write_metrics(listen: &str) {
    let exported = http_get(listen, "/_crowdb/metrics");
    let native_body_bytes = metric_value(&exported, "crowdb_s3_native_direct_bytes_total")
        + metric_value(&exported, "crowdb_s3_native_prefix_copy_bytes_total");
    assert!(native_body_bytes > 0);
    assert!(metric_value(&exported, "crowdb_s3_large_write_framed_owners_total") > 0);
    assert!(metric_value(&exported, "crowdb_s3_large_write_framed_views_total") > 0);
    assert_eq!(
        metric_value(&exported, "crowdb_s3_large_write_payload_copy_operations_total"),
        0
    );
}

fn issue_credentials(access_binary: &Path, seeds: &str) -> (String, String) {
    let missing = Command::new(access_binary)
        .args(["lookup-user", "boto3-e2e"])
        .env("CROWDB_MANAGEMENT_SEEDS", seeds)
        .env("CROWDB_S3_MASTER_KEY", MASTER_KEY)
        .output()
        .expect("look up absent S3 user");
    assert!(!missing.status.success());
    assert!(!String::from_utf8_lossy(&missing.stdout).contains("AWS_ACCESS_KEY_ID="));
    let issued = Command::new(access_binary)
        .args(["issue-user", "boto3-e2e"])
        .env("CROWDB_MANAGEMENT_SEEDS", seeds)
        .env("CROWDB_S3_MASTER_KEY", MASTER_KEY)
        .output()
        .expect("run S3 user-token issuer");
    assert!(
        issued.status.success(),
        "token issuer failed:\n{}",
        String::from_utf8_lossy(&issued.stderr)
    );
    let issued = String::from_utf8(issued.stdout).expect("token issuer output is UTF-8");
    for command in ["ensure-user", "lookup-user", "ensure-user"] {
        let resumed = Command::new(access_binary)
            .args([command, "boto3-e2e"])
            .env("CROWDB_MANAGEMENT_SEEDS", seeds)
            .env("CROWDB_S3_MASTER_KEY", MASTER_KEY)
            .output()
            .expect("run replay-safe S3 user-token issuer");
        assert!(
            resumed.status.success(),
            "token reconciliation failed:\n{}",
            String::from_utf8_lossy(&resumed.stderr)
        );
        let resumed = String::from_utf8(resumed.stdout).unwrap();
        for name in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"] {
            assert_eq!(output_value(&resumed, name), output_value(&issued, name));
        }
    }
    (
        output_value(&issued, "AWS_ACCESS_KEY_ID").to_owned(),
        output_value(&issued, "AWS_SECRET_ACCESS_KEY").to_owned(),
    )
}

fn start_access_server(
    runtime: &mut TestRuntime,
    access_binary: &Path,
    seeds: &str,
    identity: &str,
) -> (AccessServerProcess, String) {
    let port = runtime
        .assign_named_port(ServicePort::AccessServerHttp, identity)
        .expect("assign access-server port");
    let listen = format!("127.0.0.1:{port}");
    let service_root = runtime
        .service_dir("access-server", identity)
        .expect("create access-server runtime directory");
    let log_path = service_root.join("log").join("access-server.log");
    let log = std::fs::File::create(&log_path).expect("create access-server log");
    let child = Command::new(access_binary)
        .env("CROWDB_S3_LISTEN", &listen)
        .env("CROWDB_MANAGEMENT_SEEDS", seeds)
        .env("CROWDB_S3_TENANT", "boto3-e2e")
        .env("CROWDB_S3_MASTER_KEY", MASTER_KEY)
        .env("CROWDB_S3_REGION", "us-east-1")
        .env("CROWDB_S3_SMALL_OBJECT_LIMIT", "0")
        .env("CROWDB_S3_EC_DATA", "2")
        .env("CROWDB_S3_EC_CODE", "1")
        .env("CROWDB_S3_NATIVE_BUDGET_BYTES", (1024 * 1024).to_string())
        .env("CROWDB_S3_MAX_CHUNK_SIZE", (4 * 1024 * 1024).to_string())
        .stdout(Stdio::from(log.try_clone().expect("clone access-server log")))
        .stderr(Stdio::from(log))
        .spawn()
        .expect("start crowdb-access-server");
    runtime
        .record_process(child.id())
        .expect("record access-server process");
    (AccessServerProcess { child, log_path }, listen)
}

fn run_boto3_case(method: &str, context: &Boto3CaseContext<'_>) {
    let python_binary = std::env::var_os("CROWDB_S3_E2E_PYTHON").unwrap_or_else(|| "python".into());
    let python = Command::new(python_binary)
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/s3_e2e/basic.py"))
        .arg(format!("BasicS3CompatibilityTest.{method}"))
        .env("CROWDB_S3_E2E_ENDPOINT", format!("http://{}", context.listen))
        .env(
            "CROWDB_S3_E2E_SECOND_ENDPOINT",
            format!("http://{}", context.second_listen),
        )
        .env("CROWDB_S3_E2E_REGION", "us-east-1")
        .env("CROWDB_S3_E2E_ACCESS_KEY", context.access_key)
        .env("CROWDB_S3_E2E_SECRET_KEY", context.secret_key)
        .output()
        .expect("run boto3 compatibility suite");
    assert!(
        python.status.success(),
        "boto3 case {method} failed:\nstdout:\n{}\nstderr:\n{}\naccess server:\n{}\nchunk-kv:\n{}",
        String::from_utf8_lossy(&python.stdout),
        String::from_utf8_lossy(&python.stderr),
        context.access_server.log_content(),
        context.chunk_kv.log_content(),
    );
}

fn run_restart_phase(phase: &str, listen: &str, access_key: &str, secret_key: &str) {
    let python_binary = std::env::var_os("CROWDB_S3_E2E_PYTHON").unwrap_or_else(|| "python".into());
    let script = if phase == "lost-reply" {
        "tests/s3_e2e/lost_reply.py"
    } else {
        "tests/s3_e2e/restart.py"
    };
    let mut command = Command::new(python_binary);
    command.arg(Path::new(env!("CARGO_MANIFEST_DIR")).join(script));
    if phase != "lost-reply" {
        command.arg(phase);
    }
    let result = command
        .env("CROWDB_S3_E2E_ENDPOINT", format!("http://{listen}"))
        .env("CROWDB_S3_E2E_REGION", "us-east-1")
        .env("CROWDB_S3_E2E_ACCESS_KEY", access_key)
        .env("CROWDB_S3_E2E_SECRET_KEY", secret_key)
        .output()
        .expect("run access-server restart phase");
    assert!(
        result.status.success(),
        "restart {phase} failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
}

fn run_benchmark(listen: &str, server_pid: u32, access_key: &str, secret_key: &str, artifacts_dir: &Path) {
    let python_binary = std::env::var_os("CROWDB_S3_E2E_PYTHON").unwrap_or_else(|| "python".into());
    let result = Command::new(python_binary)
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/s3_e2e/benchmark.py"))
        .args(["--endpoint", &format!("http://{listen}")])
        .args(["--server-pid", &server_pid.to_string()])
        .args([
            "--sizes",
            "65536,1048576",
            "--concurrency",
            "1,2",
            "--samples",
            "1",
        ])
        .env("CROWDB_S3_E2E_REGION", "us-east-1")
        .env("CROWDB_S3_E2E_ACCESS_KEY", access_key)
        .env("CROWDB_S3_E2E_SECRET_KEY", secret_key)
        .output()
        .expect("run S3 baseline benchmark");
    assert!(
        result.status.success(),
        "S3 baseline benchmark failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(result.stdout.starts_with(b"{\n"), "benchmark did not emit JSON");
    assert!(
        String::from_utf8_lossy(&result.stdout).contains("\"server_process\": {"),
        "benchmark did not capture access-server CPU/RSS"
    );
    let artifact = artifacts_dir.join("s3-request-path.json");
    std::fs::write(&artifact, result.stdout).expect("write S3 baseline samples");
    eprintln!("S3 benchmark samples: {}", artifact.display());
}

async fn run_direct_chunk_benchmark(seeds: &[String], artifacts_dir: &Path) {
    let chunks = Arc::new(
        ChunkIoClient::connect(ChunkIoClientConfig {
            management_seeds: seeds.to_vec(),
            diskio_connections_per_endpoint: 2,
            diskio_rpc_workers: 1,
            small_write: SmallWritePolicy::default(),
        })
        .await
        .expect("connect benchmark chunk client"),
    );
    let policy = LargeWritePolicy {
        ec_scheme: EcScheme::new(2, 1),
        client: Arc::new(ChunkClientConfig {
            max_chunk_size: 4 * 1024 * 1024,
            ..ChunkClientConfig::default()
        }),
    };
    let mut samples = Vec::new();
    for size in [65_536_usize, 1_048_576] {
        for concurrency in [1_usize, 2] {
            let mut tasks = Vec::new();
            for index in 0..concurrency {
                let chunks = Arc::clone(&chunks);
                let policy = policy.clone();
                tasks.push(tokio::spawn(async move {
                    let payload = Bytes::from(
                        (0..size)
                            .map(|offset| u8::try_from(offset % 256).expect("byte value is bounded"))
                            .collect::<Vec<_>>(),
                    );
                    let started = Instant::now();
                    let result = chunks
                        .prepare_large_write(Some(size as u64), policy)
                        .write_buffers([payload.clone()])
                        .await
                        .expect("direct chunk write");
                    let put_ns = started.elapsed().as_nanos();
                    let started = Instant::now();
                    let read = chunks
                        .read_object(&result.locations)
                        .await
                        .expect("direct chunk read");
                    let get_ns = started.elapsed().as_nanos();
                    assert_eq!(read, payload);
                    let started = Instant::now();
                    let range = chunks
                        .read_range(&result.locations, 0, 4096)
                        .await
                        .expect("direct chunk range read");
                    let range_ns = started.elapsed().as_nanos();
                    assert_eq!(range, payload.slice(..4096));
                    json!({
                        "size": size,
                        "concurrency": concurrency,
                        "index": index,
                        "put_ns": put_ns,
                        "get_ns": get_ns,
                        "range_ns": range_ns,
                        "logical_bytes": result.logical_bytes,
                        "physical_bytes": result.physical_bytes,
                        "assembly_copies": result.assembly_copies,
                    })
                }));
            }
            for task in tasks {
                samples.push(task.await.expect("direct chunk benchmark task"));
            }
        }
    }
    let artifact = artifacts_dir.join("direct-chunk-path.json");
    std::fs::write(
        &artifact,
        serde_json::to_vec_pretty(&json!({
            "scope": "chunk-client only; excludes HTTP, SigV4, namespace metadata, and client network",
            "ec_data": 2,
            "ec_code": 1,
            "samples": samples,
        }))
        .expect("serialize direct chunk benchmark"),
    )
    .expect("write direct chunk baseline samples");
    eprintln!("direct chunk benchmark samples: {}", artifact.display());
}

fn http_get(address: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(address).expect("connect to access-server HTTP listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set access-server HTTP read timeout");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )
    .expect("write access-server HTTP request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read access-server HTTP response");
    response
}

fn metric_value(response: &str, name: &str) -> u64 {
    response
        .lines()
        .find_map(|line| line.strip_prefix(name).and_then(|value| value.strip_prefix(' ')))
        .unwrap_or_else(|| panic!("metrics response omitted {name}: {response}"))
        .parse()
        .unwrap_or_else(|error| panic!("invalid {name} metric: {error}"))
}

async fn seed_compact_hardware(hardware: &HardwareClient) -> Vec<DiskioGroup0Identity> {
    const RACK_ID: u64 = 1;
    const UNIT_BYTES: u32 = 1024 * 1024;
    const ZONE_UNITS: u64 = 16_384;
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
