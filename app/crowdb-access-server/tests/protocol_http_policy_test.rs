// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crowdb_access_iceberg::catalog::{Capabilities, ManagementPrivilege};
use crowdb_access_iceberg::file::{FileGrant, FileGrantIssuer, FileOperation, FileOperations, TableLocation};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::storage;
use crowdb_access_iceberg::wire::BearerAuthenticator;
use crowdb_chunk_client::{
    ChunkIoClient, ChunkIoClientConfig, ChunkIoWriter, ChunkReadPolicy, IoError, SmallWritePolicy,
};
use crowdb_chunkdb_client::ChunkdbRpcTransport;
use crowdb_console_shared::{
    config::{ConsoleConfig, ServiceType},
    lifecycle,
    ops::s3,
};
use crowdb_protocol::chunkdb::rpc::{ChunkType, ListChunksRequest, Strip};
use crowdb_protocol::port::namespace::RuntimeNamespace;
use crowdb_protocol::ServicePort;
use crowdb_test_harness::test_dirs::TestDir;
use reqwest::{Client, Method};

mod common {
    pub fn now_ms() -> u64 {
        super::now_ms()
    }
}

#[path = "common/iceberg_signed_file.rs"]
mod signed;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

fn listener_addresses() -> (RuntimeNamespace, SocketAddr, SocketAddr) {
    let mut ports = RuntimeNamespace::ephemeral("combined-access-listeners").unwrap();
    let s3 = ports.assign_port(ServicePort::AccessServerHttp, 0).unwrap();
    let iceberg = ports
        .assign_port(ServicePort::AccessServerIcebergHttp, 0)
        .unwrap();
    (
        ports,
        ([127, 0, 0, 1], s3).into(),
        ([127, 0, 0, 1], iceberg).into(),
    )
}

struct RunningAccess {
    child: Child,
    _ports: RuntimeNamespace,
}

impl Drop for RunningAccess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct StopClusterOnDrop<'a>(&'a Path);

impl Drop for StopClusterOnDrop<'_> {
    fn drop(&mut self) {
        let _ = s3::stop(self.0);
    }
}

async fn initialize_iceberg(seeds: Vec<String>) {
    let small = SmallWritePolicy {
        conversion_enabled: false,
        mirror_copies: 2,
        ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
    };
    let (repository, _, chunks) = storage::connect(seeds, ChunkReadPolicy::default(), small, 2, 2)
        .await
        .unwrap();
    for (action, epoch, capabilities) in [
        (ManagementAction::Initialize, 0, None),
        (
            ManagementAction::Activate,
            1,
            Some(Capabilities::from_bits(0x3fff).unwrap()),
        ),
    ] {
        repository
            .execute(
                ManagementRequest {
                    identity: RequestIdentity {
                        operation: OperationId::random(),
                        issued_ms: now_ms(),
                    },
                    principal: "manager".into(),
                    action,
                    expected_epoch: epoch,
                    display_name: "protected-http".into(),
                    confirmation: None,
                    capabilities,
                },
                ManagementPrivilege::Manage,
                now_ms(),
            )
            .await
            .unwrap();
    }
    chunks.shutdown_small_writes().await.unwrap();
}

async fn start_access(
    config: &Path,
    seeds: &[String],
    s3_addr: SocketAddr,
    iceberg_addr: SocketAddr,
    ports: RuntimeNamespace,
) -> RunningAccess {
    start_access_with_fault(config, seeds, s3_addr, iceberg_addr, ports, None).await
}

async fn start_access_with_fault(
    config: &Path,
    seeds: &[String],
    s3_addr: SocketAddr,
    iceberg_addr: SocketAddr,
    ports: RuntimeNamespace,
    stop_s3_manager_file: Option<&Path>,
) -> RunningAccess {
    let mut command = Command::new(env!("CARGO_BIN_EXE_crowdb-access-server"));
    command
        .args(["--config", config.to_str().unwrap()])
        .env("CROWDB_MANAGEMENT_SEEDS", seeds.join(","))
        .env("CROWDB_S3_LISTEN", s3_addr.to_string())
        .env("CROWDB_S3_TENANT", "local")
        .env(
            "CROWDB_S3_MASTER_KEY",
            "1111111111111111111111111111111111111111111111111111111111111111",
        )
        .env("CROWDB_S3_REGION", "us-east-1")
        .env("CROWDB_S3_TRUSTED_NETWORK", "true")
        .env("CROWDB_ICEBERG_LISTEN", iceberg_addr.to_string())
        .env("CROWDB_ICEBERG_READ_TOKEN", "r".repeat(32))
        .env("CROWDB_ICEBERG_WRITE_TOKEN", "w".repeat(32))
        .env("CROWDB_ICEBERG_MANAGE_TOKEN", "m".repeat(32))
        .env("CROWDB_ICEBERG_CLEAR_TOKEN", "c".repeat(32))
        .env("CROWDB_ICEBERG_GC_ENABLED", "0")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if let Some(path) = stop_s3_manager_file {
        command.env("CROWDB_TEST_STOP_S3_MANAGER_FILE", path);
    }
    let child = command.spawn().unwrap();
    let mut process = RunningAccess { child, _ports: ports };
    let client = Client::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(status) = process.child.try_wait().unwrap() {
                panic!("combined access process exited before readiness: {status}");
            }
            let s3_ready = client
                .get(format!("http://{s3_addr}/_crowdb/health/ready"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
            let iceberg_ready = client
                .get(format!("http://{iceberg_addr}/v1/config"))
                .bearer_auth("r".repeat(32))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
            if s3_ready && iceberg_ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    process
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts a complete simulated three-rack production storage stack"]
async fn combined_http_listeners_keep_protocol_chunk_policies_separate() {
    let dir = TestDir::new("access-protected-http-policy").unwrap();
    s3::start_protected_test_cluster(dir.path()).await.unwrap();
    let _cleanup = StopClusterOnDrop(dir.path());
    let (cluster, _) = s3::load(dir.path()).unwrap();
    let seeds = cluster
        .servers
        .iter()
        .filter(|server| server.service_type == ServiceType::Kv)
        .map(|server| server.url.clone())
        .collect::<Vec<_>>();
    initialize_iceberg(seeds.clone()).await;

    let (ports, s3_addr, iceberg_addr) = listener_addresses();
    let config = dir.path().join("combined-access.toml");
    std::fs::write(
        &config,
        "[s3]\nec_data = 2\nec_code = 1\nlarge_prefetch_strips_per_chunk = 2\nlarge_memory_budget_bytes = 67108864\n[s3.small_write]\nconversion_enabled = false\nmirror_copies = 2\n[iceberg]\nec_data = 4\nec_code = 2\nlarge_prefetch_strips_per_chunk = 3\nlarge_memory_budget_bytes = 100663296\n[iceberg.small_write]\nconversion_enabled = false\nmirror_copies = 2\n",
    )
    .unwrap();
    let _access = start_access(&config, &seeds, s3_addr, iceberg_addr, ports).await;
    write_s3(s3_addr).await;
    write_iceberg(iceberg_addr, &seeds).await;
    assert_chunk_layouts(&cluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts a complete simulated three-rack production storage stack"]
async fn terminal_s3_storage_failure_stops_both_access_listeners() {
    let dir = TestDir::new("access-storage-failure").unwrap();
    s3::start_protected_test_cluster(dir.path()).await.unwrap();
    let _cleanup = StopClusterOnDrop(dir.path());
    let (cluster, _) = s3::load(dir.path()).unwrap();
    let seeds = cluster
        .servers
        .iter()
        .filter(|server| server.service_type == ServiceType::Kv)
        .map(|server| server.url.clone())
        .collect::<Vec<_>>();
    initialize_iceberg(seeds.clone()).await;
    let (ports, s3_addr, iceberg_addr) = listener_addresses();
    let config = dir.path().join("combined-access.toml");
    std::fs::write(
        &config,
        "[s3.small_write]\nmirror_copies = 2\n[iceberg.small_write]\nmirror_copies = 2\n",
    )
    .unwrap();
    let sentinel = dir.path().join("stop-s3-manager");
    let mut access =
        start_access_with_fault(&config, &seeds, s3_addr, iceberg_addr, ports, Some(&sentinel)).await;
    let s3_client = s3::S3HttpClient::new(format!("http://{s3_addr}")).unwrap();
    s3_client
        .request(Method::PUT, Some("failure"), None, &[], None, None)
        .await
        .unwrap();
    s3_client
        .request(
            Method::PUT,
            Some("failure"),
            Some("small"),
            &[],
            Some(vec![0x42; 1024]),
            None,
        )
        .await
        .unwrap();
    std::fs::write(&sentinel, b"stop").unwrap();
    let status = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(status) = access.child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("combined access process must stop after storage manager failure");
    assert!(!status.success());
    assert!(tokio::net::TcpStream::connect(s3_addr).await.is_err());
    assert!(tokio::net::TcpStream::connect(iceberg_addr).await.is_err());
}

async fn write_s3(s3_addr: SocketAddr) {
    let s3_client = s3::S3HttpClient::new(format!("http://{s3_addr}")).unwrap();
    s3_client
        .request(Method::PUT, Some("policy"), None, &[], None, None)
        .await
        .unwrap();
    for (name, bytes) in [
        ("small-a", vec![0x31; 60 * 1024]),
        ("small-b", vec![0x33; 20 * 1024]),
        ("large", vec![0x32; 2 * 1024 * 1024]),
    ] {
        s3_client
            .request(
                Method::PUT,
                Some("policy"),
                Some(name),
                &[],
                Some(bytes.clone()),
                None,
            )
            .await
            .unwrap();
        let (_, read) = s3_client
            .request(Method::GET, Some("policy"), Some(name), &[], None, None)
            .await
            .unwrap();
        assert_eq!(read, bytes);
    }
}

async fn write_iceberg(iceberg_addr: SocketAddr, seeds: &[String]) {
    let client = Client::new();
    let endpoint = format!("http://{iceberg_addr}");
    let namespace = client
        .post(format!("{endpoint}/v1/namespaces"))
        .bearer_auth("w".repeat(32))
        .json(&serde_json::json!({"namespace": ["analytics"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(namespace.status(), 200, "{}", namespace.text().await.unwrap());
    let draft = client
        .post(format!("{endpoint}/v1/namespaces/analytics/tables"))
        .bearer_auth("w".repeat(32))
        .json(&serde_json::json!({"name": "files", "stage-create": true,
            "schema": {"type": "struct", "fields": []}}))
        .send()
        .await
        .unwrap();
    assert_eq!(draft.status(), 200, "{}", draft.text().await.unwrap());
    let draft: serde_json::Value = draft.json().await.unwrap();
    let table: TableLocation = format!("{}/", draft["metadata"]["location"].as_str().unwrap())
        .parse()
        .unwrap();
    let authenticator =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let issuer = FileGrantIssuer::new(authenticator.namespace_token_key(), 900_000).unwrap();
    let (repository, _, chunks) = storage::connect(
        seeds.to_vec(),
        ChunkReadPolicy::default(),
        SmallWritePolicy {
            mirror_copies: 2,
            conversion_enabled: false,
            ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
        },
        2,
        2,
    )
    .await
    .unwrap();
    let context = repository.status().await.unwrap().0.context;
    let credentials = issuer
        .issue(FileGrant {
            context,
            table: table.table,
            principal: [7; 32],
            nonce: OperationId::random(),
            issued_ms: now_ms() - 1_000,
            expires_ms: now_ms() + 600_000,
            operations: FileOperations::new(&[FileOperation::Get, FileOperation::Put, FileOperation::Head])
                .unwrap(),
            max_request_bytes: 8 * 1024 * 1024,
            max_file_bytes: 8 * 1024 * 1024,
        })
        .unwrap();
    let file_client = signed::TestFileClient {
        client,
        credentials,
        address: iceberg_addr,
    };
    for (name, bytes) in [
        ("small-a", vec![0x41; 800 * 1024]),
        ("small-b", vec![0x43; 200 * 1024]),
        ("large", vec![0x42; 4 * 1024 * 1024]),
    ] {
        let path = format!(
            "/{}/{}",
            table.bucket(),
            table.file(&format!("data/{name}.bin")).unwrap().object_key()
        );
        let put = file_client.send(Method::PUT, &path, "", &bytes, true).await;
        assert_eq!(put.status(), 200, "{}", put.text().await.unwrap());
        let get = file_client.send(Method::GET, &path, "", b"", false).await;
        assert_eq!(get.status(), 200);
        assert_eq!(get.bytes().await.unwrap().as_ref(), bytes);
    }
    chunks.shutdown_small_writes().await.unwrap();
}

async fn assert_chunk_layouts(cluster: &ConsoleConfig) {
    let transport = ChunkdbRpcTransport::new();
    let mut saw = [false; 4];
    for server in cluster
        .servers
        .iter()
        .filter(|server| server.service_type == ServiceType::Chunkdb)
    {
        let listed = transport
            .send_list_chunks(
                server.rpc_url.as_deref().unwrap(),
                &ListChunksRequest {
                    max_keys: 1_024,
                    ..ListChunksRequest::default()
                },
            )
            .await
            .unwrap();
        for chunk in listed.chunks {
            let is_s3 = chunk.chunk_type == ChunkType::S3 as i32;
            let is_iceberg = chunk.chunk_type == ChunkType::IcebergTable as i32;
            if !is_s3 && !is_iceberg {
                continue;
            }
            assert_eq!(
                chunk.id.unwrap().high >> 56,
                u64::try_from(chunk.chunk_type).unwrap()
            );
            for strip in chunk.strips {
                match strip.strip.unwrap() {
                    Strip::MirrorStrip(mirror) => {
                        assert_eq!(mirror.segments.len(), 2);
                        saw[if is_s3 { 0 } else { 2 }] = true;
                    }
                    Strip::EcStrip(ec) => {
                        assert_eq!((ec.data_num, ec.code_num), if is_s3 { (2, 1) } else { (4, 2) });
                        saw[if is_s3 { 1 } else { 3 }] = true;
                    }
                }
            }
        }
    }
    assert!(
        saw.into_iter().all(|seen| seen),
        "both protocols must write small mirror and large EC strips"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stops all real DiskIO processes in a simulated three-rack production cluster"]
async fn protected_two_copy_write_stops_after_repair_and_chunk_rotation_fail() {
    let dir = TestDir::new("access-protected-mirror-failure").unwrap();
    s3::start_protected_test_cluster(dir.path()).await.unwrap();
    let _cleanup = StopClusterOnDrop(dir.path());
    let (cluster, _) = s3::load(dir.path()).unwrap();
    let seeds = cluster
        .servers
        .iter()
        .filter(|server| server.service_type == ServiceType::Kv)
        .map(|server| server.url.clone())
        .collect();
    let client = ChunkIoClient::connect(ChunkIoClientConfig {
        management_seeds: seeds,
        diskio_connections_per_endpoint: 2,
        diskio_rpc_workers: 1,
        small_write: SmallWritePolicy {
            chunk_type: ChunkType::S3,
            conversion_enabled: false,
            mirror_copies: 2,
            ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
        },
    })
    .await
    .unwrap();
    for server in cluster
        .servers
        .iter()
        .filter(|server| server.service_type == ServiceType::Diskio)
    {
        lifecycle::stop_pid_with_timeout(server.pid.unwrap(), Duration::from_secs(5)).unwrap();
    }
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let data = hyper::body::Bytes::from_static(b"failed-protected-write");
        let mut writer = client.prepare_small_write(data.len()).await?;
        writer.on_data(data).await?;
        writer.on_finish().await.map(|_| ())
    })
    .await
    .expect("failed DiskIO write exceeded the 15-second fault budget");
    assert!(matches!(result, Err(IoError::WriteFailed(_))), "{result:?}");
    let metrics = client.small_write_metrics();
    assert_eq!(metrics.completed, 0);
    assert_eq!(metrics.failed, 1);
    assert_eq!(metrics.exhausted_repairs, 2);
    assert_eq!(metrics.repairs_avoiding_rotation, 0);
    let _ = client.shutdown_small_writes().await;
}
