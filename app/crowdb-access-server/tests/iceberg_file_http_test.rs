#[path = "common/iceberg_stack.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/iceberg_process.rs"]
#[allow(dead_code)]
mod process;

#[path = "common/iceberg_commit_child.rs"]
mod child;
#[path = "common/iceberg_commit_fault.rs"]
mod fault;
#[path = "common/iceberg_file_listing.rs"]
mod listing;
#[path = "common/iceberg_upload_profiles.rs"]
mod profiles;
#[path = "common/iceberg_file_recovery.rs"]
mod recovery;

use common::{now_ms, TestIcebergStack};
use crowdb_access_iceberg::catalog::{CatalogRepository, ClearBounds, ManagementPrivilege};
use crowdb_access_iceberg::file::{
    FileGrant, FileGrantIssuer, FileOperation, FileOperations, FileRepository, TableLocation,
};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::wire::BearerAuthenticator;
use crowdb_access_server::config::AccessConfig;
use crowdb_common::config::load_from_file;
use reqwest::{Client, Method};
use std::time::Instant;

#[path = "common/iceberg_signed_file.rs"]
mod signed;
use signed::TestFileClient;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "test-only child listener invoked by native file crash matrix"]
async fn native_fault_listener_child() {
    child::run().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native storage and kills listener subprocesses at durable FileIO boundaries"]
async fn native_file_publication_recovers_across_listeners_at_every_durable_write() {
    recovery::run().await;
}

async fn setup() -> (
    TestIcebergStack,
    process::TestIcebergProcess,
    TestFileClient,
    TableLocation,
) {
    setup_with_bounds(ClearBounds {
        delegated_access_ms: 900_000,
        ..ClearBounds::default()
    })
    .await
}

async fn setup_with_bounds(
    bounds: ClearBounds,
) -> (
    TestIcebergStack,
    process::TestIcebergProcess,
    TestFileClient,
    TableLocation,
) {
    setup_with_bounds_and_file_limit(bounds, 16 * 1024 * 1024, 64 * 1024 * 1024).await
}

async fn setup_with_bounds_and_file_limit(
    bounds: ClearBounds,
    max_request_bytes: u64,
    max_file_bytes: u64,
) -> (
    TestIcebergStack,
    process::TestIcebergProcess,
    TestFileClient,
    TableLocation,
) {
    let stack = TestIcebergStack::start().await;
    let repository = CatalogRepository::new(stack.store().await, bounds).unwrap();
    repository
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: now_ms(),
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "file-http".into(),
                confirmation: None,
                capabilities: None,
            },
            ManagementPrivilege::Manage,
            now_ms(),
        )
        .await
        .unwrap();
    common::activate(&repository).await;
    let context = repository.status().await.unwrap().0.context;
    let process = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let client = Client::new();
    let endpoint = format!("http://{}", process.address);
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
    let ports = crowdb_protocol::ServicePort::AccessServerIcebergHttp;
    assert!((ports.base()..ports.base() + ports.range_size()).contains(&process.address.port()));
    assert_eq!(draft["config"]["s3.endpoint"], endpoint);
    let table: TableLocation = format!("{}/", draft["metadata"]["location"].as_str().unwrap())
        .parse()
        .unwrap();
    let authenticator =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let issuer = FileGrantIssuer::new(authenticator.namespace_token_key(), 15 * 60 * 1000).unwrap();
    let started = now_ms();
    let credentials = issuer
        .issue(FileGrant {
            context,
            table: table.table,
            principal: [7; 32],
            nonce: OperationId::random(),
            issued_ms: started - 1_000,
            expires_ms: started + 10 * 60 * 1000,
            operations: FileOperations::new(&[
                FileOperation::Head,
                FileOperation::Get,
                FileOperation::ListObjects,
                FileOperation::Put,
                FileOperation::CreateMultipart,
                FileOperation::UploadPart,
                FileOperation::ListParts,
                FileOperation::CompleteMultipart,
                FileOperation::AbortMultipart,
            ])
            .unwrap(),
            max_request_bytes,
            max_file_bytes,
        })
        .unwrap();
    let client = TestFileClient {
        client,
        credentials,
        address: process.address,
    };
    (stack, process, client, table)
}

fn path(table: TableLocation, key: &str) -> String {
    format!("/{}/{}", table.bucket(), table.file(key).unwrap().object_key())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "manual TPC-H and TPC-DS loader stress on the small cluster"]
async fn tpc_loader_parallel_stress() {
    let (_stack, _process, client, _table) = setup().await;
    let python = std::env::var("CROWDB_TPC_LOADER_PYTHON")
        .expect("set CROWDB_TPC_LOADER_PYTHON to the installed TPC loader environment");
    let endpoint = format!("http://{}", client.address);
    let mut commands = Vec::new();
    for (benchmark, workers) in [("tpch", "8"), ("tpcds", "24")] {
        let mut command = tokio::process::Command::new("pixi");
        command
            .args([
                "run",
                "-e",
                "iceberg-e2e",
                "--",
                &python,
                "-m",
                "crowdb_tpc_loader",
                "load",
                "--benchmark",
                benchmark,
                "--sf",
                "1",
                "--namespace",
                benchmark,
                "--upload-workers",
                workers,
                "--no-download",
                "--keep-files",
            ])
            .env("ICEBERG_URI", &endpoint)
            .env("ICEBERG_TOKEN", "w".repeat(32))
            .current_dir(env!("CARGO_MANIFEST_DIR"));
        commands.push(command);
    }
    let mut tpcds = commands.pop().unwrap();
    let mut tpch = commands.pop().unwrap();
    let (tpch_result, tpcds_result) = tokio::join!(tpch.output(), tpcds.output());
    let mut failures = Vec::new();
    for (benchmark, output) in [("tpch", tpch_result.unwrap()), ("tpcds", tpcds_result.unwrap())] {
        println!("{benchmark} status: {}", output.status);
        println!("{benchmark} stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("{benchmark} stderr: {}", String::from_utf8_lossy(&output.stderr));
        if !output.status.success() {
            failures.push(benchmark);
        }
    }
    assert!(failures.is_empty(), "failed loaders: {failures:?}");
}

fn fixture_config() -> AccessConfig {
    load_from_file(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/common/iceberg_single_node.toml"),
    )
    .unwrap()
}

async fn catalog_counts(client: &TestFileClient) -> (u64, u64, u64, u64) {
    let response: serde_json::Value = client
        .client
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let catalog = &response["catalog"];
    (
        catalog["get"].as_u64().unwrap(),
        catalog["compare_exchange"].as_u64().unwrap(),
        catalog["scan"].as_u64().unwrap(),
        catalog["conditional_delete"].as_u64().unwrap(),
    )
}

fn catalog_delta(before: (u64, u64, u64, u64), after: (u64, u64, u64, u64)) -> String {
    format!(
        "get={} cas={} scan={} delete={}",
        after.0 - before.0,
        after.1 - before.1,
        after.2 - before.2,
        after.3 - before.3
    )
}

async fn upload_route_counts(client: &TestFileClient) -> (u64, u64, u64) {
    let metrics: serde_json::Value = client
        .client
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (
        metrics["chunk_small_write"]["completed"].as_u64().unwrap(),
        metrics["upload_flow"]["strip_write_successes"].as_u64().unwrap(),
        metrics["upload_flow"]["writer_feeds"].as_u64().unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "native small-file performance fixture; run during consolidated acceptance"]
async fn native_small_file_profiles() {
    profiles::small_file_profiles().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "native null-DiskIO release performance fixture"]
async fn native_file_5_mib_profile() {
    let (stack, _process, client, table) = setup().await;
    let bytes = vec![0x5a; 5 * 1024 * 1024];
    assert!(bytes.len() >= fixture_config().iceberg_small_write().threshold_exclusive());
    let object = path(table, "data/profile-put.bin");
    let before = catalog_counts(&client).await;
    let route_before = upload_route_counts(&client).await;
    let started = Instant::now();
    let put = client.send(Method::PUT, &object, "", &bytes, true).await;
    let put_ms = started.elapsed().as_millis();
    assert_eq!(put.status(), 200, "{}", put.text().await.unwrap());
    println!(
        "iceberg 5MiB PUT: {put_ms}ms {}",
        catalog_delta(before, catalog_counts(&client).await)
    );
    let route_after = upload_route_counts(&client).await;
    assert_eq!(route_after.0 - route_before.0, 0, "5MiB PUT entered small-write");
    assert!(
        route_after.1 > route_before.1,
        "5MiB PUT did not write large strips"
    );
    println!(
        "iceberg 5MiB PUT route: small_completed={} large_strips={} writer_feeds={}",
        route_after.0 - route_before.0,
        route_after.1 - route_before.1,
        route_after.2 - route_before.2
    );
    profiles::assert_one_file_chunk(&stack, table).await;

    let multipart = path(table, "data/profile-mpu.bin");
    let created = client
        .send(Method::POST, &multipart, "uploads=", b"", false)
        .await;
    assert_eq!(created.status(), 200);
    let created = created.text().await.unwrap();
    let upload = created
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let query = format!("partNumber=1&uploadId={upload}");
    let before = catalog_counts(&client).await;
    let route_before = upload_route_counts(&client).await;
    let started = Instant::now();
    let part = client.send(Method::PUT, &multipart, &query, &bytes, true).await;
    let part_ms = started.elapsed().as_millis();
    assert_eq!(part.status(), 200, "{}", part.text().await.unwrap());
    let etag = part.headers()["etag"].to_str().unwrap();
    println!(
        "iceberg 5MiB UploadPart: {part_ms}ms {}",
        catalog_delta(before, catalog_counts(&client).await)
    );
    let route_after = upload_route_counts(&client).await;
    assert_eq!(
        route_after.0 - route_before.0,
        0,
        "5MiB UploadPart entered small-write"
    );
    assert!(
        route_after.1 > route_before.1,
        "5MiB UploadPart did not write large strips"
    );
    println!(
        "iceberg 5MiB UploadPart route: small_completed={} large_strips={} writer_feeds={}",
        route_after.0 - route_before.0,
        route_after.1 - route_before.1,
        route_after.2 - route_before.2
    );
    let manifest = format!(
        "<CompleteMultipartUpload><Part><ETag>{etag}</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"
    );
    let complete = client
        .send(
            Method::POST,
            &multipart,
            &format!("uploadId={upload}"),
            manifest.as_bytes(),
            false,
        )
        .await;
    assert_eq!(complete.status(), 200);
    assert!(complete
        .text()
        .await
        .unwrap()
        .contains("</CompleteMultipartUploadResult>"));

    let before = catalog_counts(&client).await;
    let started = Instant::now();
    let get = client.send(Method::GET, &object, "", b"", false).await;
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), bytes);
    let get_ms = started.elapsed().as_millis();
    println!(
        "iceberg 5MiB GET: {get_ms}ms {}",
        catalog_delta(before, catalog_counts(&client).await)
    );
}
