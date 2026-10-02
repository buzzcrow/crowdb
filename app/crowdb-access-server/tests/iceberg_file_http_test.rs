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
#[path = "common/iceberg_file_lifecycle.rs"]
mod lifecycle;
#[path = "common/iceberg_file_listing.rs"]
mod listing;
#[path = "common/iceberg_upload_profiles.rs"]
mod profiles;
#[path = "common/iceberg_file_recovery.rs"]
mod recovery;

use common::{now_ms, TestIcebergStack};
use crowdb_access_iceberg::catalog::{CatalogRepository, ClearBounds, ManagementPrivilege};
use crowdb_access_iceberg::file::{
    FileGrant, FileGrantIssuer, FileKind, FileLocation, FileOperation, FileOperations, FileRepository,
    TableLocation,
};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::wire::BearerAuthenticator;
use crowdb_access_server::config::AccessConfig;
use crowdb_common::config::load_from_file;
use crowdb_protocol::chunkdb::rpc::{QueryChunkRequest, Strip};
use crowdb_test_harness::chunkdb::make_client as make_chunkdb_client;
use futures::StreamExt;
use md5::{Digest, Md5};
use reqwest::{Client, Method};
use std::fmt::Write as _;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native storage and checks real-time credential expiry and lifecycle fencing"]
async fn native_file_credentials_refresh_expire_and_follow_lifecycle_fences() {
    lifecycle::run().await;
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

async fn cleanup_client(
    stack: &TestIcebergStack,
    client: &TestFileClient,
    table: TableLocation,
) -> TestFileClient {
    let context = CatalogRepository::new(
        stack.store().await,
        ClearBounds {
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
    )
    .unwrap()
    .status()
    .await
    .unwrap()
    .0
    .context;
    let authenticator =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let issuer = FileGrantIssuer::new(authenticator.namespace_token_key(), 15 * 60 * 1000).unwrap();
    let started = now_ms();
    TestFileClient {
        client: client.client.clone(),
        credentials: issuer
            .issue(FileGrant {
                context,
                table: table.table,
                principal: [9; 32],
                nonce: OperationId::random(),
                issued_ms: started - 1_000,
                expires_ms: started + 10 * 60 * 1000,
                operations: FileOperations::new(&[FileOperation::DeleteObject, FileOperation::DeleteObjects])
                    .unwrap(),
                max_request_bytes: 1024 * 1024,
                max_file_bytes: 1024 * 1024,
            })
            .unwrap(),
        address: client.address,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_multipart_creates_use_independent_sessions() {
    let (_stack, _process, client, table) = setup().await;
    let mut requests = tokio::task::JoinSet::new();
    for index in 0..24 {
        let object = path(table, &format!("data/concurrent-{index}.parquet"));
        let request = client.request(Method::POST, &object, "uploads=", b"", false, None);
        requests.spawn(async move { request.send().await.unwrap() });
    }
    while let Some(result) = requests.join_next().await {
        let response = result.unwrap();
        assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_cleanup_supports_single_batch_and_same_path_reupload() {
    let (stack, _process, client, table) = setup().await;
    let object = path(table, "data/cleanup.parquet");
    let initial = client.send(Method::PUT, &object, "", b"first", false).await;
    assert_eq!(initial.status(), 200, "{}", initial.text().await.unwrap());
    let ordinary = client.send(Method::DELETE, &object, "", b"", false).await;
    assert_eq!(ordinary.status(), 403);

    let cleanup = cleanup_client(&stack, &client, table).await;
    let deleted = cleanup.send(Method::DELETE, &object, "", b"", false).await;
    assert_eq!(deleted.status(), 204, "{}", deleted.text().await.unwrap());
    let missing = client.send(Method::GET, &object, "", b"", false).await;
    assert_eq!(missing.status(), 404);
    let recreated = client.send(Method::PUT, &object, "", b"second", false).await;
    assert_eq!(recreated.status(), 200, "{}", recreated.text().await.unwrap());

    let location = table.file("data/cleanup.parquet").unwrap();
    let missing_key = table.file("data/unknown.parquet").unwrap();
    let xml = format!(
        "<Delete><Object><Key>{}</Key></Object><Object><Key>{}</Key></Object></Delete>",
        location.object_key(),
        missing_key.object_key()
    );
    let response = cleanup
        .send(
            Method::POST,
            &format!("/{}", table.bucket()),
            "delete=",
            xml.as_bytes(),
            true,
        )
        .await;
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("<Deleted>"), "{body}");
    assert!(!body.contains("<Error>"), "{body}");
    assert_eq!(
        client.send(Method::GET, &object, "", b"", false).await.status(),
        404
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_cleanup_rejects_a_retained_metadata_file() {
    let (stack, _process, client, _) = setup().await;
    let endpoint = format!("http://{}", client.address);
    let created = client
        .client
        .post(format!("{endpoint}/v1/namespaces/analytics/tables"))
        .bearer_auth("w".repeat(32))
        .json(&serde_json::json!({"name":"protected", "schema":{"type":"struct","fields":[]}}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200, "{}", created.text().await.unwrap());
    let created: serde_json::Value = created.json().await.unwrap();
    let location: FileLocation = created["metadata-location"].as_str().unwrap().parse().unwrap();
    let table = location.table();
    let cleanup = cleanup_client(&stack, &client, table).await;
    let object = format!("/{}/{}", table.bucket(), location.object_key());
    let single = cleanup.send(Method::DELETE, &object, "", b"", false).await;
    assert_eq!(single.status(), 409, "{}", single.text().await.unwrap());
    let xml = format!(
        "<Delete><Object><Key>{}</Key></Object></Delete>",
        location.object_key()
    );
    let batch = cleanup
        .send(
            Method::POST,
            &format!("/{}", table.bucket()),
            "delete=",
            xml.as_bytes(),
            true,
        )
        .await;
    let status = batch.status();
    let body = batch.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("<Error>"), "{body}");
    assert!(FileRepository::new(stack.store().await)
        .load(cleanup.credentials.grant().context, &location)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn thirty_two_concurrent_direct_puts() {
    let (_stack, _process, client, table) = setup().await;
    let payload = vec![0x5a; 8 * 1024 * 1024];
    let mut requests = tokio::task::JoinSet::new();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(33));
    for index in 0..32 {
        let object = path(table, &format!("data/parallel-{index}.parquet"));
        let request = client.request(Method::PUT, &object, "", &payload, false, None);
        let barrier = barrier.clone();
        requests.spawn(async move {
            barrier.wait().await;
            let response = request.send().await.unwrap();
            (index, response.status(), response.text().await.unwrap())
        });
    }
    barrier.wait().await;
    let mut failures = Vec::new();
    while let Some(result) = requests.join_next().await {
        let (index, status, body) = result.unwrap();
        println!("upload {index}: {status} {body}");
        if status != 200 {
            failures.push((index, status, body));
        }
    }
    assert!(failures.is_empty(), "failed uploads: {failures:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "manual TPC-H and TPC-DS loader stress on the small cluster"]
async fn tpc_loader_parallel_stress() {
    let (_stack, _process, client, _table) = setup().await;
    let python = "/cpp/crowdb-tpc-loader/.venv/bin/python";
    let endpoint = format!("http://{}", client.address);
    let mut commands = Vec::new();
    for (benchmark, workers) in [("tpch", "8"), ("tpcds", "24")] {
        let mut command = tokio::process::Command::new("/home/cj/.pixi/bin/pixi");
        command
            .args([
                "run",
                "-e",
                "iceberg-e2e",
                "--",
                python,
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

async fn file_request_counts(client: &TestFileClient) -> (u64, u64) {
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
    let catalog = &response["routes"][6][0]["catalog"];
    (
        catalog["get"].as_u64().unwrap(),
        catalog["compare_exchange"].as_u64().unwrap(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn signed_standard_put_get_and_multipart_publish_unbound_files() {
    let (stack, _process, client, table) = setup().await;
    let parquet = b"PAR1datafoot\x04\0\0\0PAR1";
    let object = path(table, "data/a.parquet");
    let put = client.send(Method::PUT, &object, "", parquet, true).await;
    assert_eq!(put.status(), 200, "{}", put.text().await.unwrap());
    let head = client.send(Method::HEAD, &object, "", b"", false).await;
    assert_eq!(head.status(), 200);
    assert_eq!(head.headers()["content-length"], parquet.len().to_string());
    let get = client.send(Method::GET, &object, "", b"", false).await;
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), parquet);
    let range = client
        .send_range(Method::GET, &object, "", b"", false, Some("bytes=4-7"))
        .await;
    assert_eq!(range.status(), 206);
    assert_eq!(range.headers()["content-range"], "bytes 4-7/20");
    assert_eq!(range.bytes().await.unwrap().as_ref(), b"data");
    let repository = FileRepository::new(stack.store().await);
    let record = repository
        .load(
            client.credentials.grant().context,
            &table.file("data/a.parquet").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.kind, FileKind::Unbound);
    assert!(record.bind_kind(FileKind::EqualityDelete).is_ok());
    let conflict = client
        .send(Method::PUT, &object, "", b"PAR1difffoot\x04\0\0\0PAR1", true)
        .await;
    assert_eq!(conflict.status(), 409);

    let medium = path(table, "data/medium.parquet");
    let medium_bytes = (0..1_200_000)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect::<Vec<_>>();
    let before = file_request_counts(&client).await;
    let started = Instant::now();
    let response = client.send(Method::PUT, &medium, "", &medium_bytes, true).await;
    let elapsed = started.elapsed();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let after = file_request_counts(&client).await;
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "1.2 MiB PUT took {elapsed:?}"
    );
    assert!(
        after.0 - before.0 <= 15 && after.1 - before.1 <= 2,
        "1.2 MiB PUT used {} gets and {} CAS operations",
        after.0 - before.0,
        after.1 - before.1
    );
    let stored = repository
        .load(
            client.credentials.grant().context,
            &table.file("data/medium.parquet").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.content.locations(stored.length).unwrap().unwrap().len(), 1);
    let range = client
        .send_range(
            Method::GET,
            &medium,
            "",
            b"",
            false,
            Some("bytes=1048550-1048600"),
        )
        .await;
    assert_eq!(range.status(), 206);
    assert_eq!(
        range.bytes().await.unwrap().as_ref(),
        &medium_bytes[1_048_550..1_048_601]
    );
    let metrics: serde_json::Value = Client::new()
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(metrics["chunk_read"]["stream_windows"].as_u64().unwrap() > 0);
    assert!(metrics["chunk_small_write"]["completed"].as_u64().unwrap() > 0);

    let metadata = path(table, "metadata/b.json");
    let create = client.send(Method::POST, &metadata, "uploads=", b"", false).await;
    assert_eq!(create.status(), 200);
    let xml = create.text().await.unwrap();
    let upload = xml
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let mut document = b"{\"answer\":\"".to_vec();
    document.extend(std::iter::repeat_n(b'x', 5 * 1024 * 1024));
    document.extend_from_slice(b"\"}");
    let first = &document[..5 * 1024 * 1024];
    let second = &document[5 * 1024 * 1024..];
    let mut etags = Vec::new();
    for (number, bytes) in [(1, first), (2, second)] {
        let query = format!("partNumber={number}&uploadId={upload}");
        let response = client.send(Method::PUT, &metadata, &query, bytes, true).await;
        assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
        etags.push(response.headers()["etag"].to_str().unwrap().to_owned());
    }
    let listed = client
        .send(Method::GET, &metadata, &format!("uploadId={upload}"), b"", false)
        .await;
    assert_eq!(listed.status(), 200);
    assert!(listed
        .text()
        .await
        .unwrap()
        .contains("<PartNumber>2</PartNumber>"));
    let complete_xml = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Part><ETag>{}</ETag><PartNumber>1</PartNumber></Part><Part><ETag>{}</ETag><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>", etags[0], etags[1]);
    let complete = client
        .send(
            Method::POST,
            &metadata,
            &format!("uploadId={upload}"),
            complete_xml.as_bytes(),
            false,
        )
        .await;
    assert_eq!(complete.status(), 200, "{}", complete.text().await.unwrap());
    assert!(complete
        .text()
        .await
        .unwrap()
        .ends_with("</CompleteMultipartUploadResult>"));
    let mut composite = Md5::new();
    composite.update(Md5::digest(first));
    composite.update(Md5::digest(second));
    let mut expected_etag = String::with_capacity(34);
    for byte in composite.finalize() {
        write!(&mut expected_etag, "{byte:02x}").unwrap();
    }
    expected_etag.push_str("-2");
    let published = repository
        .load(
            client.credentials.grant().context,
            &table.file("metadata/b.json").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(published.content.etag(), Some(expected_etag.as_str()));
    assert_eq!(
        published
            .content
            .locations(published.length)
            .unwrap()
            .unwrap()
            .len(),
        2
    );
    let replay = client
        .send(
            Method::POST,
            &metadata,
            &format!("uploadId={upload}"),
            complete_xml.as_bytes(),
            false,
        )
        .await;
    assert_eq!(replay.status(), 200);
    assert!(replay
        .text()
        .await
        .unwrap()
        .ends_with("</CompleteMultipartUploadResult>"));
    let get = client.send(Method::GET, &metadata, "", b"", false).await;
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), document);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_put_size_matrix_streams_and_reads_ranges() {
    let (stack, _process, client, table) = setup_with_bounds_and_file_limit(
        ClearBounds {
            request_ms: 120_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
        128 * 1024 * 1024,
        128 * 1024 * 1024,
    )
    .await;
    let repository = FileRepository::new(stack.store().await);
    for size in [10 * 1024, 1024 * 1024, 12 * 1024 * 1024, 100 * 1024 * 1024] {
        let key = format!("data/size-{size}.parquet");
        let object = path(table, &key);
        let bytes = (0..size)
            .map(|offset| u8::try_from(offset % 256).unwrap())
            .collect::<Vec<_>>();
        let expected = Md5::digest(&bytes);
        let started = Instant::now();
        let put = client.send(Method::PUT, &object, "", &bytes, true).await;
        assert_eq!(put.status(), 200, "{}", put.text().await.unwrap());
        let put_elapsed = started.elapsed();
        let head = client.send(Method::HEAD, &object, "", b"", false).await;
        assert_eq!(head.status(), 200);
        assert_eq!(head.headers()["content-length"], size.to_string());
        let record = repository
            .load(client.credentials.grant().context, &table.file(&key).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.length, size as u64);
        let locations = record.content.locations(record.length).unwrap().unwrap();
        assert!(!locations.is_empty());
        if size == 12 * 1024 * 1024 {
            let chunkdb = make_chunkdb_client(stack.cluster.make_service_registry_client());
            let chunk = chunkdb
                .query_chunk(QueryChunkRequest {
                    chunk_id: locations[0].chunk_id,
                })
                .await
                .unwrap()
                .chunk
                .unwrap();
            let expected_copies = fixture_config().iceberg.large_mirror_copies.unwrap();
            assert!(chunk.strips.iter().any(|strip| strip.sealed_length > 0));
            for strip in &chunk.strips {
                let Some(Strip::MirrorStrip(mirror)) = strip.strip.as_ref() else {
                    panic!("single-node large Iceberg write has a mirror strip");
                };
                assert_eq!(mirror.segments.len(), usize::try_from(expected_copies).unwrap());
            }
        }
        let get_started = Instant::now();
        let mut response = client.send(Method::GET, &object, "", b"", false).await;
        assert_eq!(response.status(), 200);
        let mut received = Md5::new();
        let mut received_size = 0;
        while let Some(chunk) = response.chunk().await.unwrap() {
            received.update(&chunk);
            received_size += chunk.len();
        }
        assert_eq!(received_size, size);
        assert_eq!(received.finalize().as_slice(), expected.as_slice());
        let get_elapsed = get_started.elapsed();
        for start in [0, usize::min(size - 1, 65500), size - 1] {
            let end = usize::min(size - 1, start + 127);
            let range = format!("bytes={start}-{end}");
            let response = client
                .send_range(Method::GET, &object, "", b"", false, Some(&range))
                .await;
            assert_eq!(response.status(), 206);
            assert_eq!(response.bytes().await.unwrap().as_ref(), &bytes[start..=end]);
        }
        println!("iceberg ordinary size={size} PUT={put_elapsed:?} GET={get_elapsed:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_socket_resumes_upload_after_writer_drains() {
    const BLOCK_BYTES: usize = 1024 * 1024;
    let (_stack, _process, client, table) = setup_with_bounds_and_file_limit(
        ClearBounds {
            request_ms: 120_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
        16 * 1024 * 1024,
        16 * 1024 * 1024,
    )
    .await;
    let block = hyper::body::Bytes::from(vec![41; BLOCK_BYTES]);
    let mut md5 = Md5::new();
    md5.update(&block);
    md5.update(&block);
    let digest: [u8; 16] = md5.finalize().into();
    let object = path(table, "data/slow-socket.parquet");
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let request = client.request_stream(Method::PUT, &object, 2 * BLOCK_BYTES, digest, receiver);
    let upload = tokio::spawn(async move { request.send().await.unwrap() });
    sender.send(Ok(block.clone())).await.unwrap();
    // Let the first strip finish so no disk task remains to wake the upload.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    sender.send(Ok(block)).await.unwrap();
    drop(sender);
    let response = tokio::time::timeout(std::time::Duration::from_secs(30), upload)
        .await
        .expect("socket readiness must resume the idle upload")
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let get = client.send(Method::GET, &object, "", b"", false).await;
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().len(), 2 * BLOCK_BYTES);
    let metrics: serde_json::Value = Client::new()
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(metrics["upload_flow"]["body_waits"].as_u64().unwrap() > 0);
    assert!(metrics["upload_flow"]["body_wait_ns"].as_u64().unwrap() > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual 100 MiB upload profile"]
async fn repeated_100_mib_put_streams_without_client_payload_copy() {
    const BLOCK_BYTES: usize = 1024 * 1024;
    const BLOCK_COUNT: usize = 100;
    let (_stack, _process, client, table) = setup_with_bounds_and_file_limit(
        ClearBounds {
            request_ms: 120_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
        128 * 1024 * 1024,
        128 * 1024 * 1024,
    )
    .await;
    let block = hyper::body::Bytes::from(vec![37; BLOCK_BYTES]);
    let mut md5 = Md5::new();
    for _ in 0..BLOCK_COUNT {
        md5.update(&block);
    }
    let digest: [u8; 16] = md5.finalize().into();
    let object = path(table, "data/repeated-100-mib.parquet");
    let started = Instant::now();
    let response = client
        .send_repeated(Method::PUT, &object, block.clone(), BLOCK_COUNT, digest)
        .await;
    let put_elapsed = started.elapsed();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let metrics: serde_json::Value = Client::new()
        .get(format!("http://{}/_crowdb/metrics", client.address))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let upload_flow = &metrics["upload_flow"];
    assert_eq!(upload_flow["attempts"], 1);
    assert_eq!(upload_flow["completed"], 1);
    assert_eq!(upload_flow["logical_bytes"], BLOCK_BYTES * BLOCK_COUNT);
    assert!(upload_flow["strip_write_successes"].as_u64().unwrap() > 0);
    assert!(upload_flow["strip_write_success_ns"].as_u64().unwrap() > 0);
    let head = client.send(Method::HEAD, &object, "", b"", false).await;
    assert_eq!(head.status(), 200);
    assert_eq!(
        head.headers()["content-length"],
        (BLOCK_BYTES * BLOCK_COUNT).to_string()
    );
    let last = format!(
        "bytes={}-{}",
        BLOCK_BYTES * BLOCK_COUNT - 128,
        BLOCK_BYTES * BLOCK_COUNT - 1
    );
    let range = client
        .send_range(Method::GET, &object, "", b"", false, Some(&last))
        .await;
    assert_eq!(range.status(), 206);
    assert_eq!(range.bytes().await.unwrap().as_ref(), &[37; 128]);
    let invalid = path(table, "data/repeated-invalid-md5.parquet");
    let rejected = client
        .send_repeated(Method::PUT, &invalid, block, 1, [0; 16])
        .await;
    assert_eq!(rejected.status(), 400);
    assert!(rejected.text().await.unwrap().contains("BadDigest"));
    let absent = client.send(Method::HEAD, &invalid, "", b"", false).await;
    assert_eq!(absent.status(), 404);
    let sha_object = path(table, "data/signed-sha256.parquet");
    let sha = client
        .send(Method::PUT, &sha_object, "", b"sha256 payload", false)
        .await;
    assert_eq!(sha.status(), 200, "{}", sha.text().await.unwrap());
    println!("iceberg repeated 100 MiB PUT={put_elapsed:?}");
    println!("iceberg direct upload flow metrics={upload_flow}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual 100 MiB multipart upload profile"]
async fn repeated_100_mib_multipart_upload_profile() {
    const BLOCK_BYTES: usize = 1024 * 1024;
    const PART_COUNT: usize = 13;
    let (_stack, _process, client, table) = setup_with_bounds_and_file_limit(
        ClearBounds {
            request_ms: 120_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
        128 * 1024 * 1024,
        128 * 1024 * 1024,
    )
    .await;
    let block = hyper::body::Bytes::from(vec![37; BLOCK_BYTES]);
    let digests = [8, 4].map(|repetitions| {
        let mut md5 = Md5::new();
        for _ in 0..repetitions {
            md5.update(&block);
        }
        <[u8; 16]>::from(md5.finalize())
    });
    let object = path(table, "data/repeated-100-mib-multipart.parquet");
    let upload_started = Instant::now();
    let created = client.send(Method::POST, &object, "uploads=", b"", false).await;
    assert_eq!(created.status(), 200, "{}", created.text().await.unwrap());
    let created_body = created.text().await.unwrap();
    let upload = created_body
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let create_elapsed = upload_started.elapsed();
    let part_started = Instant::now();
    let parts = futures::stream::iter(1..=PART_COUNT)
        .map(|number| {
            let block = block.clone();
            let query = format!("partNumber={number}&uploadId={upload}");
            let object = object.clone();
            let repetitions = if number == PART_COUNT { 4 } else { 8 };
            let digest = if repetitions == 4 { digests[1] } else { digests[0] };
            let client = &client;
            async move {
                let started = Instant::now();
                let response = client
                    .send_repeated_with_query(Method::PUT, &object, &query, block, repetitions, digest)
                    .await;
                assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
                let etag = response.headers()["etag"].to_str().unwrap().to_owned();
                (number, etag, started.elapsed())
            }
        })
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;
    let parts_elapsed = part_started.elapsed();
    let mut parts = parts;
    parts.sort_unstable_by_key(|part| part.0);
    let mut manifest = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    for (number, etag, _) in &parts {
        write!(
            manifest,
            "<Part><ETag>{etag}</ETag><PartNumber>{number}</PartNumber></Part>"
        )
        .unwrap();
    }
    manifest.push_str("</CompleteMultipartUpload>");
    let complete_started = Instant::now();
    let completed = client
        .send(
            Method::POST,
            &object,
            &format!("uploadId={upload}"),
            manifest.as_bytes(),
            false,
        )
        .await;
    assert_eq!(completed.status(), 200, "{}", completed.text().await.unwrap());
    let completed_body = completed.text().await.unwrap();
    assert!(
        completed_body.contains("</CompleteMultipartUploadResult>"),
        "{completed_body}"
    );
    let complete_elapsed = complete_started.elapsed();
    let last = format!("bytes={}-{}", 100 * BLOCK_BYTES - 128, 100 * BLOCK_BYTES - 1);
    let range = client
        .send_range(Method::GET, &object, "", b"", false, Some(&last))
        .await;
    assert_eq!(range.status(), 206);
    assert_eq!(range.bytes().await.unwrap().as_ref(), &[37; 128]);
    println!(
        "iceberg 100 MiB multipart create={create_elapsed:?} parts={parts_elapsed:?} complete={complete_elapsed:?} slowest_part={:?}",
        parts.iter().map(|part| part.2).max().unwrap()
    );
    profiles::assert_multipart_metrics(&client, PART_COUNT, 100 * BLOCK_BYTES).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_routing_is_strict_at_the_strip_threshold() {
    let (_stack, _process, client, table) = setup().await;
    let threshold = fixture_config().iceberg_small_write().threshold_exclusive();
    let completed = async || {
        let metrics: serde_json::Value = Client::new()
            .get(format!("http://{}/_crowdb/metrics", client.address))
            .bearer_auth("m".repeat(32))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        metrics["chunk_small_write"]["completed"].as_u64().unwrap()
    };
    let mut previous = completed().await;
    for size in [threshold - 1, threshold, threshold + 1] {
        let object = path(table, &format!("data/threshold-{size}.parquet"));
        let bytes = vec![u8::try_from(size % 251).unwrap(); size];
        let response = client.send(Method::PUT, &object, "", &bytes, true).await;
        assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
        let current = completed().await;
        assert_eq!(current - previous, u64::from(size < threshold), "size={size}");
        previous = current;
        let last = format!("bytes={}-{}", size - 2, size - 1);
        let response = client
            .send_range(Method::GET, &object, "", b"", false, Some(&last))
            .await;
        assert_eq!(response.status(), 206);
        assert_eq!(response.bytes().await.unwrap().as_ref(), &bytes[size - 2..]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multipart_100_mib_survives_restart_and_complete_replay() {
    const PART_BYTES: usize = 5 * 1024 * 1024;
    const PART_COUNT: usize = 20;
    let (stack, process, mut client, table) = setup_with_bounds_and_file_limit(
        ClearBounds {
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
        128 * 1024 * 1024,
        128 * 1024 * 1024,
    )
    .await;
    let object = path(table, "data/large-multipart.parquet");
    let created = client.send(Method::POST, &object, "uploads=", b"", false).await;
    assert_eq!(created.status(), 200, "{}", created.text().await.unwrap());
    let created = created.text().await.unwrap();
    let upload = created
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let mut manifest = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    let mut composite = Md5::new();
    for first_number in (1..=PART_COUNT).step_by(2) {
        let second_number = first_number + 1;
        let first_bytes = vec![u8::try_from(first_number).unwrap(); PART_BYTES];
        let second_bytes = vec![u8::try_from(second_number).unwrap(); PART_BYTES];
        let first_query = format!("partNumber={first_number}&uploadId={upload}");
        let second_query = format!("partNumber={second_number}&uploadId={upload}");
        let (first_response, second_response) = tokio::join!(
            client.send(Method::PUT, &object, &first_query, &first_bytes, true),
            client.send(Method::PUT, &object, &second_query, &second_bytes, true),
        );
        for (number, bytes, response) in [
            (first_number, first_bytes, first_response),
            (second_number, second_bytes, second_response),
        ] {
            assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
            let etag = response.headers()["etag"].to_str().unwrap();
            composite.update(Md5::digest(&bytes));
            write!(
                manifest,
                "<Part><ETag>{etag}</ETag><PartNumber>{number}</PartNumber></Part>"
            )
            .unwrap();
        }
    }
    manifest.push_str("</CompleteMultipartUpload>");
    let mut expected_etag = String::new();
    for byte in composite.finalize() {
        write!(expected_etag, "{byte:02x}").unwrap();
    }
    expected_etag.push_str("-20");
    let query = format!("uploadId={upload}");
    let completed = client
        .send(Method::POST, &object, &query, manifest.as_bytes(), false)
        .await;
    assert_eq!(completed.status(), 200);
    let completed = completed.text().await.unwrap();
    assert!(
        completed.contains("</CompleteMultipartUploadResult>"),
        "{completed}"
    );
    let repository = FileRepository::new(stack.store().await);
    let record = repository
        .load(
            client.credentials.grant().context,
            &table.file("data/large-multipart.parquet").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.length, (PART_BYTES * PART_COUNT) as u64);
    assert_eq!(record.content.etag(), Some(expected_etag.as_str()));
    assert_eq!(
        record.content.locations(record.length).unwrap().unwrap().len(),
        PART_COUNT
    );

    drop(process);
    let restarted = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    client.address = restarted.address;
    let replay = client
        .send(Method::POST, &object, &query, manifest.as_bytes(), false)
        .await;
    assert_eq!(replay.status(), 200, "{}", replay.text().await.unwrap());
    let response = client.send(Method::GET, &object, "", b"", false).await;
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let bytes = response.bytes().await.unwrap();
    assert_eq!(bytes.len(), PART_BYTES * PART_COUNT);
    for (index, part) in bytes.chunks_exact(PART_BYTES).enumerate() {
        assert!(part.iter().all(|byte| *byte == u8::try_from(index + 1).unwrap()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Maven and the pinned Apache Iceberg Java dependencies"]
async fn official_java_s3_fileio_uploads_and_reads_native_files() {
    use base64::Engine as _;
    use crowdb_access_iceberg::wire::LoadCredentialsResponse;
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    let (_stack, _process, client, table) = setup().await;
    let response = serde_json::to_vec(&LoadCredentialsResponse::from(client.credentials)).unwrap();
    let configuration = format!(
        "endpoint=http://{}\ncredentials={}\nlocation={}\n",
        client.address,
        base64::engine::general_purpose::STANDARD.encode(response),
        table
            .file("placeholder")
            .unwrap()
            .to_string()
            .trim_end_matches("placeholder"),
    );
    let status = tokio::task::spawn_blocking(move || {
        let maven = std::env::var_os("CROWDB_ICEBERG_E2E_MVN").unwrap_or_else(|| "mvn".into());
        let mut child = Command::new("timeout")
            .arg("600")
            .arg(maven)
            .args(["--batch-mode", "--no-transfer-progress", "-f"])
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_java/pom.xml"
            ))
            .args(["compile", "exec:java"])
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(configuration.as_bytes())
            .unwrap();
        child.wait().unwrap()
    })
    .await
    .unwrap();
    assert!(status.success(), "official Apache Iceberg S3FileIO failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native storage services, Maven and pinned Apache Iceberg dependencies"]
async fn official_java_catalog_commits_native_parquet_snapshots_and_staged_tables() {
    let (stack, process, _, _) = setup_with_bounds(ClearBounds {
        request_ms: 300_000,
        delegated_access_ms: 900_000,
        ..ClearBounds::default()
    })
    .await;
    let endpoint = format!("http://{}", process.address);
    run_catalog_sdk(endpoint, "data").await;
    drop(process);
    let restarted = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    run_catalog_sdk(format!("http://{}", restarted.address), "verify").await;
}

async fn run_catalog_sdk(endpoint: String, mode: &'static str) {
    run_sdk(endpoint, "TestIcebergCatalogWrites", mode).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native storage services, Maven and pinned Apache Iceberg dependencies"]
async fn official_java_opaque_metadata_publication_preserves_valid_data_and_delete_reads() {
    let (_stack, process, _, _) = setup_with_bounds(ClearBounds {
        request_ms: 300_000,
        delegated_access_ms: 900_000,
        ..ClearBounds::default()
    })
    .await;
    let endpoint = format!("http://{}", process.address);
    run_sdk(endpoint, "TestIcebergSelectedFiles", "").await;
}

async fn run_sdk(endpoint: String, class: &'static str, mode: &'static str) {
    let status = tokio::task::spawn_blocking(move || {
        let maven = std::env::var_os("CROWDB_ICEBERG_E2E_MVN").unwrap_or_else(|| "mvn".into());
        std::process::Command::new("timeout")
            .arg("600")
            .arg(maven)
            .args(["-o", "--batch-mode", "--no-transfer-progress", "-f"])
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_java/pom.xml"
            ))
            .args(["compile", "exec:java"])
            .arg(format!("-Dexec.mainClass={class}"))
            .arg(format!("-Dexec.args={endpoint} {mode}"))
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        status.success(),
        "official native catalog and Parquet acceptance failed"
    );
}
