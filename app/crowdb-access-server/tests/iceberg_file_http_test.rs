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
#[path = "common/iceberg_file_recovery.rs"]
mod recovery;

use common::{now_ms, TestIcebergStack};
use crowdb_access_iceberg::catalog::{CatalogRepository, ClearBounds, ManagementPrivilege};
use crowdb_access_iceberg::file::{
    FileGrant, FileGrantIssuer, FileKind, FileOperation, FileOperations, FileRepository, TableLocation,
};
use crowdb_access_iceberg::key::{OperationId, TableId};
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::wire::BearerAuthenticator;
use reqwest::{Client, Method};

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
    setup_with_bounds(ClearBounds::default()).await
}

async fn setup_with_bounds(
    bounds: ClearBounds,
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
            },
            ManagementPrivilege::Manage,
            now_ms(),
        )
        .await
        .unwrap();
    let context = repository.status().await.unwrap().0.context;
    let table = TableLocation {
        catalog: context.catalog,
        table: TableId::random(),
    };
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
                FileOperation::Put,
                FileOperation::CreateMultipart,
                FileOperation::UploadPart,
                FileOperation::ListParts,
                FileOperation::CompleteMultipart,
                FileOperation::AbortMultipart,
            ])
            .unwrap(),
            max_request_bytes: 16 * 1024 * 1024,
            max_file_bytes: 64 * 1024 * 1024,
        })
        .unwrap();
    let process = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let client = TestFileClient {
        client: Client::new(),
        credentials,
        address: process.address,
    };
    (stack, process, client, table)
}

fn path(table: TableLocation, key: &str) -> String {
    format!("/{}/{}", table.bucket(), table.file(key).unwrap().object_key())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        .send(Method::PUT, &object, "", b"PAR1difffoot\x04\0\0\0PAR1", false)
        .await;
    assert_eq!(conflict.status(), 409);

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
    let response = Client::new()
        .post(format!("{endpoint}/v1/namespaces"))
        .bearer_auth("w".repeat(32))
        .header("content-type", "application/json")
        .body(r#"{"namespace":["analytics"]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
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
async fn official_java_identical_s3_uploads_validate_selected_data_and_delete_uses() {
    let (_stack, process, _, _) = setup_with_bounds(ClearBounds {
        request_ms: 300_000,
        delegated_access_ms: 900_000,
        ..ClearBounds::default()
    })
    .await;
    let endpoint = format!("http://{}", process.address);
    let response = Client::new()
        .post(format!("{endpoint}/v1/namespaces"))
        .bearer_auth("w".repeat(32))
        .header("content-type", "application/json")
        .body(r#"{"namespace":["analytics"]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
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
