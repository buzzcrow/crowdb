#[path = "common/iceberg_stack.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/iceberg_process.rs"]
#[allow(dead_code)]
mod process;

use std::fmt::Write;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::{now_ms, TestIcebergStack};
use crowdb_access_iceberg::catalog::{CatalogRepository, ClearBounds, ManagementPrivilege};
use crowdb_access_iceberg::file::{
    FileGrant, FileGrantIssuer, FileKind, FileOperation, FileOperations, FileRepository, TableLocation,
};
use crowdb_access_iceberg::key::{OperationId, TableId};
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::wire::BearerAuthenticator;
use hmac::{Hmac, Mac};
use md5::Md5;
use reqwest::{Client, Method, Response};
use sha2::{Digest, Sha256};

fn hex(bytes: &[u8]) -> String {
    let mut result = String::new();
    for byte in bytes {
        write!(result, "{byte:02x}").unwrap();
    }
    result
}

fn mac(key: &[u8], input: &str) -> Vec<u8> {
    let mut signer = Hmac::<Sha256>::new_from_slice(key).unwrap();
    signer.update(input.as_bytes());
    signer.finalize().into_bytes().to_vec()
}

struct TestFileClient {
    client: Client,
    credentials: crowdb_access_iceberg::file::FileCredentials,
    address: std::net::SocketAddr,
}

impl TestFileClient {
    async fn send(&self, method: Method, path: &str, query: &str, body: &[u8], md5: bool) -> Response {
        self.send_range(method, path, query, body, md5, None).await
    }

    async fn send_range(
        &self,
        method: Method,
        path: &str,
        query: &str,
        body: &[u8],
        md5: bool,
        range: Option<&str>,
    ) -> Response {
        let now =
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(i64::try_from(now_ms()).unwrap()).unwrap();
        let date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let short = now.format("%Y%m%d").to_string();
        let hash = hex(&Sha256::digest(body));
        let host = self.address.to_string();
        let names = "host;x-amz-content-sha256;x-amz-date;x-amz-security-token";
        let canonical = format!(
            "{}\n{path}\n{query}\nhost:{host}\nx-amz-content-sha256:{hash}\nx-amz-date:{date}\nx-amz-security-token:{}\n\n{names}\n{hash}",
            method.as_str(), self.credentials.session_token()
        );
        let date_key = mac(
            format!("AWS4{}", self.credentials.secret_access_key()).as_bytes(),
            &short,
        );
        let region_key = mac(&date_key, "us-east-1");
        let service_key = mac(&region_key, "s3");
        let signing_key = mac(&service_key, "aws4_request");
        let scope = format!("{short}/us-east-1/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
            hex(&Sha256::digest(canonical))
        );
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={names}, Signature={}",
            self.credentials.access_key_id(),
            hex(&mac(&signing_key, &string_to_sign))
        );
        let url = if query.is_empty() {
            format!("http://{host}{path}")
        } else {
            format!("http://{host}{path}?{query}")
        };
        let mut request = self
            .client
            .request(method, url)
            .header("host", host)
            .header("x-amz-content-sha256", hash)
            .header("x-amz-date", date)
            .header("x-amz-security-token", self.credentials.session_token())
            .header("authorization", authorization)
            .body(body.to_vec());
        if md5 {
            request = request.header("content-md5", STANDARD.encode(Md5::digest(body)));
        }
        if let Some(range) = range {
            request = request.header("range", range);
        }
        request.send().await.unwrap()
    }
}

async fn setup() -> (
    TestIcebergStack,
    process::TestIcebergProcess,
    TestFileClient,
    TableLocation,
) {
    let stack = TestIcebergStack::start().await;
    let repository = CatalogRepository::new(stack.store().await, ClearBounds::default()).unwrap();
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
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    let (_stack, _process, client, table) = setup().await;
    let configuration = format!(
        "endpoint=http://{}\naccess={}\nsecret={}\ntoken={}\nlocation={}\n",
        client.address,
        client.credentials.access_key_id(),
        client.credentials.secret_access_key(),
        client.credentials.session_token(),
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
