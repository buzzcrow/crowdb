#[path = "common/iceberg_commit_recovery/blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_commit_recovery/fixture.rs"]
#[allow(dead_code, unused_imports)]
mod common;
#[path = "common/iceberg_store.rs"]
mod memory;
#[path = "common/iceberg_signed_file.rs"]
#[allow(dead_code)]
mod signed;
#[path = "common/iceberg_commit_recovery/store.rs"]
mod store;

use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds, ManagementPrivilege},
    file::{
        DirectFileLocations, FileGrant, FileGrantIssuer, FileOperation, FileOperations, FileRepository,
        TableLocation,
    },
    key::{NamespaceId, OperationId, TableId},
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
    wire::BearerAuthenticator,
};
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use reqwest::Method;
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

fn authentication() -> BearerAuthenticator {
    BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap()
}

fn rotated_authentication() -> BearerAuthenticator {
    BearerAuthenticator::new(&"a".repeat(32), &"b".repeat(32), &"d".repeat(32), &"e".repeat(32)).unwrap()
}

async fn rest(
    address: std::net::SocketAddr,
    method: Method,
    path: &str,
    body: serde_json::Value,
    serial: u64,
) -> serde_json::Value {
    let now = common::now_ms();
    let identity = format!("{:08x}-{:04x}-7000-8000-{serial:012x}", now >> 16, now & 0xffff);
    let request = reqwest::Client::new()
        .request(method, format!("http://{address}{path}"))
        .bearer_auth("w".repeat(32))
        .header("idempotency-key", identity);
    let request = if body.is_null() {
        request
    } else {
        request.json(&body)
    };
    let response = request.send().await.unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "{path}: {status}: {text}");
    if text.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&text).unwrap()
    }
}

#[tokio::test]
async fn full_storage_identity_refresh_survives_move_and_default_namespace_recreation() {
    use serde_json::json;
    let (_stack, _repository, _table, client, server) = setup().await;
    let create = json!({"name":"events","schema":{"type":"struct","schema-id":0,
        "fields":[{"id":1,"name":"id","type":"long","required":true}]}});
    let initial = rest(
        client.address,
        Method::POST,
        "/v1/namespaces/default/tables",
        create.clone(),
        2,
    )
    .await;
    let original = initial["metadata"]["location"].as_str().unwrap();
    let refresh = initial["config"]["client.refresh-credentials-endpoint"]
        .as_str()
        .unwrap();
    assert!(refresh.contains("storage-namespace-id="));
    rest(
        client.address,
        Method::POST,
        "/v1/namespaces",
        json!({"namespace":["dest"]}),
        3,
    )
    .await;
    rest(
        client.address,
        Method::POST,
        "/v1/tables/rename",
        json!({
        "source":{"namespace":["default"],"name":"events"},
        "destination":{"namespace":["dest"],"name":"renamed"}}),
        4,
    )
    .await;
    rest(
        client.address,
        Method::DELETE,
        "/v1/namespaces/default",
        json!(null),
        5,
    )
    .await;
    let recreated = rest(
        client.address,
        Method::POST,
        "/v1/namespaces/default/tables",
        create,
        6,
    )
    .await;
    assert_ne!(recreated["metadata"]["location"].as_str().unwrap(), original);
    let response = reqwest::Client::new()
        .get(format!("http://{}{refresh}", client.address))
        .bearer_auth("r".repeat(32))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(status, 200, "{text}");
    let refreshed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        refreshed["storage-credentials"][0]["prefix"]
            .as_str()
            .unwrap()
            .trim_end_matches('/'),
        original.trim_end_matches('/')
    );
    let moved = reqwest::Client::new()
        .get(format!(
            "http://{}/v1/namespaces/dest/tables/renamed",
            client.address
        ))
        .bearer_auth("r".repeat(32))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(moved["metadata"]["location"].as_str().unwrap(), original);
    server.abort();
    let _ = server.await;
}

async fn listen(
    stack: &common::TestCommitFixture,
    repository: Arc<CatalogRepository>,
    rotate: bool,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let auth = if rotate {
        rotated_authentication()
    } else {
        authentication()
    };
    let mut service = IcebergHttpService::new(repository, auth, Duration::from_secs(300))
        .with_namespaces(stack.store())
        .unwrap()
        .with_fileio(stack.store(), stack.blocks.clone(), "us-east-1".into())
        .unwrap()
        .with_tables(stack.store(), stack.blocks.clone())
        .unwrap();
    if rotate {
        service = service
            .with_direct_file_locations(DirectFileLocations::new(2, BTreeMap::from([(2, [4; 32])])).unwrap())
            .unwrap();
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    service = service
        .with_table_credentials(stack.store(), format!("http://{address}"))
        .unwrap();
    let task = tokio::spawn(async move {
        serve(listener, Arc::new(service), std::future::pending())
            .await
            .unwrap();
    });
    (address, task)
}

async fn setup() -> (
    common::TestCommitFixture,
    Arc<CatalogRepository>,
    TableLocation,
    signed::TestFileClient,
    tokio::task::JoinHandle<()>,
) {
    let stack = common::TestCommitFixture::new();
    let repository = Arc::new(
        CatalogRepository::new(
            stack.store(),
            ClearBounds {
                request_ms: 300_000,
                delegated_access_ms: 900_000,
                ..ClearBounds::default()
            },
        )
        .unwrap(),
    );
    let now = common::now_ms();
    repository
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: now,
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "native".into(),
                confirmation: None,
                capabilities: None,
            },
            ManagementPrivilege::Manage,
            now,
        )
        .await
        .unwrap();
    common::activate(&repository).await;
    assert_eq!(
        repository.status().await.unwrap().1.storage_layout,
        crowdb_access_iceberg::catalog::CatalogStorageLayout::Native
    );
    let context = repository.status().await.unwrap().0.context;
    let table = TableLocation {
        catalog: context.catalog,
        table: TableId::random(),
        storage_namespace: Some(NamespaceId::random()),
    };
    let credentials = FileGrantIssuer::new(authentication().namespace_token_key(), 900_000)
        .unwrap()
        .issue(FileGrant {
            context,
            storage_namespace: table.storage_namespace,
            table: table.table,
            principal: [3; 32],
            nonce: OperationId::random(),
            issued_ms: now,
            expires_ms: now + 900_000,
            max_request_bytes: 1024 * 1024,
            max_file_bytes: 1024 * 1024,
            operations: FileOperations::new(&[
                FileOperation::Put,
                FileOperation::Get,
                FileOperation::Head,
                FileOperation::ListObjects,
            ])
            .unwrap(),
        })
        .unwrap();
    let (address, server) = listen(&stack, repository.clone(), false).await;
    let client = signed::TestFileClient {
        client: reqwest::Client::new(),
        credentials,
        address,
    };
    (stack, repository, table, client, server)
}

async fn assert_full_retry(
    client: &signed::TestFileClient,
    path: &str,
    query: &str,
    bytes: &[u8],
    previous_uri: &str,
) -> crowdb_access_iceberg::file::FileLocation {
    let replay = client.send(Method::PUT, path, query, bytes, false).await;
    assert_eq!(replay.status(), 200);
    let repeated: serde_json::Value = replay.json().await.unwrap();
    let repeated_uri = repeated["location"].as_str().unwrap();
    assert_ne!(repeated_uri, previous_uri);
    let repeated_location = repeated_uri
        .parse::<crowdb_access_iceberg::file::FileLocation>()
        .unwrap();
    let repeated_bytes = client
        .send(
            Method::GET,
            &format!("/iceberg/{}", repeated_location.object_key()),
            "",
            b"",
            false,
        )
        .await;
    assert_eq!(repeated_bytes.status(), 200);
    assert_eq!(repeated_bytes.bytes().await.unwrap(), bytes);
    repeated_location
}

async fn list_native_files(client: &signed::TestFileClient, table: TableLocation) -> String {
    let response = client
        .send(
            Method::GET,
            "/iceberg",
            &format!("list-type=2&prefix={}", table.storage_path().unwrap()),
            b"",
            false,
        )
        .await;
    assert_eq!(response.status(), 200);
    response.text().await.unwrap()
}

#[tokio::test]
async fn direct_http_reads_skip_file_kv_after_upload_restart_rotation_and_logical_delete() {
    let (stack, repository, table, mut client, server) = setup().await;
    let context = repository.status().await.unwrap().0.context;
    let now = common::now_ms();
    let identity = format!("{:08x}-{:04x}-7000-8000-000000000001", now >> 16, now & 0xffff);
    let path = format!("/iceberg/{}data/part.parquet", table.storage_path().unwrap());
    let query = format!("crowdb-native-upload={identity}");
    let bytes = b"PAR1datafoot\x04\0\0\0PAR1";
    let response = client.send(Method::PUT, &path, &query, bytes, false).await;
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    let uri = serde_json::from_str::<serde_json::Value>(&body).unwrap()["location"]
        .as_str()
        .unwrap()
        .to_owned();
    let location = uri.parse::<crowdb_access_iceberg::file::FileLocation>().unwrap();
    let direct = format!("/iceberg/{}", location.object_key());
    assert!(location.relative_key().starts_with("f/v1/part."));
    let repeated = assert_full_retry(&client, &path, &query, bytes, &uri).await;
    let file = FileRepository::new(stack.store())
        .load(context, &location)
        .await
        .unwrap()
        .unwrap();
    server.abort();
    let _ = server.await;
    let (address, server) = listen(&stack, repository, true).await;
    client.address = address;
    let mut grant = client.credentials.grant().clone();
    grant.nonce = OperationId::random();
    client.credentials = FileGrantIssuer::new(rotated_authentication().namespace_token_key(), 900_000)
        .unwrap()
        .issue(grant)
        .unwrap();
    let kv = stack.store();
    kv.native_file_reads.store(0, Ordering::SeqCst);
    kv.deny_native_file_reads.store(true, Ordering::SeqCst);
    assert_eq!(
        client
            .send(Method::HEAD, "/iceberg", "", b"", false)
            .await
            .status(),
        200
    );
    let head = client.send(Method::HEAD, &direct, "", b"", false).await;
    assert_eq!(head.status(), 200);
    assert_eq!(head.headers()["content-length"], bytes.len().to_string());
    let tag = head.headers()["etag"].to_str().unwrap().to_owned();
    assert_conditional_ranges(&client, &direct, &tag).await;
    let list = list_native_files(&client, table).await;
    assert!(list.contains(&location.object_key()), "{list}");
    assert!(list.contains(&repeated.object_key()), "{list}");
    assert_eq!(kv.native_file_reads.load(Ordering::SeqCst), 0);
    kv.deny_native_file_reads.store(false, Ordering::SeqCst);
    FileRepository::new(kv.clone())
        .mark_deleted(context, &file, common::now_ms())
        .await
        .unwrap();
    kv.native_file_reads.store(0, Ordering::SeqCst);
    kv.deny_native_file_reads.store(true, Ordering::SeqCst);
    let response = client.send(Method::GET, &direct, "", b"", false).await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.unwrap(), bytes.as_slice());
    let list = list_native_files(&client, table).await;
    assert!(!list.contains(&location.object_key()), "{list}");
    assert!(list.contains(&repeated.object_key()), "{list}");
    assert_eq!(kv.native_file_reads.load(Ordering::SeqCst), 0);
    let crowdb_access_iceberg::file::FileContent::Chunks { root: Some(root) } = &file.content else {
        panic!()
    };
    stack.blocks.remove_root(root);
    for method in [Method::HEAD, Method::GET] {
        let response = client
            .request(method, &direct, "", b"", false, None)
            .header("If-None-Match", &tag)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 404);
    }
    assert_eq!(kv.native_file_reads.load(Ordering::SeqCst), 0);
    server.abort();
    let _ = server.await;
}

async fn assert_conditional_ranges(client: &signed::TestFileClient, direct: &str, tag: &str) {
    let response = client
        .send_range(Method::GET, direct, "", b"", false, Some("bytes=-4"))
        .await;
    assert_eq!(response.status(), 206);
    assert_eq!(response.bytes().await.unwrap(), b"PAR1".as_slice());
    let response = client
        .request(Method::GET, direct, "", b"", false, None)
        .header("If-None-Match", tag)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 304);
    let response = client
        .request(Method::GET, direct, "", b"", false, None)
        .header("If-Match", "\"wrong\"")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 412);
}
