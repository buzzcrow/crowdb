#![cfg(feature = "iceberg")]

#[path = "common/iceberg_store.rs"]
mod common;

use crowdb_access_iceberg::catalog::{CatalogRepository, CatalogStore, ClearBounds, ManagementPrivilege};
use crowdb_access_iceberg::key::{OperationId, SystemScope};
use crowdb_access_iceberg::operation::{ledger_key, ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::wire::BearerAuthenticator;
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn setup() -> (
    Arc<common::TestStore>,
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let store = Arc::new(common::TestStore::default());
    let repository = Arc::new(CatalogRepository::new(store.clone(), ClearBounds::default()).unwrap());
    repository
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: 100,
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "catalog".into(),
                confirmation: None,
                capabilities: None,
            },
            ManagementPrivilege::Manage,
            100,
        )
        .await
        .unwrap();
    common::activate(&repository).await;
    let auth =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let service = Arc::new(
        IcebergHttpService::new(repository, auth, Duration::from_secs(2))
            .with_namespaces(store.clone())
            .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        serve(listener, service, async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    (store, address, stop, server)
}

async fn fresh_key(store: &common::TestStore) -> String {
    for _ in 0..32 {
        let mut bytes = *OperationId::random().as_bytes();
        let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap();
        bytes[..6].copy_from_slice(&now.to_be_bytes()[2..]);
        bytes[6] = (bytes[6] & 15) | 0x70;
        bytes[8] = (bytes[8] & 63) | 0x80;
        let operation = OperationId::from_bytes(&bytes).unwrap();
        if store
            .get(
                &ledger_key(SystemScope::RetryBinding, operation)
                    .unwrap()
                    .encode()
                    .unwrap(),
            )
            .await
            .unwrap()
            .is_some()
        {
            continue;
        }
        let hex = operation.to_string();
        return format!(
            "{}-{}-{}-{}-{}",
            &hex[..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..]
        );
    }
    panic!("no free retry fixture slot");
}

async fn send(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    principal: &str,
    key: Option<&str>,
    body: &str,
) -> (u16, String) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let idempotency = key.map_or_else(String::new, |key| format!("Idempotency-Key: {key}\r\n"));
    stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{idempotency}Connection: close\r\n\r\n{body}", principal.repeat(32), body.len()).as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.unwrap();
    let response = String::from_utf8(bytes).unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    (
        headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        body.to_owned(),
    )
}

#[tokio::test]
async fn namespace_writes_use_distinct_credentials_and_replay_success_and_terminal_errors() {
    let (store, address, stop, server) = setup().await;
    let body = r#"{"namespace":["parent"],"properties":{"owner":"original"}}"#;
    let before = store.values.load().len();
    for principal in ["r", "m", "c"] {
        assert_eq!(
            send(address, "POST", "/v1/namespaces", principal, None, body)
                .await
                .0,
            403
        );
    }
    assert_eq!(store.values.load().len(), before);
    let key = fresh_key(&store).await;
    let created = send(address, "POST", "/v1/namespaces", "w", Some(&key), body).await;
    assert_eq!(created.0, 200);
    assert_eq!(
        send(address, "POST", "/v1/namespaces", "w", Some(&key), body).await,
        created
    );
    assert_eq!(
        send(
            address,
            "POST",
            "/v1/namespaces",
            "w",
            Some(&key),
            r#"{"namespace":["different"]}"#
        )
        .await
        .0,
        409
    );
    let duplicate_key = fresh_key(&store).await;
    let duplicate = send(address, "POST", "/v1/namespaces", "w", Some(&duplicate_key), body).await;
    assert_eq!(duplicate.0, 409);
    verify_property_replay(&store, address).await;
    verify_drop_replay(&store, address, &duplicate_key, &duplicate, body).await;
    stop.send(()).unwrap();
    server.await.unwrap();
}

async fn verify_property_replay(store: &common::TestStore, address: std::net::SocketAddr) {
    let update_key = fresh_key(store).await;
    let changes = r#"{"removals":["owner","missing"],"updates":{"value":"kept"}}"#;
    let updated = send(
        address,
        "POST",
        "/v1/namespaces/parent/properties",
        "w",
        Some(&update_key),
        changes,
    )
    .await;
    assert_eq!(updated.0, 200);
    assert_eq!(
        send(
            address,
            "POST",
            "/v1/namespaces/parent/properties",
            "w",
            Some(&update_key),
            changes
        )
        .await,
        updated
    );
    assert_eq!(
        send(
            address,
            "POST",
            "/v1/namespaces/parent/properties",
            "w",
            None,
            r#"{"removals":["value"],"updates":{"value":"bad"}}"#
        )
        .await
        .0,
        422
    );
}

async fn verify_drop_replay(
    store: &common::TestStore,
    address: std::net::SocketAddr,
    duplicate_key: &str,
    duplicate: &(u16, String),
    body: &str,
) {
    let drop_key = fresh_key(store).await;
    assert_eq!(
        send(
            address,
            "DELETE",
            "/v1/namespaces/parent",
            "w",
            Some(&drop_key),
            ""
        )
        .await
        .0,
        204
    );
    assert_eq!(
        send(address, "POST", "/v1/namespaces", "w", Some(duplicate_key), body).await,
        *duplicate
    );
    assert_eq!(
        send(address, "POST", "/v1/namespaces", "w", None, body).await.0,
        200
    );
    assert_eq!(
        send(
            address,
            "DELETE",
            "/v1/namespaces/parent",
            "w",
            Some(&drop_key),
            ""
        )
        .await
        .0,
        204
    );
    assert_eq!(
        send(address, "GET", "/v1/namespaces/parent", "r", None, "")
            .await
            .0,
        200
    );
}

#[tokio::test]
async fn malformed_and_missing_parent_results_are_retained_before_any_later_retry() {
    let (store, address, stop, server) = setup().await;
    let key = fresh_key(&store).await;
    let body = r#"{"namespace":["missing","child"]}"#;
    let failed = send(address, "POST", "/v1/namespaces", "w", Some(&key), body).await;
    assert_eq!(failed.0, 400);
    assert_eq!(
        send(
            address,
            "POST",
            "/v1/namespaces",
            "w",
            None,
            r#"{"namespace":["missing"]}"#
        )
        .await
        .0,
        200
    );
    assert_eq!(
        send(address, "POST", "/v1/namespaces", "w", Some(&key), body).await,
        failed
    );
    let invalid_key = fresh_key(&store).await;
    let malformed = send(address, "POST", "/v1/namespaces", "w", Some(&invalid_key), "{").await;
    assert_eq!(malformed.0, 400);
    assert_eq!(
        send(address, "POST", "/v1/namespaces", "w", Some(&invalid_key), "{").await,
        malformed
    );
    assert_eq!(
        send(address, "POST", "/v1/namespaces", "w", Some("invalid"), "{}")
            .await
            .0,
        400
    );
    assert_eq!(
        send(address, "POST", "/v1/namespaces", "w", None, body).await.0,
        200
    );
    assert_eq!(
        send(address, "DELETE", "/v1/namespaces/missing", "w", None, "")
            .await
            .0,
        409
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn server_errors_leave_recoverable_publication_and_large_final_responses() {
    let (store, address, stop, server) = setup().await;
    let properties: std::collections::BTreeMap<_, _> = (0..7)
        .map(|index| (format!("key{index}"), "v".repeat(8190)))
        .collect();
    for mode in [1, 2, 3] {
        let key = fresh_key(&store).await;
        let namespace = format!("lost-{mode}");
        let body =
            serde_json::to_string(&serde_json::json!({"namespace": [namespace], "properties": properties}))
                .unwrap();
        store
            .lose_reply_kind
            .store(mode, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            send(address, "POST", "/v1/namespaces", "w", Some(&key), &body)
                .await
                .0,
            503
        );
        let resumed = send(address, "POST", "/v1/namespaces", "w", Some(&key), &body).await;
        assert_eq!(resumed.0, 200);
        assert!(resumed.1.len() > 16 * 1024);
        assert_eq!(
            send(address, "POST", "/v1/namespaces", "w", Some(&key), &body).await,
            resumed
        );
        assert_eq!(
            send(
                address,
                "GET",
                &format!("/v1/namespaces/{namespace}"),
                "r",
                None,
                ""
            )
            .await
            .0,
            200
        );
    }
    stop.send(()).unwrap();
    server.await.unwrap();
}
