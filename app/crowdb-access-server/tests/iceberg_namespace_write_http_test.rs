#[path = "common/iceberg_store.rs"]
mod common;

use async_trait::async_trait;
use crowdb_access_iceberg::catalog::{
    CasOutcome, CatalogRepository, CatalogStore, ClearBounds, ManagementPrivilege, StoreError, StoredValue,
};
use crowdb_access_iceberg::key::{IcebergKey, NamespaceId, OperationId, SystemScope};
use crowdb_access_iceberg::namespace::{ChildScan, NamespaceMappingState, NamespaceStore};
use crowdb_access_iceberg::operation::{ledger_key, ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::record::StorageRecord;
use crowdb_access_iceberg::wire::BearerAuthenticator;
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;
use std::sync::atomic::Ordering;
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
            .with_namespaces(Arc::new(TestNamespaceReplyStore { inner: store.clone() }))
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

struct TestNamespaceReplyStore {
    inner: Arc<common::TestStore>,
}

#[async_trait]
impl CatalogStore for TestNamespaceReplyStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        let result = self
            .inner
            .compare_exchange(key, expected, value, identity)
            .await?;
        // The shared fixture's third mode targets a historical create receipt.
        // Lose the acknowledgement of the final name publication instead.
        if matches!(&result, CasOutcome::Applied(_))
            && matches!(IcebergKey::decode(key).and_then(|key| StorageRecord::decode(&key, value)), Ok(StorageRecord::NamespaceMapping(mapping)) if mapping.state == NamespaceMappingState::Published)
            && self
                .inner
                .lose_reply_kind
                .compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(StoreError::Response);
        }
        Ok(result)
    }
}

#[async_trait]
impl NamespaceStore for TestNamespaceReplyStore {
    async fn scan_children(&self, scan: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_children(scan).await
    }
    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete_mapping(key, expected, identity).await
    }
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
        return wire_key(operation);
    }
    panic!("no free retry fixture slot");
}

fn wire_key(operation: OperationId) -> String {
    let hex = operation.to_string();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
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
async fn namespace_writes_use_distinct_credentials_and_current_resources_without_create_receipts() {
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
        serde_json::from_str::<serde_json::Value>(&created.1).unwrap(),
        serde_json::from_str::<serde_json::Value>(body).unwrap()
    );
    let original = namespace_id(&store, "parent");
    assert_already_exists(&send(address, "POST", "/v1/namespaces", "w", Some(&key), body).await);
    assert_eq!(namespace_id(&store, "parent"), original);
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
        200
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&namespace_response(address, "different").await.1).unwrap(),
        serde_json::json!({"namespace": ["different"], "properties": {}})
    );
    let duplicate_key = fresh_key(&store).await;
    let duplicate = send(address, "POST", "/v1/namespaces", "w", Some(&duplicate_key), body).await;
    assert_already_exists(&duplicate);
    verify_property_replay(&store, address).await;
    verify_drop_replay(&store, address, &duplicate_key, body).await;
    stop.send(()).unwrap();
    server.await.unwrap();
}

fn assert_already_exists(response: &(u16, String)) {
    assert_eq!(response.0, 409);
    let body: serde_json::Value = serde_json::from_str(&response.1).unwrap();
    assert_eq!(body["error"]["code"], 409);
    assert_eq!(body["error"]["type"], "AlreadyExistsException");
}

fn namespace_id(store: &common::TestStore, name: &str) -> NamespaceId {
    store
        .values
        .load()
        .iter()
        .find_map(|(key, value)| {
            match IcebergKey::decode(key).and_then(|key| StorageRecord::decode(&key, &value.bytes)) {
                Ok(StorageRecord::NamespaceMapping(mapping))
                    if mapping.parent.is_none() && mapping.name == name =>
                {
                    Some(mapping.namespace)
                }
                _ => None,
            }
        })
        .unwrap()
}

async fn namespace_response(address: std::net::SocketAddr, name: &str) -> (u16, String) {
    let response = send(address, "GET", &format!("/v1/namespaces/{name}"), "r", None, "").await;
    assert_eq!(response.0, 200);
    response
}

async fn verify_property_replay(store: &common::TestStore, address: std::net::SocketAddr) {
    let update_key = fresh_key(store).await;
    let records = store.values.load().len();
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
    let repeated = send(
        address,
        "POST",
        "/v1/namespaces/parent/properties",
        "w",
        Some(&update_key),
        changes,
    )
    .await;
    assert_eq!(repeated.0, 200);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&repeated.1).unwrap(),
        serde_json::json!({"removed": [], "updated": ["value"], "missing": ["missing", "owner"]})
    );
    assert_eq!(
        store.values.load().len(),
        records,
        "property updates must not create journals or response records"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&namespace_response(address, "parent").await.1).unwrap(),
        serde_json::json!({"namespace": ["parent"], "properties": {"value": "kept"}})
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
    body: &str,
) {
    let original = namespace_id(store, "parent");
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
    let recreated = send(address, "POST", "/v1/namespaces", "w", Some(duplicate_key), body).await;
    assert_eq!(recreated.0, 200);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&recreated.1).unwrap(),
        serde_json::from_str::<serde_json::Value>(body).unwrap()
    );
    let replacement = namespace_id(store, "parent");
    assert_ne!(replacement, original);
    assert_eq!(
        send(address, "POST", "/v1/namespaces", "w", None, body).await.0,
        409
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
    assert_eq!(namespace_id(store, "parent"), replacement);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&namespace_response(address, "parent").await.1).unwrap(),
        serde_json::from_str::<serde_json::Value>(body).unwrap()
    );
}

#[tokio::test]
async fn failed_requests_without_a_resource_operation_can_retry_after_the_cause_changes() {
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
        send(address, "POST", "/v1/namespaces", "w", Some(&key), body)
            .await
            .0,
        200
    );
    let invalid_key = fresh_key(&store).await;
    let malformed = send(address, "POST", "/v1/namespaces", "w", Some(&invalid_key), "{").await;
    assert_eq!(malformed.0, 400);
    assert_eq!(
        send(address, "POST", "/v1/namespaces", "w", Some(&invalid_key), "{").await,
        malformed
    );
    assert_eq!(
        send(
            address,
            "POST",
            "/v1/namespaces",
            "w",
            Some(&invalid_key),
            r#"{"namespace":["corrected"]}"#
        )
        .await
        .0,
        200
    );
    assert_eq!(
        send(address, "POST", "/v1/namespaces", "w", Some("invalid"), "{}")
            .await
            .0,
        400
    );
    assert_eq!(
        send(address, "POST", "/v1/namespaces", "w", None, body).await.0,
        409
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
async fn colliding_uuidv7_headers_do_not_create_shared_http_binding_state() {
    let (store, address, stop, server) = setup().await;
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap();
    let mut seen = std::collections::BTreeMap::new();
    let mut collision = None;
    for sequence in 0_u16..=4096 {
        let mut bytes = [0; 16];
        bytes[..6].copy_from_slice(&now.to_be_bytes()[2..]);
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        bytes[14..].copy_from_slice(&sequence.to_be_bytes());
        let operation = OperationId::from_bytes(&bytes).unwrap();
        let slot = ledger_key(SystemScope::RetryBinding, operation)
            .unwrap()
            .encode()
            .unwrap();
        if let Some(first) = seen.insert(slot, operation) {
            collision = Some((first, operation));
            break;
        }
    }
    let (first, second) = collision.expect("4097 identities must collide in 4096 slots");
    let first_key = wire_key(first);
    let second_key = wire_key(second);
    let first_body = r#"{"namespace":["first"]}"#;
    let second_body = r#"{"namespace":["second"]}"#;
    let first_response = send(
        address,
        "POST",
        "/v1/namespaces",
        "w",
        Some(&first_key),
        first_body,
    )
    .await;
    let second_response = send(
        address,
        "POST",
        "/v1/namespaces",
        "w",
        Some(&second_key),
        second_body,
    )
    .await;
    assert_eq!(first_response.0, 200);
    assert_eq!(second_response.0, 200);
    let first_id = namespace_id(&store, "first");
    let second_id = namespace_id(&store, "second");
    assert_ne!(first_id, second_id);
    assert_already_exists(
        &send(
            address,
            "POST",
            "/v1/namespaces",
            "w",
            Some(&first_key),
            first_body,
        )
        .await,
    );
    assert_already_exists(
        &send(
            address,
            "POST",
            "/v1/namespaces",
            "w",
            Some(&second_key),
            second_body,
        )
        .await,
    );
    assert_eq!(namespace_id(&store, "first"), first_id);
    assert_eq!(namespace_id(&store, "second"), second_id);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&namespace_response(address, "first").await.1).unwrap(),
        serde_json::from_str::<serde_json::Value>(&first_response.1).unwrap()
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&namespace_response(address, "second").await.1).unwrap(),
        serde_json::from_str::<serde_json::Value>(&second_response.1).unwrap()
    );
    let overflow = IcebergKey::System {
        scope: SystemScope::RetryOverflow,
        suffix: second.as_bytes().to_vec(),
    };
    assert!(!store.values.load().contains_key(&overflow.encode().unwrap()));
    assert!(store.values.load().keys().all(|key| !matches!(
        IcebergKey::decode(key),
        Ok(IcebergKey::System {
            scope: SystemScope::RetryBinding | SystemScope::RetryOverflow,
            ..
        })
    )));
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
        assert_eq!(
            store.lose_reply_kind.load(Ordering::SeqCst),
            0,
            "mode {mode} must hit its durable write"
        );
        let candidate = namespace_id(&store, &namespace);
        let visibility = send(
            address,
            "GET",
            &format!("/v1/namespaces/{namespace}"),
            "r",
            None,
            "",
        )
        .await;
        assert_eq!(visibility.0, if mode == 3 { 200 } else { 404 });
        let resumed = send(address, "POST", "/v1/namespaces", "w", Some(&key), &body).await;
        assert_eq!(resumed.0, if mode == 1 { 200 } else { 409 });
        if mode == 1 {
            assert!(resumed.1.len() > 16 * 1024);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&resumed.1).unwrap(),
                serde_json::from_str::<serde_json::Value>(&body).unwrap()
            );
        } else {
            assert_already_exists(&resumed);
        }
        let selected = namespace_id(&store, &namespace);
        assert_eq!(selected == candidate, mode != 1);
        assert_already_exists(&send(address, "POST", "/v1/namespaces", "w", Some(&key), &body).await);
        assert_eq!(namespace_id(&store, &namespace), selected);
        let loaded = namespace_response(address, &namespace).await;
        assert!(loaded.1.len() > 16 * 1024);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&loaded.1).unwrap(),
            serde_json::from_str::<serde_json::Value>(&body).unwrap()
        );
    }
    stop.send(()).unwrap();
    server.await.unwrap();
}
