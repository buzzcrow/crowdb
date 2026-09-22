#![cfg(feature = "iceberg")]

#[path = "common/iceberg_store.rs"]
mod common;

use crowdb_access_iceberg::catalog::{CatalogContext, CatalogRepository, ClearBounds, ManagementPrivilege};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::namespace::{
    NamespaceCreateRequest, NamespaceCreator, NamespaceIdentifier, NamespaceProperties,
};
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::wire::BearerAuthenticator;
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn setup() -> (
    Arc<common::TestStore>,
    CatalogContext,
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
            },
            ManagementPrivilege::Manage,
            100,
        )
        .await
        .unwrap();
    let context = repository.status().await.unwrap().0.context;
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
    (store, context, address, stop, server)
}

async fn create(store: Arc<common::TestStore>, context: CatalogContext, names: &[&str]) {
    let request = NamespaceCreateRequest {
        context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(names.iter().map(|name| (*name).into()).collect()).unwrap(),
        properties: NamespaceProperties::default(),
    };
    NamespaceCreator::new(store).create(&request).await.unwrap();
}

async fn send(address: std::net::SocketAddr, method: &str, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n", "r".repeat(32)).as_bytes()).await.unwrap();
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
async fn namespace_reads_preserve_single_decoding_and_page_token_semantics() {
    let (store, context, address, stop, server) = setup().await;
    for names in [&["parent"][..], &["parent", "a+b"], &["parent", "%2F"]] {
        create(store.clone(), context, names).await;
    }
    let (status, body) = send(address, "GET", "/v1/namespaces/parent%1Fa+b").await;
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["namespace"],
        serde_json::json!(["parent", "a+b"])
    );
    assert_eq!(send(address, "GET", "/v1/namespaces/parent%1F%252F").await.0, 200);
    assert_eq!(send(address, "GET", "/v1/namespaces/parent%1F%2F").await.0, 404);
    assert_eq!(
        send(address, "HEAD", "/v1/namespaces/parent").await,
        (204, String::new())
    );
    assert_eq!(
        send(address, "HEAD", "/v1/namespaces/absent").await,
        (404, String::new())
    );
    let (status, body) = send(address, "GET", "/v1/namespaces?parent=parent&pageSize=1").await;
    assert_eq!(status, 200);
    let complete: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(complete["namespaces"].as_array().unwrap().len(), 2);
    assert!(complete["next-page-token"].is_null());
    let (_, body) = send(
        address,
        "GET",
        "/v1/namespaces?parent=parent&pageSize=1&pageToken=",
    )
    .await;
    let page: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(page["namespaces"].as_array().unwrap().len(), 1);
    let token = page["next-page-token"].as_str().unwrap();
    assert_eq!(
        send(
            address,
            "GET",
            &format!("/v1/namespaces?parent=parent&pageSize=2&pageToken={token}")
        )
        .await
        .0,
        400
    );
    assert_eq!(
        send(
            address,
            "GET",
            &format!("/v1/namespaces?parent=parent&pageSize=1&pageToken={token}")
        )
        .await
        .0,
        200
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn complete_list_spool_admission_is_bounded_and_released() {
    let (store, _, address, stop, server) = setup().await;
    store.scan_delay_ms.store(500, Ordering::SeqCst);
    let mut requests = Vec::new();
    for _ in 0..4 {
        requests.push(tokio::spawn(send(address, "GET", "/v1/namespaces")));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.scans.load(Ordering::SeqCst) < 4 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(send(address, "GET", "/v1/namespaces").await.0, 503);
    for request in requests {
        assert_eq!(request.await.unwrap().0, 200);
    }
    store.scan_delay_ms.store(0, Ordering::SeqCst);
    assert_eq!(send(address, "GET", "/v1/namespaces").await.0, 200);
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn complete_list_exhaustion_never_returns_a_truncated_success() {
    use crowdb_access_iceberg::catalog::StoredValue;
    use crowdb_access_iceberg::key::NamespaceId;
    use crowdb_access_iceberg::namespace::{
        authority_key, name_key, NamespaceAuthority, NamespaceLifecycle, NamespaceMapping,
        NamespaceMappingState,
    };
    use crowdb_access_iceberg::record::StorageRecord;
    let (store, context, address, stop, server) = setup().await;
    let baseline = store.values.load_full();
    for (count, padding, stale) in [(1025, 0, false), (400, 3500, false), (4100, 0, true)] {
        let mut values = (*baseline).clone();
        for index in 0..count {
            let name = format!("{index:04}{}", "\"".repeat(padding));
            let namespace = NamespaceId::random();
            let mapping = NamespaceMapping {
                catalog: context.catalog,
                parent: None,
                name: name.clone(),
                namespace,
                name_epoch: 1,
                operation: OperationId::random(),
                state: NamespaceMappingState::Published,
            };
            values.insert(
                name_key(context.catalog, None, &name).unwrap().encode().unwrap(),
                StoredValue {
                    bytes: StorageRecord::NamespaceMapping(mapping).encode().unwrap(),
                    revision: 1,
                },
            );
            if !stale {
                let authority = NamespaceAuthority {
                    catalog: context.catalog,
                    namespace,
                    parent: None,
                    identifier: NamespaceIdentifier::new(vec![name]).unwrap(),
                    name_epoch: 1,
                    property_revision: 1,
                    admission_fence: 1,
                    mutation_revision: 1,
                    lifecycle: NamespaceLifecycle::Ready,
                    pending_operation: None,
                    properties: NamespaceProperties::default(),
                };
                values.insert(
                    authority_key(context.catalog, namespace).encode().unwrap(),
                    StoredValue {
                        bytes: StorageRecord::NamespaceAuthority(Box::new(authority))
                            .encode()
                            .unwrap(),
                        revision: 1,
                    },
                );
            }
        }
        store.values.store(Arc::new(values));
        let (status, body) = send(address, "GET", "/v1/namespaces").await;
        assert_eq!(status, 503, "count={count}, padding={padding}, stale={stale}");
        let error: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(error["error"]["code"], 503);
        assert!(error.get("namespaces").is_none());
        assert_eq!(
            send(address, "GET", "/v1/namespaces?pageSize=1&pageToken=")
                .await
                .0,
            200
        );
        store.values.store(baseline.clone());
        assert_eq!(send(address, "GET", "/v1/namespaces").await.0, 200);
    }
    stop.send(()).unwrap();
    server.await.unwrap();
}
