#![cfg(feature = "iceberg")]

#[path = "common/iceberg_store.rs"]
mod common;

use crowdb_access_iceberg::catalog::{CatalogRepository, ClearBounds, ManagementPrivilege};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::wire::BearerAuthenticator;
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn authenticated_config_warehouse_errors_and_shutdown_use_real_http() {
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
    let authentication = BearerAuthenticator::new(&"r".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let service = Arc::new(IcebergHttpService::new(
        repository,
        authentication,
        Duration::from_secs(2),
    ));
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
    for (path, expected) in [
        ("/v1/config", 200),
        ("/v1/config?warehouse=", 200),
        ("/v1/config?warehouse=unknown", 404),
        ("/v1/config?warehouse=%ZZ", 400),
        ("/v1/config?warehouse=&warehouse=", 400),
        ("/v1/namespaces", 406),
    ] {
        let response = get(address, path, &"r".repeat(32)).await;
        assert!(
            response.starts_with(&format!("HTTP/1.1 {expected}")),
            "{response}"
        );
        let body = response.split_once("\r\n\r\n").unwrap().1;
        let json: serde_json::Value = serde_json::from_str(body).unwrap();
        if expected == 200 {
            assert_eq!(json["endpoints"], serde_json::json!([]));
        }
        if expected == 404 {
            assert_eq!(json["error"]["type"], "NoSuchWarehouseException");
        }
    }
    assert!(get(address, "/v1/config", "wrong")
        .await
        .starts_with("HTTP/1.1 401"));
    store
        .read_delay_ms
        .store(3000, std::sync::atomic::Ordering::SeqCst);
    let expired = tokio::time::timeout(
        Duration::from_millis(2500),
        get(address, "/v1/config", &"r".repeat(32)),
    )
    .await
    .unwrap();
    assert!(!expired.starts_with("HTTP/1.1 200"));
    store.read_delay_ms.store(0, std::sync::atomic::Ordering::SeqCst);
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

async fn get(address: std::net::SocketAddr, path: &str, token: &str) -> String {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.unwrap();
    String::from_utf8(bytes).unwrap()
}
