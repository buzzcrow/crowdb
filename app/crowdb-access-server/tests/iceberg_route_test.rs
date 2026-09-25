#![cfg(feature = "iceberg")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;

use std::{sync::Arc, time::Duration};

use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds, ManagementPrivilege},
    key::OperationId,
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
    wire::BearerAuthenticator,
};
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION},
    Client, Method, StatusCode,
};
use tokio::net::TcpListener;

async fn start(
    namespaces: bool,
    tables: bool,
    credentials: bool,
) -> (
    Arc<common::TestStore>,
    String,
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
    let authentication =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let mut service = IcebergHttpService::new(repository, authentication, Duration::from_secs(2));
    if namespaces {
        service = service.with_namespaces(store.clone()).unwrap();
    }
    if tables {
        service = service
            .with_tables(store.clone(), Arc::new(blocks::TestFileBlocks::default()))
            .unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    if credentials {
        service = service
            .with_table_credentials(store.clone(), origin.clone())
            .unwrap();
    }
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        serve(listener, Arc::new(service), async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    (store, origin, stop, server)
}

async fn send(client: &Client, origin: &str, method: Method, path: &str, token: &str) -> reqwest::Response {
    client
        .request(method, format!("{origin}{path}"))
        .bearer_auth(token.repeat(32))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn discovery_uses_installed_routes_and_unsupported_paths_leave_no_record() {
    let client = Client::builder().timeout(Duration::from_secs(3)).build().unwrap();
    for (namespaces, tables, credentials, expected) in [
        (false, false, false, 0),
        (false, true, false, 0),
        (true, false, false, 6),
        (true, true, true, 14),
    ] {
        let (store, origin, stop, server) = start(namespaces, tables, credentials).await;
        let config = send(&client, &origin, Method::GET, "/v1/config", "r")
            .await
            .json::<serde_json::Value>()
            .await
            .unwrap();
        let endpoints = config["endpoints"].as_array().unwrap();
        assert_eq!(endpoints.len(), expected);
        if !namespaces {
            assert!(config.get("idempotency-key-lifetime").is_none());
        }
        for endpoint in endpoints {
            let template = endpoint.as_str().unwrap();
            assert!(!template.contains("/plan"));
            assert!(!template.contains("/metrics"));
            assert!(!template.contains("/register"));
            assert!(!template.contains("/oauth"));
        }
        let authority = store.values.load_full();
        for (method, path) in [
            (Method::POST, "/v1/namespaces/analytics/tables/events/plan"),
            (Method::POST, "/v1/namespaces/analytics/tables/events/metrics"),
            (Method::POST, "/v1/namespaces/analytics/register"),
            (Method::POST, "/v1/transactions/commit"),
            (Method::POST, "/v1/oauth/tokens"),
            (Method::DELETE, "/v1/namespaces/analytics/tables"),
            (Method::POST, "/v1/namespaces/analytics/tables/events/credentials"),
        ] {
            let response = send(&client, &origin, method, path, "w").await;
            assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE, "{path}");
            assert_eq!(
                response.json::<serde_json::Value>().await.unwrap()["error"]["type"],
                "UnsupportedOperationException"
            );
            assert_eq!(*store.values.load_full(), *authority, "{path}");
        }
        let unauthenticated = client
            .post(format!("{origin}/v1/namespaces/analytics/register"))
            .send()
            .await
            .unwrap();
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
        let mut duplicate = HeaderMap::new();
        duplicate.append(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", "r".repeat(32))).unwrap(),
        );
        duplicate.append(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", "w".repeat(32))).unwrap(),
        );
        let duplicate = client
            .get(format!("{origin}/v1/config"))
            .headers(duplicate)
            .send()
            .await
            .unwrap();
        assert_eq!(duplicate.status(), StatusCode::UNAUTHORIZED);
        let table_path = "/v1/namespaces/analytics/tables/events";
        let table_read = send(&client, &origin, Method::GET, table_path, "r").await;
        assert_eq!(
            table_read.status(),
            if namespaces && tables {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::NOT_ACCEPTABLE
            }
        );
        stop.send(()).unwrap();
        server.await.unwrap();
    }
}
