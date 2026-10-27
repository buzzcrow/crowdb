// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::any;
use crowdb_access_s3::auth::{RawAuthRequest, RequestAuthenticator, SigV4Verifier};
use crowdb_console_shared::config::{ServerEntry, ServiceType};
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use tower::ServiceExt;

#[tokio::test]
async fn node_proxy_uses_internal_listeners_while_exporting_separate_public_client_origins() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("split-origins");
    let credentials = crowdb_monitor::ServerCredentials::load_or_create(root.path()).unwrap();
    credentials
        .persist_client(&crowdb_monitor::ClientCredentials {
            s3_endpoint: "http://public.example.test:19091".into(),
            iceberg_endpoint: "http://public.example.test:19092".into(),
            dataset_endpoint: Some("http://public.example.test:19093".into()),
            region: "us-east-1".into(),
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "server-secret".into(),
        })
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let internal = format!("http://{}", listener.local_addr().unwrap());
    let upstream = axum::Router::new()
        .route("/", any(|| async { "internal-service" }))
        .route("/*path", any(|| async { "internal-service" }));
    let task = tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });
    let mut config = ConsoleConfig::default();
    let mut entry = ServerEntry::new("access-1", internal.clone());
    entry.node_id = Some(1);
    entry.service_type = ServiceType::AccessServer;
    for protocol in ["s3", "iceberg", "dataset"] {
        entry.access_origins.insert(protocol.into(), internal.clone());
    }
    config.servers.push(entry);
    let state = AppState::with_runtime_root(config, root.path().to_owned())
        .with_node_monitor("http://127.0.0.1:1")
        .unwrap();
    let app = router(state);
    for path in ["/api/access/s3", "/api/access/iceberg/v1/namespaces"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 100).await.unwrap(),
            "internal-service"
        );
    }
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/access/credentials")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    let clients: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(clients["s3"]["endpoint"], "http://public.example.test:19091");
    assert_eq!(clients["iceberg"]["uri"], "http://public.example.test:19092");
    assert_eq!(clients["dataset"]["uri"], "http://public.example.test:19093");
    assert_eq!(clients["dataset"]["token"], credentials.dataset_token());
    assert!(!String::from_utf8_lossy(&bytes).contains(credentials.s3_master_key()));
    task.abort();
}

#[tokio::test]
async fn native_deployment_resolves_cluster_origins_and_private_reader() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("native-access-origin");
    crowdb_monitor::ServerCredentials::load_or_create(root.path()).unwrap();
    let upstream = axum::Router::new().route(
        "/*path",
        any(|request: Request<Body>| async move {
            assert!(request.headers()["authorization"]
                .to_str()
                .unwrap()
                .starts_with("Bearer "));
            (StatusCode::OK, "configured-catalog")
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });
    let mut config = ConsoleConfig::default();
    let mut entry = ServerEntry::new("access-server-1", origin.clone());
    entry.service_type = ServiceType::AccessServer;
    config.servers.push(entry);
    config.local_launches.insert(
        "access-server-1".into(),
        crowdb_console_shared::config::LocalLaunchSpec {
            program: "unused".into(),
            args: vec![],
            workdir: root.path().to_string_lossy().into_owned(),
            env: std::collections::BTreeMap::from([
                ("CROWDB_ICEBERG_PUBLIC_URI".into(), origin.clone()),
                ("CROWDB_S3_PUBLIC_URI".into(), origin.clone()),
            ]),
            env_file: Some(
                root.path()
                    .join("secrets/server.env")
                    .to_string_lossy()
                    .into_owned(),
            ),
            readiness_url: None,
        },
    );
    let app = router(AppState::with_runtime_root(config, root.path().to_owned()));
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/access/connections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(metadata["iceberg"], origin);
    assert_eq!(metadata["s3"], origin);
    assert_eq!(metadata["iceberg_ready"], true);
    assert!(!String::from_utf8_lossy(&bytes).contains("TOKEN"));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/access/iceberg/v1/namespaces")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    task.abort();
}

fn configured(origin: &str) -> axum::Router {
    let mut config = ConsoleConfig::default();
    for protocol in ["s3", "iceberg"] {
        let mut entry = ServerEntry::new(format!("console-access-{protocol}"), origin.to_string());
        entry.service_type = ServiceType::AccessServer;
        entry.auto_start = false;
        config.servers.push(entry);
    }
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("root-access").keep();
    let credentials = crowdb_monitor::ServerCredentials::load_or_create(&root).unwrap();
    credentials
        .persist_client(&crowdb_monitor::ClientCredentials {
            s3_endpoint: origin.into(),
            iceberg_endpoint: origin.into(),
            dataset_endpoint: None,
            region: "us-east-1".into(),
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "server-secret".into(),
        })
        .unwrap();
    router(
        AppState::with_runtime_root(config, root)
            .with_iceberg_reader("r".repeat(32))
            .unwrap(),
    )
}

#[tokio::test]
async fn iceberg_proxy_preserves_client_retry_identity_and_duplicate_header_validation() {
    const KEY: &str = "019a2f6c-0000-7000-8000-000000000001";
    let upstream = axum::Router::new().route(
        "/*path",
        any(|request: Request<Body>| async move {
            let keys: Vec<_> = request
                .headers()
                .get_all("idempotency-key")
                .iter()
                .map(|value| value.to_str().unwrap().to_owned())
                .collect();
            if keys.len() != 1 {
                return (StatusCode::BAD_REQUEST, "duplicate retry identity".to_owned());
            }
            (StatusCode::OK, keys[0].clone())
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });
    let app = configured(&origin);
    for duplicate in [false, true] {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/api/access/iceberg/v1/namespaces/example/tables/events")
            .header("idempotency-key", KEY);
        if duplicate {
            builder = builder.header("idempotency-key", KEY);
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::from("{}")).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if duplicate {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::OK
            }
        );
        let body = axum::body::to_bytes(response.into_body(), 100).await.unwrap();
        assert_eq!(
            body,
            if duplicate {
                "duplicate retry identity"
            } else {
                KEY
            }
        );
    }
    task.abort();
}

#[tokio::test]
async fn remote_deployment_resolves_persisted_origins_without_local_launches() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("remote-access-origin");
    let credentials = crowdb_monitor::ServerCredentials::load_or_create(root.path()).unwrap();
    credentials
        .persist_client(&crowdb_monitor::ClientCredentials {
            s3_endpoint: "http://192.0.2.10:9001".into(),
            iceberg_endpoint: "http://192.0.2.10:9000".into(),
            dataset_endpoint: None,
            region: "us-east-1".into(),
            access_key_id: "test-client".into(),
            secret_access_key: "private-test-secret".into(),
        })
        .unwrap();
    let mut config = ConsoleConfig::default();
    let mut entry = ServerEntry::new("access-server-1", "http://192.0.2.10:9000");
    entry.service_type = ServiceType::AccessServer;
    entry.node_id = Some(1);
    config.servers.push(entry);
    assert!(config.local_launches.is_empty());
    let state = AppState::with_runtime_root(config, root.path().to_owned());
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/access/connections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(metadata["s3"], "http://192.0.2.10:9001");
    assert_eq!(metadata["iceberg"], "http://192.0.2.10:9000");
    assert_eq!(metadata["iceberg_ready"], true);
    assert!(!String::from_utf8_lossy(&bytes).contains("private-test-secret"));
    state.config.write().unwrap().servers.clear();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/access/connections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(metadata["s3"].is_null());
    assert!(metadata["iceberg"].is_null());
}

#[tokio::test]
async fn root_catalog_uses_server_held_read_and_write_credentials() {
    let upstream = axum::Router::new().route(
        "/*path",
        any(|request: Request<Body>| async move {
            if request.method() == "GET" {
                assert_eq!(
                    request.headers()["authorization"],
                    format!("Bearer {}", "r".repeat(32))
                );
                (StatusCode::OK, "catalog")
            } else {
                assert!(request.headers()["authorization"]
                    .to_str()
                    .unwrap()
                    .starts_with("Bearer "));
                assert_ne!(request.headers()["authorization"], "Bearer browser-credential");
                (StatusCode::OK, "catalog updated")
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app = configured(&format!("http://{}", listener.local_addr().unwrap()));
    let task = tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });
    for (method, expected) in [("GET", StatusCode::OK), ("POST", StatusCode::OK)] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri("/api/access/iceberg/v1/namespaces")
                    .header("authorization", "Bearer browser-credential")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/access/connections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(metadata["iceberg_ready"], true);
    assert!(!String::from_utf8_lossy(&bytes).contains(&"r".repeat(32)));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/access/connections")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"protocol":"iceberg","origin":"http://untrusted.example"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    task.abort();
}

#[tokio::test]
async fn s3_root_with_or_without_trailing_slash_reaches_native_service() {
    let upstream = axum::Router::new().route("/", any(|| async { "native-root" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });
    for path in ["/api/access/s3", "/api/access/s3/"] {
        let response = configured(&address)
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 100).await.unwrap(),
            "native-root"
        );
    }
    task.abort();
}

#[tokio::test]
async fn root_proxy_signs_with_server_credentials_and_preserves_ranges_and_status() {
    let upstream = axum::Router::new().route(
        "/*path",
        any(|request: Request<Body>| async move {
            assert_eq!(request.uri().path(), "/bucket/a%20b");
            assert_eq!(request.uri().query(), Some("uploadId=a%2Fb"));
            SigV4Verifier::new(TestCredentials, "us-east-1".into(), 60)
                .authenticate(RawAuthRequest::from_parts(
                    request.method(),
                    request.uri(),
                    request.headers(),
                ))
                .await
                .unwrap();
            assert_eq!(request.headers()["range"], "bytes=0-3");
            assert!(!request.headers().contains_key("x-console-secret"));
            (
                StatusCode::PARTIAL_CONTENT,
                [("etag", "native-etag"), ("content-range", "bytes 0-3/100")],
                "data",
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app = configured(&format!("http://{}", listener.local_addr().unwrap()));
    let task = tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/access/s3/bucket/a%20b?uploadId=a%2Fb")
                .header("authorization", "native-credential")
                .header("range", "bytes=0-3")
                .header("x-console-secret", "do-not-forward")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["etag"], "native-etag");
    assert_eq!(response.headers()["content-range"], "bytes 0-3/100");
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 100).await.unwrap(),
        "data"
    );
    task.abort();
}

#[tokio::test]
async fn proxy_rejects_unknown_protocol_and_non_catalog_paths_before_network() {
    for path in ["/api/access/other/x", "/api/access/iceberg/admin"] {
        let response = configured("http://127.0.0.1:1")
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
    }
}

#[tokio::test]
async fn access_configuration_rejects_embedded_credentials_and_paths() {
    for origin in [
        "file:///tmp/secret",
        "http://user:secret@localhost",
        "http://localhost/admin",
    ] {
        let response = router(AppState::new(vec![]))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/access/connections")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"protocol":"s3", "origin":origin}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{origin}");
    }
}

#[tokio::test]
async fn group0_data_mutations_are_rejected_before_leader_resolution() {
    for operation in ["put", "delete"] {
        let response = router(AppState::new(vec![]))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/stores/0/groups/0/kv/{operation}"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"key":"key","value":"value"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn chunk_query_rejects_invalid_ids_and_unbounded_pages() {
    for path in [
        "/api/chunks?limit=257",
        "/api/chunks?prefix=not-hex",
        "/api/chunks?after=abc",
        "/api/chunks/abc",
        "/api/stores/0/groups/1/chunks?chunk_type=0",
        "/api/stores/0/groups/1/chunks?chunk_type=7",
    ] {
        let response = router(AppState::new(vec![]))
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
    }
}

struct TestCredentials;
impl crowdb_access_s3::auth::CredentialProvider for TestCredentials {
    fn lookup(&self, access_key: &str) -> Option<crowdb_access_s3::auth::Credential> {
        (access_key == "AKIDEXAMPLE").then(|| crowdb_access_s3::auth::Credential {
            secret_key: b"server-secret".to_vec(),
            session_token: None,
            enabled: true,
        })
    }
}
