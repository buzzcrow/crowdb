// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use axum::response::IntoResponse;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::any,
};
use crowdb_console_shared::{config::ServerEntry, ConsoleConfig};
use crowdb_web::{router, AppState};
use tower::ServiceExt;
const PREFIX: &str = "s3://iceberg/11111111111111111111111111111111/22222222222222222222222222222222/33333333333333333333333333333333/";
const CREDENTIALS: &str = "/v1/namespaces/ns/tables/events/credentials?table-id=33333333333333333333333333333333&storage-namespace-id=22222222222222222222222222222222&format-version=2";

#[tokio::test]
async fn files_use_configured_origin_fresh_writer_grants_and_authorized_table_prefix() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("iceberg-file-proxy");
    crowdb_monitor::ServerCredentials::load_or_create(root.path()).unwrap();
    let upstream = axum::Router::new().route("/*path", any(|request: Request<Body>| async move {
        if request.uri().path().ends_with("/credentials") {
            assert!(request.headers()["authorization"].to_str().unwrap().starts_with("Bearer "));
            assert!(request.uri().query().unwrap().contains("table-id=33333333333333333333333333333333"));
            return (StatusCode::OK, axum::Json(serde_json::json!({"storage-credentials":[{"prefix":PREFIX,"config":{
                "s3.access-key-id":"temporary-key","s3.secret-access-key":"temporary-secret","s3.session-token":"temporary-session"
            }}]}))).into_response();
        }
        assert!(request.uri().path().ends_with("/data/new.parquet"));
        assert_eq!(request.headers()["x-amz-security-token"], "temporary-session");
        assert!(request.headers()["authorization"].to_str().unwrap().contains("Credential=temporary-key/"));
        assert!(!request.headers().contains_key("x-crowdb-file-credentials"));
        assert_eq!(axum::body::to_bytes(request.into_body(), 100).await.unwrap(), "PAR1");
        (StatusCode::OK, [("etag", "file-etag")], "uploaded").into_response()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });
    let mut config = ConsoleConfig::default();
    config
        .servers
        .push(ServerEntry::new("console-access-iceberg", origin));
    let app = router(AppState::with_runtime_root(config, root.path().to_owned()));
    for (credential, location, expected) in [
        (CREDENTIALS.to_owned(), format!("{PREFIX}data/new.parquet"), StatusCode::OK),
        (CREDENTIALS.to_owned(), format!("{}data/new.parquet", PREFIX.replace("33333333", "44444444")), StatusCode::BAD_REQUEST),
        ("http://untrusted.invalid/v1/namespaces/ns/tables/events/credentials?table-id=33333333333333333333333333333333".into(), format!("{PREFIX}data/new.parquet"), StatusCode::BAD_REQUEST),
        (format!("{CREDENTIALS}&cleanup=true"), format!("{PREFIX}data/new.parquet"), StatusCode::BAD_REQUEST),
        (CREDENTIALS.to_owned(), "s3://iceberg/../outside".into(), StatusCode::BAD_REQUEST),
    ] {
        let response = app.clone().oneshot(Request::builder().method("PUT").uri("/api/access/iceberg-files")
            .header("x-crowdb-file-credentials", credential).header("x-crowdb-file-location", location)
            .header("authorization", "browser-secret").body(Body::from("PAR1")).unwrap()).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 65536).await.unwrap();
        assert_eq!(status, expected, "{}", String::from_utf8_lossy(&bytes));
    }
    task.abort();
}
