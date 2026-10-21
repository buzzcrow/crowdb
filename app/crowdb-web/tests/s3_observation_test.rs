// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{body::Body, http::Request, response::Response, routing::get, Router};
use crowdb_access_s3::auth::{RawAuthRequest, RequestAuthenticator, SigV4Verifier};
use crowdb_web::{s3_protocol_probe_for_tests, AppState};

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

#[tokio::test]
async fn local_s3_probe_signs_list_buckets_and_reports_exact_failures() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("s3-observation");
    let credentials = crowdb_monitor::ServerCredentials::load_or_create(root.path()).unwrap();
    credentials
        .persist_client(&crowdb_monitor::ClientCredentials {
            s3_endpoint: "http://public.example.test:19091".into(),
            iceberg_endpoint: "http://public.example.test:19092".into(),
            dataset_endpoint: None,
            region: "us-east-1".into(),
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "server-secret".into(),
        })
        .unwrap();
    let state = AppState::with_runtime_root(
        crowdb_console_shared::ConsoleConfig::default(),
        root.path().to_owned(),
    );
    for (status, body, code) in [
        (200, "<ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>", None),
        (200, "<?xml version=\"1.0\"?><ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Buckets><Bucket><Name>existing</Name></Bucket></Buckets></ListAllMyBucketsResult>", None),
        (200, r#"{"ready":true}"#, Some("protocol-response-invalid")),
        (200, "<ListAllMyBucketsResult><Buckets/></wrong>", Some("protocol-response-invalid")),
        (403, "<Error><Code>AccessDenied</Code></Error>", Some("protocol-http-status")),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let app = Router::new().route("/", get(move |request: Request<Body>| async move {
            assert_eq!(request.method(), "GET");
            assert_eq!(request.uri().path(), "/");
            assert!(request.uri().query().is_none());
            SigV4Verifier::new(TestCredentials, "us-east-1".into(), 60)
                .authenticate(RawAuthRequest::from_parts(request.method(), request.uri(), request.headers()))
                .await.unwrap();
            Response::builder().status(status).body(Body::from(body)).unwrap()
        }));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let result = s3_protocol_probe_for_tests(&state, &url).await;
        assert_eq!(result.as_ref().err().map(|(code, _)| *code), code, "{result:?}");
        let detail = match result { Ok(detail) | Err((_, detail)) => detail };
        assert!(detail.contains(&format!("HTTP {status}")));
        assert!(!detail.contains("server-secret"));
        assert!(!detail.contains("AKIDEXAMPLE"));
        task.abort();
    }
}
