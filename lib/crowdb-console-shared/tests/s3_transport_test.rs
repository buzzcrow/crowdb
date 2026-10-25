// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::ops::s3::{sign_request, S3Credentials, S3HttpClient};
use reqwest::{header::HeaderMap, Method};

fn credentials() -> S3Credentials {
    S3Credentials {
        access: "test-access".into(),
        secret: "private-secret".into(),
        region: "us-east-1".into(),
        session: Some("private-session-token".into()),
    }
}

#[test]
fn selected_origins_reject_embedded_credentials_and_non_origin_paths() {
    for endpoint in [
        "http://user:secret@127.0.0.1:9091",
        "http://127.0.0.1:9091/api",
        "http://127.0.0.1:9091?token=private-secret",
        "file:///private-secret",
        "http://127.0.0.1:0",
    ] {
        let error = S3HttpClient::signed(endpoint.into(), credentials())
            .err()
            .expect("invalid endpoint");
        assert!(!error.to_string().contains("private-secret"));
    }
    let mut empty = credentials();
    empty.secret.clear();
    assert!(S3HttpClient::signed("http://127.0.0.1:9091".into(), empty).is_err());
}

#[test]
fn authentication_headers_are_sensitive_in_structured_diagnostics() {
    let mut headers = HeaderMap::new();
    let url = "http://127.0.0.1:9091/bucket/key?prefix=a%20b&list-type=2"
        .parse()
        .unwrap();
    sign_request(&credentials(), &Method::GET, &url, b"", &mut headers).unwrap();
    assert!(headers["authorization"].is_sensitive());
    assert!(headers["x-amz-security-token"].is_sensitive());
    let debug = format!("{headers:?}");
    assert!(!debug.contains("private-session-token"));
    assert!(!debug.contains("private-secret"));
}

#[tokio::test]
async fn selected_s3_redirects_are_reported_without_forwarding_credentials() {
    use axum::{http::StatusCode, routing::get, Router};
    let destination = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let location = format!("http://{}", destination.local_addr().unwrap());
    let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", first.local_addr().unwrap());
    let router = Router::new().route(
        "/",
        get(move || {
            let location = location.clone();
            async move { (StatusCode::TEMPORARY_REDIRECT, [("location", location)]) }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(first, router).await.unwrap();
    });
    let client = S3HttpClient::signed(endpoint, credentials()).unwrap();
    let reply = client
        .request_exchange(Method::GET, None, None, &[], None, None)
        .await
        .unwrap();
    assert_eq!(reply.status, StatusCode::TEMPORARY_REDIRECT);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), destination.accept())
            .await
            .is_err()
    );
    server.abort();
}
