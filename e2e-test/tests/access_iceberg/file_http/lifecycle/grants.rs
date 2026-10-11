// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::super::{now_ms, signed_file::TestFileClient};
use crowdb_access_iceberg::{
    catalog::CatalogContext,
    file::{FileCredentials, FileGrantIssuer, TableLocation},
    key::OperationId,
};
use reqwest::{Client, Method, Response};
use serde_json::{json, Value};
use std::time::Duration;
pub(super) fn location(response: &Value) -> TableLocation {
    format!(
        "{}/",
        response["metadata"]["location"]
            .as_str()
            .unwrap()
            .trim_end_matches('/')
    )
    .parse()
    .unwrap()
}

pub(super) async fn expire(writer: &TestFileClient, issuer: &FileGrantIssuer, object: &str) {
    let mut grant = writer.credentials.grant().clone();
    grant.nonce = OperationId::random();
    grant.issued_ms = now_ms();
    grant.expires_ms = grant.issued_ms + 2_000;
    let expires = grant.expires_ms;
    let short = TestFileClient {
        client: Client::new(),
        credentials: issuer.issue(grant).unwrap(),
        address: writer.address,
    };
    assert_eq!(
        short.send(Method::GET, object, "", b"", false).await.status(),
        200
    );
    tokio::time::sleep(Duration::from_millis(expires.saturating_sub(now_ms()))).await;
    assert!(now_ms() >= expires);
    assert_eq!(
        short.send(Method::GET, object, "", b"", false).await.status(),
        403
    );
    assert_eq!(
        writer.send(Method::GET, object, "", b"", false).await.status(),
        200
    );
}

pub(super) async fn refresh(
    endpoint: &str,
    name: &str,
    role: &str,
    issuer: &FileGrantIssuer,
    context: CatalogContext,
) -> FileCredentials {
    let response = value(
        rest(endpoint, Method::GET, &format!("{name}/credentials"), role, None).await,
        200,
    )
    .await;
    let config = &response["storage-credentials"][0]["config"];
    let credentials = issuer
        .verify(
            config["s3.access-key-id"].as_str().unwrap(),
            config["s3.session-token"].as_str().unwrap(),
            context,
            now_ms(),
        )
        .unwrap();
    assert_eq!(
        credentials.secret_access_key(),
        config["s3.secret-access-key"].as_str().unwrap()
    );
    credentials
}

pub(super) async fn create(endpoint: &str) -> Value {
    value(rest(endpoint, Method::POST, "/v1/namespaces/analytics/tables", "w", Some(json!({
        "name":"grants","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"long","required":true}]}
    }))).await, 200).await
}

pub(super) async fn rest(
    endpoint: &str,
    method: Method,
    path: &str,
    role: &str,
    body: Option<Value>,
) -> Response {
    let mut request = Client::new()
        .request(method, format!("{endpoint}{path}"))
        .bearer_auth(role.repeat(32));
    if let Some(body) = body {
        request = request
            .header("content-type", "application/json")
            .body(body.to_string());
    }
    request.send().await.unwrap()
}

pub(super) async fn value(response: Response, expected: u16) -> Value {
    let status = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(status, expected, "{text}");
    if expected == 204 {
        Value::Null
    } else {
        serde_json::from_str(&text).unwrap()
    }
}
