#![cfg(feature = "iceberg")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::key::{CatalogScope, IcebergKey};
use fixture::TestTableHttp;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TABLE: &str = "/v1/namespaces/analytics/tables/events";

async fn fixture() -> TestTableHttp {
    let fixture = TestTableHttp::writable().await;
    let response = fixture
        .post(
            "/v1/namespaces/analytics/tables",
            "w",
            None,
            &serde_json::json!({"name":"events","schema":{"type":"struct","schema-id":0,"fields":[
            {"id":1,"name":"id","type":"long","required":true}]}}),
        )
        .await;
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    fixture
}

fn mutations(fixture: &TestTableHttp) -> Vec<(Vec<u8>, Vec<u8>)> {
    fixture
        .store
        .values
        .load()
        .iter()
        .filter(|(key, _)| {
            matches!(
                IcebergKey::decode(key),
                Ok(IcebergKey::Catalog {
                    scope: CatalogScope::TableHead
                        | CatalogScope::TableCommitOperation
                        | CatalogScope::File
                        | CatalogScope::FileLocation,
                    ..
                })
            )
        })
        .map(|(key, value)| (key.clone(), value.bytes.clone()))
        .collect()
}

#[tokio::test]
async fn commit_request_byte_limits_reject_before_candidate_or_operation_creation() {
    let fixture = fixture().await;
    let mut body = r#"{"requirements":[],"updates":[]}"#.as_bytes().to_vec();
    body.resize(2 * 1024 * 1024 - 64 * 1024, b' ');
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}{TABLE}", fixture.endpoint()))
        .bearer_auth("w".repeat(32))
        .header("content-type", "application/json")
        .body(body.clone())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let response = response.text().await.unwrap();
    assert_eq!(status, 200, "{response}");
    let before = mutations(&fixture);
    for length in [body.len() + 1, 2 * 1024 * 1024 + 1] {
        body.resize(length, b' ');
        let response = client
            .post(format!("{}{TABLE}", fixture.endpoint()))
            .bearer_auth("w".repeat(32))
            .header("content-type", "application/json")
            .body(body.clone())
            .send()
            .await
            .unwrap();
        let status = response.status();
        let response = response.text().await.unwrap();
        assert_eq!(status, 400, "{response}");
        assert_eq!(mutations(&fixture), before);
    }
    let response = fixture
        .post(
            TABLE,
            "w",
            None,
            &serde_json::json!({"requirements":[],"updates":[]}),
        )
        .await;
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    fixture.finish().await;
}

async fn pending_body(endpoint: &str) -> tokio::net::TcpStream {
    let address = endpoint.strip_prefix("http://").unwrap();
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("POST {TABLE} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {}\r\nContent-Length: 1\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n", "w".repeat(32)).as_bytes()).await.unwrap();
    let mut header = [0; 25];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(&header, b"HTTP/1.1 100 Continue\r\n\r\n");
    stream
}

#[tokio::test]
async fn pending_commit_bodies_share_admission_and_errors_release_every_slot() {
    let fixture = fixture().await;
    let before = mutations(&fixture);
    let mut streams = Vec::new();
    for _ in 0..4 {
        streams.push(pending_body(&fixture.endpoint()).await);
    }
    let response = fixture
        .post(
            TABLE,
            "w",
            None,
            &serde_json::json!({"requirements":[],"updates":[]}),
        )
        .await;
    assert_eq!(response.status(), 503, "{}", response.text().await.unwrap());
    assert_eq!(mutations(&fixture), before);
    for mut stream in streams {
        stream.write_all(b"x").await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert!(
            response.starts_with(b"HTTP/1.1 400"),
            "{}",
            String::from_utf8_lossy(&response)
        );
    }
    assert_eq!(mutations(&fixture), before);
    let response = fixture
        .post(
            TABLE,
            "w",
            None,
            &serde_json::json!({"requirements":[],"updates":[]}),
        )
        .await;
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    fixture.finish().await;
}
