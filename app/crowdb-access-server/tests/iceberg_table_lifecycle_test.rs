#![cfg(feature = "iceberg")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;

use fixture::TestTableHttp;
use reqwest::Method;
use serde_json::{json, Value};

const TABLES: &str = "/v1/namespaces/analytics/tables";
const TABLE: &str = "/v1/namespaces/analytics/tables/events";
const RENAME: &str = "/v1/tables/rename";

fn key() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!(
        "{:08x}-{:04x}-7000-8000-{:012x}",
        now >> 16,
        now & 0xffff,
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

fn create() -> Value {
    json!({"name":"events", "schema":{"type":"struct","schema-id":0,
        "fields":[{"id":1,"name":"id","type":"long","required":true}]}})
}

async fn value(response: reqwest::Response, status: u16) -> Value {
    let actual = response.status();
    let bytes = response.text().await.unwrap();
    assert_eq!(actual.as_u16(), status, "{bytes}");
    serde_json::from_str(&bytes).unwrap()
}

async fn empty(response: reqwest::Response) {
    let status = response.status();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(status.as_u16(), 204, "{bytes:?}");
    assert!(bytes.is_empty());
}

async fn delete(test: &TestTableHttp, path: &str, role: &str, identity: &str) -> reqwest::Response {
    reqwest::Client::new()
        .delete(format!("{}{path}", test.endpoint()))
        .bearer_auth(role.repeat(32))
        .header("idempotency-key", identity)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn drop_enforces_writer_and_replays_without_deleting_recreated_table() {
    for purge in [false, true] {
        let test = TestTableHttp::writable().await;
        let before = value(test.post(TABLES, "w", None, &create()).await, 200).await;
        let path = format!("{TABLE}?purgeRequested={purge}");
        for role in ["r", "m", "c"] {
            value(delete(&test, &path, role, &key()).await, 403).await;
        }
        let identity = key();
        empty(delete(&test, &path, "w", &identity).await).await;
        value(test.request(Method::GET, TABLE, "r", None).await, 404).await;
        let after = value(test.post(TABLES, "w", None, &create()).await, 200).await;
        assert_ne!(before["metadata"]["table-uuid"], after["metadata"]["table-uuid"]);
        empty(delete(&test, &path, "w", &identity).await).await;
        assert_eq!(
            value(test.request(Method::GET, TABLE, "r", None).await, 200).await,
            after
        );
        value(
            delete(
                &test,
                &format!("{TABLE}?purgeRequested={}", !purge),
                "w",
                &identity,
            )
            .await,
            409,
        )
        .await;
        test.finish().await;
    }
}

#[tokio::test]
async fn rename_preserves_metadata_supports_cross_namespace_and_never_aliases_old_name() {
    let test = TestTableHttp::writable().await;
    let before = value(test.post(TABLES, "w", None, &create()).await, 200).await;
    let source = json!({"namespace":["analytics"], "name":"events"});
    let target = json!({"namespace":["analytics"], "name":"renamed"});
    let rename = json!({"source":source,"destination":target});
    for role in ["r", "m", "c"] {
        value(test.post(RENAME, role, None, &rename).await, 403).await;
    }
    let identity = key();
    empty(test.post(RENAME, "w", Some(&identity), &rename).await).await;
    value(test.request(Method::GET, TABLE, "r", None).await, 404).await;
    let renamed = "/v1/namespaces/analytics/tables/renamed";
    assert_eq!(
        value(test.request(Method::GET, renamed, "r", None).await, 200).await,
        before
    );
    value(
        test.post(TABLE, "w", None, &json!({"requirements":[],"updates":[]}))
            .await,
        404,
    )
    .await;
    value(test.post(TABLES, "w", None, &create()).await, 200).await;
    empty(test.post(RENAME, "w", Some(&identity), &rename).await).await;
    value(
        test.post("/v1/namespaces", "w", None, &json!({"namespace":["destination"]}))
            .await,
        200,
    )
    .await;
    empty(
        test.post(
            RENAME,
            "w",
            None,
            &json!({"source":target,
        "destination":{"namespace":["destination"],"name":"moved"}}),
        )
        .await,
    )
    .await;
    value(test.request(Method::GET, renamed, "r", None).await, 404).await;
    let moved = "/v1/namespaces/destination/tables/moved";
    assert_eq!(
        value(test.request(Method::GET, moved, "r", None).await, 200).await,
        before
    );
    let update =
        json!({"requirements":[],"updates":[{"action":"set-properties","updates":{"renamed":"yes"}}]});
    let committed = value(test.post(moved, "w", None, &update).await, 200).await;
    assert_eq!(committed["metadata"]["properties"]["renamed"], "yes");
    assert_eq!(committed["metadata"]["location"], before["metadata"]["location"]);
    let config = value(test.request(Method::GET, "/v1/config", "r", None).await, 200).await;
    assert!(config["endpoints"]
        .as_array()
        .unwrap()
        .contains(&json!("POST /v1/{prefix}/tables/rename")));
    test.finish().await;
}

#[tokio::test]
async fn lifecycle_rejects_malformed_or_unsupported_requests_without_mutation() {
    let test = TestTableHttp::writable().await;
    let before = value(test.post(TABLES, "w", None, &create()).await, 200).await;
    value(test.request(Method::GET, RENAME, "w", None).await, 406).await;
    value(delete(&test, RENAME, "w", &key()).await, 406).await;
    value(delete(&test, TABLES, "w", &key()).await, 406).await;
    for query in [
        "purgeRequested=1",
        "purgeRequested=true&purgeRequested=false",
        "unknown=true",
    ] {
        value(delete(&test, &format!("{TABLE}?{query}"), "w", &key()).await, 400).await;
    }
    for body in [
        json!({}),
        json!({"source":{"namespace":[],"name":"events"},"destination":{"namespace":["analytics"],"name":"other"}}),
    ] {
        value(test.post(RENAME, "w", None, &body).await, 400).await;
    }
    let source = json!({"namespace":["analytics"],"name":"events"});
    value(
        test.post(
            RENAME,
            "w",
            None,
            &json!({"source":source,"destination":{"namespace":["absent"],"name":"events"}}),
        )
        .await,
        404,
    )
    .await;
    empty(
        test.post(RENAME, "w", None, &json!({"source":source,"destination":source}))
            .await,
    )
    .await;
    value(
        test.post("/v1/namespaces/analytics/register", "w", None, &json!({}))
            .await,
        406,
    )
    .await;
    assert_eq!(
        value(test.request(Method::GET, TABLE, "r", None).await, 200).await,
        before
    );
    test.finish().await;
}

#[tokio::test]
async fn drop_accepts_boolean_query_spelling_from_official_client() {
    let test = TestTableHttp::writable().await;
    value(test.post(TABLES, "w", None, &create()).await, 200).await;
    empty(delete(&test, &format!("{TABLE}?purgeRequested=False"), "w", &key()).await).await;
    value(test.request(Method::GET, TABLE, "r", None).await, 404).await;
    test.finish().await;
}

#[tokio::test]
async fn credential_refresh_follows_exact_renamed_identity_and_stops_after_drop() {
    let test = TestTableHttp::vending().await;
    let created = value(test.post(TABLES, "w", None, &create()).await, 200).await;
    let old = created["config"]["client.refresh-credentials-endpoint"]
        .as_str()
        .unwrap();
    value(test.request(Method::GET, old, "w", None).await, 200).await;
    empty(
        test.post(
            RENAME,
            "w",
            None,
            &json!({"source":{"namespace":["analytics"],"name":"events"},
        "destination":{"namespace":["analytics"],"name":"renamed"}}),
        )
        .await,
    )
    .await;
    value(test.request(Method::GET, old, "w", None).await, 404).await;
    let renamed = "/v1/namespaces/analytics/tables/renamed";
    let moved = value(test.request(Method::GET, renamed, "w", None).await, 200).await;
    let current = moved["config"]["client.refresh-credentials-endpoint"]
        .as_str()
        .unwrap();
    assert_ne!(old, current);
    value(test.request(Method::GET, current, "w", None).await, 200).await;
    empty(delete(&test, renamed, "w", &key()).await).await;
    value(test.request(Method::GET, current, "w", None).await, 404).await;
    let replacement = value(test.post(TABLES, "w", None, &create()).await, 200).await;
    assert_ne!(
        replacement["metadata"]["location"],
        created["metadata"]["location"]
    );
    value(test.request(Method::GET, old, "w", None).await, 404).await;
    test.finish().await;
}
