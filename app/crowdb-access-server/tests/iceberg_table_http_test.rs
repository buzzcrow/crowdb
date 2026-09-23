#![cfg(feature = "iceberg")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
mod fixture;

use fixture::TestTableHttp;
use reqwest::Method;
use serde_json::Value;

const PATH: &str = "/v1/namespaces/analytics/tables/events";

#[tokio::test]
async fn table_load_preserves_raw_metadata_and_mode_specific_conditional_responses() {
    let fixture = TestTableHttp::new().await;
    let (_, bytes) = fixture.install("events").await;
    let loaded = fixture.request(Method::GET, PATH, "r", None).await;
    assert_eq!(loaded.status(), 200);
    let etag = loaded.headers()["etag"].to_str().unwrap().to_owned();
    let body = loaded.text().await.unwrap();
    assert!(body.contains(std::str::from_utf8(&bytes).unwrap()));
    let value: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["metadata"]["snapshots"].as_array().unwrap().len(), 3);
    let loaded = fixture
        .request(Method::GET, &format!("{PATH}?snapshots=refs"), "r", Some(&etag))
        .await;
    assert_eq!(loaded.status(), 200);
    let refs_etag = loaded.headers()["etag"].to_str().unwrap().to_owned();
    assert_ne!(refs_etag, etag);
    let body = loaded.text().await.unwrap();
    assert!(body.contains("123456789012345678901234567890"));
    let value: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["metadata"]["snapshots"].as_array().unwrap().len(), 2);
    let unchanged = fixture
        .request(Method::GET, PATH, "r", Some(&format!("W/{etag}")))
        .await;
    assert_eq!(unchanged.status(), 304);
    assert_eq!(unchanged.headers()["etag"], etag);
    assert!(unchanged.bytes().await.unwrap().is_empty());
    let exists = fixture.request(Method::HEAD, PATH, "r", None).await;
    assert_eq!(exists.status(), 204);
    assert!(exists.bytes().await.unwrap().is_empty());
    fixture.finish().await;
}

#[tokio::test]
async fn table_list_has_complete_and_paged_modes_with_bound_tokens() {
    let fixture = TestTableHttp::new().await;
    fixture.install("a+b").await;
    fixture.install("%2F").await;
    for path in [
        "/v1/namespaces/analytics/tables/a+b",
        "/v1/namespaces/analytics/tables/%252F",
    ] {
        assert_eq!(fixture.request(Method::HEAD, path, "r", None).await.status(), 204);
    }
    let path = "/v1/namespaces/analytics/tables?pageSize=1";
    let response = fixture.request(Method::GET, path, "r", None).await;
    assert_eq!(response.status(), 200);
    let complete: Value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    assert_eq!(complete["identifiers"].as_array().unwrap().len(), 2);
    assert!(complete["next-page-token"].is_null());
    let response = fixture
        .request(Method::GET, &format!("{path}&pageToken="), "r", None)
        .await;
    let page: Value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    assert_eq!(page["identifiers"].as_array().unwrap().len(), 1);
    let token = page["next-page-token"].as_str().unwrap();
    assert_eq!(
        fixture
            .request(Method::GET, &format!("{path}&pageToken={token}"), "r", None)
            .await
            .status(),
        200
    );
    assert_eq!(
        fixture
            .request(
                Method::GET,
                &format!("/v1/namespaces/analytics/tables?pageSize=2&pageToken={token}"),
                "r",
                None
            )
            .await
            .status(),
        400
    );
    fixture.finish().await;
}

#[tokio::test]
async fn read_routes_authenticate_reject_bad_parameters_and_do_not_advertise_unfinished_support() {
    let fixture = TestTableHttp::new().await;
    fixture.install("events").await;
    for role in ["r", "w", "m", "c"] {
        assert_eq!(fixture.request(Method::GET, PATH, role, None).await.status(), 200);
    }
    assert_eq!(
        fixture.request(Method::GET, PATH, "invalid", None).await.status(),
        401
    );
    for query in ["snapshots=unknown", "snapshots=all&snapshots=refs", "pageSize=2"] {
        assert_eq!(
            fixture
                .request(Method::GET, &format!("{PATH}?{query}"), "r", None)
                .await
                .status(),
            400
        );
    }
    assert_eq!(fixture.request(Method::POST, PATH, "w", None).await.status(), 406);
    assert_eq!(
        fixture
            .request(Method::HEAD, "/v1/namespaces/analytics/tables/absent", "r", None)
            .await
            .status(),
        404
    );
    let response = fixture
        .request(Method::GET, "/v1/namespaces/absent/tables", "r", None)
        .await;
    assert_eq!(response.status(), 404);
    assert!(response
        .text()
        .await
        .unwrap()
        .contains("NoSuchNamespaceException"));
    let config = fixture
        .request(Method::GET, "/v1/config", "r", None)
        .await
        .text()
        .await
        .unwrap();
    assert!(!config.contains("/tables"));
    fixture.finish().await;
}

#[tokio::test]
async fn corrupt_authority_cannot_become_not_modified() {
    let fixture = TestTableHttp::new().await;
    let (mut head, _) = fixture.install("events").await;
    head.metadata_digest[0] ^= 1;
    fixture.put(
        &crowdb_access_iceberg::table::head_key(head.catalog, head.table),
        &crowdb_access_iceberg::record::StorageRecord::TableHead(Box::new(head)),
    );
    assert_eq!(
        fixture.request(Method::GET, PATH, "r", Some("*")).await.status(),
        503
    );
    fixture.finish().await;
}

#[tokio::test]
async fn read_admission_is_shared_by_complete_and_paged_lists_and_releases_after_errors() {
    use std::sync::{atomic::Ordering, Arc};
    use std::time::Duration;

    let fixture = Arc::new(TestTableHttp::new().await);
    fixture.store.scan_delay_ms.store(500, Ordering::SeqCst);
    let mut readers = Vec::new();
    for index in 0..4 {
        let fixture = fixture.clone();
        readers.push(tokio::spawn(async move {
            let path = if index % 2 == 0 {
                "/v1/namespaces/analytics/tables"
            } else {
                "/v1/namespaces/analytics/tables?pageToken="
            };
            fixture.request(Method::GET, path, "r", None).await.status()
        }));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while fixture.store.scans.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fixture.request(Method::GET, PATH, "r", None).await.status(), 503);
    for reader in readers {
        assert_eq!(reader.await.unwrap(), 200);
    }
    fixture.store.scan_delay_ms.store(0, Ordering::SeqCst);
    for _ in 0..8 {
        assert_eq!(
            fixture
                .request(Method::GET, &format!("{PATH}?snapshots=bad"), "r", None)
                .await
                .status(),
            400
        );
    }
    assert_eq!(
        fixture
            .request(Method::GET, PATH, "r", Some(&"x".repeat(8193)))
            .await
            .status(),
        400
    );
    assert_eq!(fixture.request(Method::GET, PATH, "r", None).await.status(), 404);
    Arc::try_unwrap(fixture).ok().unwrap().finish().await;
}
