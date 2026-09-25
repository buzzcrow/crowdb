#![cfg(feature = "iceberg")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds},
    file::{FileGrantIssuer, FileOperation, TableLocation},
    wire::BearerAuthenticator,
};
use fixture::TestTableHttp;
use reqwest::Method;
use serde_json::{json, Value};

#[tokio::test]
async fn credential_refresh_requires_selected_version_read_capability() {
    let fixture = TestTableHttp::vending_with_capabilities(0x0033).await;
    fixture.install("events").await;
    let path = "/v1/namespaces/analytics/tables/events/credentials";
    assert_eq!(fixture.request(Method::GET, path, "r", None).await.status(), 406);
    let repository = CatalogRepository::new(fixture.store.clone(), ClearBounds::default()).unwrap();
    common::activate_bits(&repository, 0x3fff).await;
    assert_eq!(fixture.request(Method::GET, path, "r", None).await.status(), 200);
    fixture.finish().await;
}

#[tokio::test]
async fn read_only_format_profile_never_vends_file_mutation_permission() {
    let fixture = TestTableHttp::vending_with_capabilities(0x0300).await;
    fixture.install("events").await;
    let path = "/v1/namespaces/analytics/tables/events/credentials";
    let response = fixture.request(Method::GET, path, "w", None).await;
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    let config = &body["storage-credentials"][0]["config"];
    let authentication =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let issuer = FileGrantIssuer::new(authentication.namespace_token_key(), 900_000).unwrap();
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let grant = issuer
        .verify(
            config["s3.access-key-id"].as_str().unwrap(),
            config["s3.session-token"].as_str().unwrap(),
            fixture.context,
            now,
        )
        .unwrap();
    assert!(grant.grant().operations.allows(FileOperation::Get));
    assert!(!grant.grant().operations.allows(FileOperation::Put));
    let repository = CatalogRepository::new(fixture.store.clone(), ClearBounds::default()).unwrap();
    common::activate_bits(&repository, 0x3fff).await;
    let response = fixture.request(Method::GET, path, "w", None).await;
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    let config = &body["storage-credentials"][0]["config"];
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let grant = issuer
        .verify(
            config["s3.access-key-id"].as_str().unwrap(),
            config["s3.session-token"].as_str().unwrap(),
            fixture.context,
            now,
        )
        .unwrap();
    assert!(grant.grant().operations.allows(FileOperation::Put));
    fixture.finish().await;
}

async fn draft(fixture: &TestTableHttp) -> Value {
    let response = fixture
        .post(
            "/v1/namespaces/analytics/tables",
            "w",
            None,
            &json!({"name":"events","stage-create":true,"schema":{"type":"struct","fields":[]}}),
        )
        .await;
    let status = response.status();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&bytes));
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn same_name_drafts_refresh_only_the_exact_original_writer_scope() {
    let fixture = TestTableHttp::vending().await;
    let auth =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let issuer = FileGrantIssuer::new(auth.namespace_token_key(), 900_000).unwrap();
    let first = draft(&fixture).await;
    let second = draft(&fixture).await;
    assert_ne!(first["metadata"]["location"], second["metadata"]["location"]);
    for draft in [&first, &second] {
        let path = draft["config"]["client.refresh-credentials-endpoint"]
            .as_str()
            .unwrap();
        for role in ["r", "m", "c"] {
            assert_eq!(fixture.request(Method::GET, path, role, None).await.status(), 404);
        }
        let response = fixture.request(Method::GET, path, "w", None).await;
        assert_eq!(response.status(), 200);
        let value: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(value["storage-credentials"].as_array().unwrap().len(), 1);
        let credential = &value["storage-credentials"][0];
        assert_eq!(
            credential["prefix"],
            format!("{}/", draft["metadata"]["location"].as_str().unwrap())
        );
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap();
        let grant = issuer
            .verify(
                credential["config"]["s3.access-key-id"].as_str().unwrap(),
                credential["config"]["s3.session-token"].as_str().unwrap(),
                fixture.context,
                now,
            )
            .unwrap();
        let table: TableLocation = format!("{}/", draft["metadata"]["location"].as_str().unwrap())
            .parse()
            .unwrap();
        assert_eq!(grant.grant().table, table.table);
        assert!(grant.grant().operations.allows(FileOperation::Put));
        let wrong = path.replace("/events/", "/other/");
        assert_eq!(
            fixture.request(Method::GET, &wrong, "w", None).await.status(),
            404
        );
        assert_eq!(
            fixture
                .request(
                    Method::GET,
                    &format!("{path}&table-id={}", table.table),
                    "w",
                    None
                )
                .await
                .status(),
            400
        );
    }
    assert_eq!(
        fixture
            .request(
                Method::GET,
                "/v1/namespaces/analytics/tables/events/credentials",
                "w",
                None
            )
            .await
            .status(),
        404
    );
    expire_first(&fixture, &first, &second).await;
    fixture.finish().await;
}

async fn expire_first(fixture: &TestTableHttp, first: &Value, second: &Value) {
    let first_table: TableLocation = format!("{}/", first["metadata"]["location"].as_str().unwrap())
        .parse()
        .unwrap();
    let creator = crowdb_access_iceberg::commit::TableCreator::new(
        fixture.store.clone(),
        std::sync::Arc::new(blocks::TestFileBlocks::default()),
    );
    assert!(creator
        .expire_stage(fixture.context, first_table.table, i64::MAX)
        .await
        .unwrap());
    assert_eq!(
        fixture
            .request(
                Method::GET,
                first["config"]["client.refresh-credentials-endpoint"]
                    .as_str()
                    .unwrap(),
                "w",
                None
            )
            .await
            .status(),
        404
    );
    assert_eq!(
        fixture
            .request(
                Method::GET,
                second["config"]["client.refresh-credentials-endpoint"]
                    .as_str()
                    .unwrap(),
                "w",
                None
            )
            .await
            .status(),
        200
    );
}

#[tokio::test]
async fn published_table_credentials_preserve_read_only_roles() {
    let fixture = TestTableHttp::vending().await;
    let (head, _) = fixture.install("events").await;
    let auth =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let issuer = FileGrantIssuer::new(auth.namespace_token_key(), 900_000).unwrap();
    for role in ["r", "w", "m", "c"] {
        let response = fixture
            .request(
                Method::GET,
                "/v1/namespaces/analytics/tables/events/credentials",
                role,
                None,
            )
            .await;
        assert_eq!(response.status(), 200);
        let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        let config = &body["storage-credentials"][0]["config"];
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap();
        let grant = issuer
            .verify(
                config["s3.access-key-id"].as_str().unwrap(),
                config["s3.session-token"].as_str().unwrap(),
                fixture.context,
                now,
            )
            .unwrap();
        assert_eq!(grant.grant().table, head.table);
        assert_eq!(grant.grant().operations.allows(FileOperation::Put), role == "w");
        assert!(grant.grant().operations.allows(FileOperation::Get));
    }
    fixture.finish().await;
}
