#![cfg(feature = "iceberg")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::catalog::{CatalogRepository, ClearBounds};
use fixture::TestTableHttp;
use reqwest::Method;
use serde_json::{json, Value};

const TABLES: &str = "/v1/namespaces/analytics/tables";
const TABLE: &str = "/v1/namespaces/analytics/tables/events";

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

fn create(staged: bool) -> Value {
    json!({"name":"events", "stage-create":staged, "schema":{"type":"struct","schema-id":0,
        "fields":[{"id":91,"name":"id","type":"long","required":true}]}})
}

async fn value(response: reqwest::Response, status: u16) -> Value {
    let actual = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(actual.as_u16(), status, "{text}");
    serde_json::from_str(&text).unwrap()
}

#[tokio::test]
async fn create_and_upgrade_follow_the_selected_persisted_version_profile() {
    let fixture = TestTableHttp::writable_with_capabilities(0x003f).await;
    let refused = value(fixture.post(TABLES, "w", None, &create(false)).await, 406).await;
    assert_eq!(refused["error"]["type"], "UnsupportedOperationException");
    let mut v1 = create(false);
    v1["properties"] = json!({"format-version":"1"});
    let created = value(fixture.post(TABLES, "w", None, &v1).await, 200).await;
    assert_eq!(created["metadata"]["format-version"], 1);
    let upgrade =
        json!({"requirements":[],"updates":[{"action":"upgrade-format-version","format-version":2}]});
    value(fixture.post(TABLE, "w", None, &upgrade).await, 406).await;
    let repository = CatalogRepository::new(fixture.store.clone(), ClearBounds::default()).unwrap();
    common::activate_bits(&repository, 0x1fff).await;
    let upgraded = value(fixture.post(TABLE, "w", None, &upgrade).await, 200).await;
    assert_eq!(upgraded["metadata"]["format-version"], 2);
    fixture.finish().await;
}

#[tokio::test]
async fn direct_v1_to_v3_upgrade_requires_both_persisted_edges() {
    let fixture = TestTableHttp::writable_with_capabilities(0x1fff).await;
    let mut v1 = create(false);
    v1["properties"] = json!({"format-version":"1"});
    value(fixture.post(TABLES, "w", None, &v1).await, 200).await;
    let upgrade =
        json!({"requirements":[],"updates":[{"action":"upgrade-format-version","format-version":3}]});
    value(fixture.post(TABLE, "w", None, &upgrade).await, 406).await;
    let repository = CatalogRepository::new(fixture.store.clone(), ClearBounds::default()).unwrap();
    common::activate_bits(&repository, 0x3fff).await;
    let updated = value(fixture.post(TABLE, "w", None, &upgrade).await, 200).await;
    assert_eq!(updated["metadata"]["format-version"], 3);
    fixture.finish().await;
}

#[tokio::test]
async fn create_and_update_replay_exact_results_and_enforce_independent_writer() {
    let fixture = TestTableHttp::writable().await;
    for role in ["r", "m", "c"] {
        value(fixture.post(TABLES, role, None, &create(false)).await, 403).await;
    }
    let identity = key();
    let created = value(
        fixture.post(TABLES, "w", Some(&identity), &create(false)).await,
        200,
    )
    .await;
    assert_eq!(created["metadata"]["schemas"][0]["fields"][0]["id"], 1);
    assert_eq!(
        value(
            fixture.post(TABLES, "w", Some(&identity), &create(false)).await,
            200
        )
        .await,
        created
    );
    let mut changed = create(false);
    changed["name"] = json!("other");
    value(fixture.post(TABLES, "w", Some(&identity), &changed).await, 409).await;
    let identity = key();
    let update = json!({"requirements":[{"type":"assert-table-uuid","uuid":created["metadata"]["table-uuid"]}],
        "updates":[{"action":"set-properties","updates":{"owner":"writer"}}]});
    let committed = value(fixture.post(TABLE, "w", Some(&identity), &update).await, 200).await;
    assert_eq!(committed["metadata"]["properties"]["owner"], "writer");
    assert_eq!(
        value(fixture.post(TABLE, "w", Some(&identity), &update).await, 200).await,
        committed
    );
    assert_eq!(
        value(fixture.request(Method::GET, TABLE, "r", None).await, 200).await,
        committed
    );
    fixture.finish().await;
}

#[tokio::test]
async fn failed_requirement_is_durable_and_does_not_publish_or_rebase() {
    let fixture = TestTableHttp::writable().await;
    let before = value(fixture.post(TABLES, "w", None, &create(false)).await, 200).await;
    let identity = key();
    let update = json!({"requirements":[{"type":"assert-current-schema-id","current-schema-id":123}],
        "updates":[{"action":"set-properties","updates":{"bad":"value"}}]});
    let rejected = value(fixture.post(TABLE, "w", Some(&identity), &update).await, 409).await;
    assert_eq!(rejected["error"]["type"], "CommitFailedException");
    assert_eq!(
        value(fixture.post(TABLE, "w", Some(&identity), &update).await, 409).await,
        rejected
    );
    assert_eq!(
        value(fixture.request(Method::GET, TABLE, "r", None).await, 200).await,
        before
    );
    let operation: crowdb_access_iceberg::key::OperationId = identity.parse().unwrap();
    let journal = crowdb_access_iceberg::commit::TableCommitJournal::new(fixture.store.clone());
    assert_eq!(
        journal
            .load(fixture.context, operation)
            .await
            .unwrap()
            .unwrap()
            .phase,
        crowdb_access_iceberg::commit::TableCommitPhase::Rejected
    );
    fixture.finish().await;
}

#[tokio::test]
async fn staged_create_is_invisible_until_standard_assert_create_commit() {
    let fixture = TestTableHttp::writable().await;
    let draft = value(fixture.post(TABLES, "w", None, &create(true)).await, 200).await;
    assert!(draft.get("metadata-location").is_none());
    assert_eq!(
        fixture.request(Method::HEAD, TABLE, "r", None).await.status(),
        404
    );
    let metadata = &draft["metadata"];
    let body = json!({"requirements":[{"type":"assert-create"}],"updates":[
        {"action":"assign-uuid","uuid":metadata["table-uuid"]},
        {"action":"upgrade-format-version","format-version":metadata["format-version"]},
        {"action":"add-schema","schema":metadata["schemas"][0]},
        {"action":"set-current-schema","schema-id":-1},
        {"action":"add-spec","spec":metadata["partition-specs"][0]},
        {"action":"set-default-spec","spec-id":-1},
        {"action":"add-sort-order","sort-order":metadata["sort-orders"][0]},
        {"action":"set-default-sort-order","sort-order-id":-1},
        {"action":"set-location","location":metadata["location"]}]});
    let identity = key();
    let committed = value(fixture.post(TABLE, "w", Some(&identity), &body).await, 200).await;
    assert_eq!(committed["metadata"]["table-uuid"], metadata["table-uuid"]);
    assert_eq!(
        value(fixture.post(TABLE, "w", Some(&identity), &body).await, 200).await,
        committed
    );
    assert_eq!(
        fixture.request(Method::HEAD, TABLE, "r", None).await.status(),
        204
    );
    fixture.finish().await;
}

#[tokio::test]
async fn concurrent_identical_commit_keys_cannot_rebase_or_finalize_a_transient_conflict() {
    let fixture = TestTableHttp::writable().await;
    value(fixture.post(TABLES, "w", None, &create(false)).await, 200).await;
    let identity = key();
    let body =
        json!({"requirements":[],"updates":[{"action":"set-properties","updates":{"concurrent":"once"}}]});
    let (first, second) = tokio::join!(
        fixture.post(TABLE, "w", Some(&identity), &body),
        fixture.post(TABLE, "w", Some(&identity), &body)
    );
    for response in [first, second] {
        assert!(
            matches!(response.status().as_u16(), 200 | 503),
            "{}",
            response.text().await.unwrap()
        );
    }
    let result = value(fixture.post(TABLE, "w", Some(&identity), &body).await, 200).await;
    assert_eq!(result["metadata"]["properties"]["concurrent"], "once");
    let selected = crowdb_access_iceberg::table::TableRepository::new(fixture.store.clone())
        .select(fixture.context, fixture.namespace, "events")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.head.generation, 2);
    value(
        fixture
            .post(
                "/v1/namespaces/analytics/tables/other",
                "w",
                Some(&identity),
                &body,
            )
            .await,
        409,
    )
    .await;
    fixture.finish().await;
}

#[tokio::test]
async fn foreign_create_location_is_a_replayable_client_error() {
    let fixture = TestTableHttp::writable().await;
    let mut body = create(false);
    body["location"] = json!("s3://external/table");
    let identity = key();
    let first = value(fixture.post(TABLES, "w", Some(&identity), &body).await, 400).await;
    assert_eq!(
        value(fixture.post(TABLES, "w", Some(&identity), &body).await, 400).await,
        first
    );
    assert_eq!(
        fixture.request(Method::HEAD, TABLE, "r", None).await.status(),
        404
    );
    fixture.finish().await;
}

#[tokio::test]
async fn missing_selected_file_rejection_survives_lost_durable_reply() {
    let fixture = TestTableHttp::writable().await;
    let created = value(fixture.post(TABLES, "w", None, &create(false)).await, 200).await;
    let identity = key();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let body = json!({"requirements":[],"updates":[{"action":"add-snapshot","snapshot":{
        "snapshot-id":123,"sequence-number":1,"timestamp-ms":u64::try_from(now).unwrap(),"schema-id":0,
        "summary":{"operation":"append"},"manifest-list":format!("{}/metadata/missing.avro", created["metadata"]["location"].as_str().unwrap())}},
        {"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":123}]});
    fixture
        .store
        .lose_reply_kind
        .store(4, std::sync::atomic::Ordering::SeqCst);
    value(fixture.post(TABLE, "w", Some(&identity), &body).await, 503).await;
    let result = value(fixture.post(TABLE, "w", Some(&identity), &body).await, 400).await;
    assert_eq!(result["error"]["type"], "BadRequestException");
    assert_eq!(
        value(fixture.request(Method::GET, TABLE, "r", None).await, 200).await,
        created
    );
    fixture.finish().await;
}
