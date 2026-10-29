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

const TABLE: &str = "/v1/namespaces/analytics/tables/events";

async fn value(response: reqwest::Response, status: u16) -> Value {
    let actual = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(actual.as_u16(), status, "{text}");
    serde_json::from_str(&text).unwrap()
}

async fn setup() -> (TestTableHttp, Value) {
    let fixture = TestTableHttp::writable().await;
    let created = value(
        fixture
            .post(
                "/v1/namespaces/analytics/tables",
                "w",
                None,
                &json!({"name":"events","schema":{"type":"struct","schema-id":0,"fields":[]}}),
            )
            .await,
        200,
    )
    .await;
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let snapshot = |id, sequence| {
        json!({"action":"add-snapshot","snapshot":{
        "snapshot-id":id,"sequence-number":sequence,"timestamp-ms":now,"schema-id":0,
        "summary":{"operation":"append"},"manifest-list":format!("{}/metadata/{id}.avro", created["metadata"]["location"].as_str().unwrap())}})
    };
    let current = value(
        fixture
            .post(
                TABLE,
                "w",
                None,
                &json!({"requirements":[],"updates":[
        snapshot(10,1), snapshot(20,2),
        {"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":20}]}),
            )
            .await,
        200,
    )
    .await;
    (fixture, current)
}

fn expire(base: &Value, id: i64) -> Value {
    json!({"x-crowdb-base-metadata-location":base["metadata-location"],"requirements":[
        {"type":"assert-table-uuid","uuid":base["metadata"]["table-uuid"]}],
        "updates":[{"action":"remove-snapshots","snapshot-ids":[id]}]})
}

#[tokio::test]
async fn guarded_expiry_protects_current_and_new_tags_then_allows_unreferenced_history() {
    let (fixture, before) = setup().await;
    let config = value(fixture.request(Method::GET, "/v1/config", "w", None).await, 200).await;
    assert_eq!(config["overrides"]["crowdb.guarded-maintenance"], "true");
    let error = value(fixture.post(TABLE, "w", None, &expire(&before, 20)).await, 409).await;
    assert!(error["error"]["message"].as_str().unwrap().contains("referenced"));
    let tag = json!({"requirements":[],"updates":[{"action":"set-snapshot-ref","ref-name":"keep","type":"tag","snapshot-id":10}]});
    let tagged = value(fixture.post(TABLE, "w", None, &tag).await, 200).await;
    value(fixture.post(TABLE, "w", None, &expire(&before, 10)).await, 409).await;
    value(fixture.post(TABLE, "w", None, &expire(&tagged, 10)).await, 409).await;
    assert_eq!(
        value(fixture.request(Method::GET, TABLE, "r", None).await, 200).await,
        tagged
    );
    let cleared = value(
        fixture
            .post(
                TABLE,
                "w",
                None,
                &json!({"requirements":[],"updates":[{"action":"remove-snapshot-ref","ref-name":"keep"}]}),
            )
            .await,
        200,
    )
    .await;
    let expired = value(fixture.post(TABLE, "w", None, &expire(&cleared, 10)).await, 200).await;
    assert_eq!(expired["metadata"]["snapshots"].as_array().unwrap().len(), 1);
    assert_eq!(expired["metadata"]["current-snapshot-id"], 20);
    fixture.finish().await;
}

#[tokio::test]
async fn guarded_commit_recovers_lost_reply_but_keeps_later_replay_uncertain() {
    let (fixture, before) = setup().await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let identity = format!("{:08x}-{:04x}-7000-8000-000000000001", now >> 16, now & 0xffff);
    let body = expire(&before, 10);
    fixture
        .store
        .lose_reply_kind
        .store(5, std::sync::atomic::Ordering::SeqCst);
    value(fixture.post(TABLE, "w", Some(&identity), &body).await, 503).await;
    let expired = value(fixture.post(TABLE, "w", Some(&identity), &body).await, 200).await;
    assert_eq!(expired["metadata"]["snapshots"].as_array().unwrap().len(), 1);
    let later = value(
        fixture
            .post(
                TABLE,
                "w",
                None,
                &json!({"requirements":[],"updates":[{"action":"set-properties","updates":{"later":"yes"}}]}),
            )
            .await,
        200,
    )
    .await;
    value(fixture.post(TABLE, "w", Some(&identity), &body).await, 503).await;
    assert_eq!(
        value(fixture.request(Method::GET, TABLE, "r", None).await, 200).await,
        later
    );
    fixture.finish().await;
}

#[tokio::test]
async fn guarded_retry_can_publish_prepared_metadata_and_a_late_ref_wins_the_head_cas() {
    for concurrent_ref in [false, true] {
        let (fixture, before) = setup().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let identity = format!("{:08x}-{:04x}-7000-8000-000000000002", now >> 16, now & 0xffff);
        let body = expire(&before, 10);
        fixture
            .store
            .pause_head_cas
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (first, other) = tokio::join!(fixture.post(TABLE, "w", Some(&identity), &body), async {
            fixture.store.head_cas_entered.notified().await;
            let response = if concurrent_ref {
                fixture.post(TABLE, "w", None, &json!({"requirements":[],"updates":[
                        {"action":"set-snapshot-ref","ref-name":"new-branch","type":"branch","snapshot-id":10}]})).await
            } else {
                fixture.post(TABLE, "w", Some(&identity), &body).await
            };
            fixture.store.head_cas_release.notify_one();
            response
        });
        value(other, 200).await;
        value(first, if concurrent_ref { 409 } else { 200 }).await;
        let current = value(fixture.request(Method::GET, TABLE, "r", None).await, 200).await;
        assert_eq!(
            current["metadata"]["snapshots"].as_array().unwrap().len(),
            if concurrent_ref { 2 } else { 1 }
        );
        if concurrent_ref {
            assert_eq!(current["metadata"]["refs"]["new-branch"]["snapshot-id"], 10);
        }
        fixture.finish().await;
    }
}
