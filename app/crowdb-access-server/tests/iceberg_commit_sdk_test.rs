#![cfg(feature = "iceberg-e2e")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::key::IcebergKey;
use serde_json::{json, Value};
use std::{sync::atomic::Ordering, time::Duration};

const TABLE: &str = "/v1/namespaces/analytics/tables/events";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Maven and pinned Apache Iceberg Java dependencies"]
async fn official_commit_loses_real_head_cas_without_rebase_and_keeps_hidden_retry_uncertain() {
    let fixture = fixture::TestTableHttp::writable().await;
    let initial = value(
        fixture
            .post(
                "/v1/namespaces/analytics/tables",
                "w",
                None,
                &json!({"name":"events", "schema":{"type":"struct", "schema-id":0,
                    "fields":[{"id":1,"name":"id","type":"long","required":true}]}}),
            )
            .await,
    )
    .await;
    let identity = request_key();
    fixture.store.pause_head_cas.store(true, Ordering::SeqCst);
    let endpoint = fixture.endpoint();
    let operation_name = identity.clone();
    let client = tokio::task::spawn_blocking(move || run_sdk(&endpoint, &identity));
    tokio::time::timeout(Duration::from_secs(60), fixture.store.head_cas_entered.notified())
        .await
        .expect("SDK did not reach head publication");
    // Ordinary commits publish through one head CAS without a phase journal.
    // Observe the selected head while the losing candidate is still unpublished.
    let paused = value(fixture.request(reqwest::Method::GET, TABLE, "r", None).await).await;
    assert_eq!(
        paused, initial,
        "unpublished candidate must not change the selected table"
    );
    let winner = value(
        fixture
            .post(
                TABLE,
                "w",
                None,
                &json!({"requirements":[], "updates":[
                {"action":"set-properties", "updates":{"winner-only":"visible"}}]}),
            )
            .await,
    )
    .await;
    fixture.store.head_cas_release.notify_one();
    assert!(
        client.await.unwrap().success(),
        "official SDK CAS conflict acceptance failed"
    );
    assert!(
        fixture.store.values.load().keys().all(|key| {
            !matches!(
                IcebergKey::decode(key).unwrap(),
                IcebergKey::Catalog {
                    scope: crowdb_access_iceberg::key::CatalogScope::TableCommitOperation,
                    ..
                }
            )
        }),
        "ordinary commits must not write a phase journal"
    );
    assert_ne!(initial["metadata-location"], winner["metadata-location"]);
    assert!(!winner["metadata-location"]
        .as_str()
        .unwrap()
        .contains(&operation_name));
    let selected = value(fixture.request(reqwest::Method::GET, TABLE, "r", None).await).await;
    assert_eq!(selected, winner);
    let listed = value(
        fixture
            .request(reqwest::Method::GET, "/v1/namespaces/analytics/tables", "r", None)
            .await,
    )
    .await;
    assert_eq!(
        listed["identifiers"],
        json!([{"namespace":["analytics"],"name":"events"}])
    );
    fixture.finish().await;
}

fn request_key() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{:08x}-{:04x}-7000-8000-000000000001", now >> 16, now & 0xffff)
}

fn run_sdk(endpoint: &str, identity: &str) -> std::process::ExitStatus {
    let maven = std::env::var_os("CROWDB_ICEBERG_E2E_MVN").unwrap_or_else(|| "mvn".into());
    std::process::Command::new("timeout")
        .arg("60")
        .arg(maven)
        .args(["-o", "--batch-mode", "--no-transfer-progress", "-f"])
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/common/iceberg_java/pom.xml"
        ))
        .args(["compile", "exec:java", "-Dexec.mainClass=TestIcebergCommitRace"])
        .arg(format!("-Dexec.args={endpoint} {identity}"))
        .status()
        .unwrap()
}

async fn value(response: reqwest::Response) -> Value {
    let status = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(status.as_u16(), 200, "{text}");
    serde_json::from_str(&text).unwrap()
}
