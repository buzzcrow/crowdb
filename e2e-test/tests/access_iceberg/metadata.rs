// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release Iceberg frontends"]
async fn metadata_create_and_commit_stay_within_operation_budget() {
    let ips = crate::common::management::addresses(14);
    let origin = format!("http://{}:7502", ips[12]);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let namespace = client
        .post(format!("{origin}/v1/namespaces"))
        .bearer_auth("w".repeat(32))
        .json(&serde_json::json!({"namespace": ["budget"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(namespace.status(), 200, "{}", namespace.text().await.unwrap());

    let before = metadata_counters(&client, &origin).await;
    let started = std::time::Instant::now();
    let created = client
        .post(format!("{origin}/v1/namespaces/budget/tables"))
        .bearer_auth("w".repeat(32))
        .json(
            &serde_json::json!({"name":"small", "schema":{"type":"struct", "schema-id":0,
            "fields":[{"id":1,"name":"value","type":"long","required":false}]}}),
        )
        .send()
        .await
        .unwrap();
    let create_duration = started.elapsed();
    assert_eq!(created.status(), 200, "{}", created.text().await.unwrap());
    let after_create = metadata_counters(&client, &origin).await;

    let started = std::time::Instant::now();
    let updated = client
        .post(format!("{origin}/v1/namespaces/budget/tables/small"))
        .bearer_auth("w".repeat(32))
        .json(&serde_json::json!({"requirements":[], "updates":[
            {"action":"set-properties", "updates":{"sample":"done"}}]}))
        .send()
        .await
        .unwrap();
    let update_duration = started.elapsed();
    assert_eq!(updated.status(), 200, "{}", updated.text().await.unwrap());
    let after_update = metadata_counters(&client, &origin).await;

    let create_get = after_create.0 - before.0;
    let create_cas = after_create.1 - before.1;
    let update_get = after_update.0 - after_create.0;
    let update_cas = after_update.1 - after_create.1;
    eprintln!(
        "metadata budget: create={create_duration:?} get={create_get} cas={create_cas}; \
         update={update_duration:?} get={update_get} cas={update_cas}"
    );
    assert!(create_duration < Duration::from_secs(2));
    assert!(update_duration < Duration::from_secs(2));
    assert!(
        create_get <= 160 && create_cas <= 30,
        "table create performed too many storage operations"
    );
    assert!(
        update_get <= 160 && update_cas <= 20,
        "table update performed too many storage operations"
    );
}

async fn metadata_counters(client: &reqwest::Client, origin: &str) -> (u64, u64) {
    let response = client
        .get(format!("{origin}/_crowdb/metrics"))
        .bearer_auth("m".repeat(32))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let value: serde_json::Value = response.json().await.unwrap();
    let catalog = &value["routes"][4][0]["catalog"];
    (
        catalog["get"].as_u64().unwrap(),
        catalog["compare_exchange"].as_u64().unwrap(),
    )
}
