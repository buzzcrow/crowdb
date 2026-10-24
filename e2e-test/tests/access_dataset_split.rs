// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use reqwest::{Client, Method};
use serde_json::{json, Value};

fn root() -> String {
    format!(
        "http://{}:7503/api/dataset/v2/namespaces/dataset-ns/datasets/split-native",
        crate::common::management::addresses(13)[12]
    )
}
async fn call(client: &Client, method: Method, path: &str, body: Option<&Value>, expected: u16) -> Value {
    let mut request = client
        .request(method, format!("{}{path}", root()))
        .bearer_auth("token");
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    let body: Value = response.json().await.unwrap();
    assert_eq!(status.as_u16(), expected, "{path}: {body}");
    body
}
async fn restart() {
    for node in [12, 11] {
        crate::common::control::node("kill", node).await;
    }
    for node in [11, 12] {
        crate::common::control::node("start", node).await;
    }
}

#[tokio::test]
#[ignore = "requires the owned release Dataset fixture"]
async fn split_commit_restart_preserves_candidates_receipts_and_historical_members() {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap();
    call(&client, Method::PUT, "", None, 200).await;
    let snapshot = "21212121212121212121212121212121";
    let resource = format!("/snapshots/{snapshot}");
    let input = json!({"parent":null,"source":{"kind":"all"},"schema":{"version":1,"fields":[{"name":"value","required":true}]},
        "samples":[{"kind":"upsert","id":"AAH/","fields":[{"name":"value","value":{"kind":"inline","data":"ZGF0YQ=="}}]}],
        "splits":[{"name":"train","definition":{"kind":"selection","predicate":{"kind":"all"}},"origin":{"kind":"evaluated"}}],
        "policy":{"overlap":"disjoint","coverage":"allow_unassigned"}});
    let prepared = call(&client, Method::PUT, &resource, Some(&input), 200).await;
    restart().await;
    assert_eq!(
        call(&client, Method::PUT, &resource, Some(&input), 200).await,
        prepared
    );
    complete_result_survives_restart(&client, snapshot).await;
    let publish = json!({"snapshot":snapshot,"expected_head":null});
    let current = call(&client, Method::PUT, "/head", Some(&publish), 200).await;
    restart().await;
    assert_eq!(
        call(&client, Method::PUT, "/head", Some(&publish), 200).await,
        current
    );
    assert_eq!(
        call(
            &client,
            Method::GET,
            "/commits/23232323232323232323232323232323",
            None,
            200
        )
        .await["receipt"],
        Value::Null
    );
    revise(&client, snapshot).await;
    assert_eq!(
        call(
            &client,
            Method::GET,
            &format!("/snapshots/{snapshot}/samples"),
            None,
            200
        )
        .await["entries"],
        json!(["AAH/"])
    );
    assert_eq!(
        call(
            &client,
            Method::GET,
            &format!("/snapshots/{snapshot}/preview?sample=AAH%2F&field=value"),
            None,
            200
        )
        .await["data"],
        "ZGF0YQ=="
    );
}

async fn complete_result_survives_restart(client: &Client, snapshot: &str) {
    let summary = call(
        client,
        Method::GET,
        &format!("/snapshots/{snapshot}/summary"),
        None,
        200,
    )
    .await;
    assert_eq!(summary["sample_count"], "1");
    assert_eq!(summary["inspection"], "ready");
    assert_eq!(
        call(client, Method::GET, "/head", None, 200).await["latest"],
        Value::Null
    );
    restart().await;
    assert_eq!(
        call(
            client,
            Method::GET,
            &format!("/snapshots/{snapshot}/summary"),
            None,
            200
        )
        .await,
        summary
    );
}

async fn revise(client: &Client, snapshot: &str) {
    let data_head = call(client, Method::GET, "/head", None, 200).await;
    let head = call(
        client,
        Method::GET,
        &format!("/snapshots/{snapshot}/split-head"),
        None,
        200,
    )
    .await;
    let old = head["revision"].as_str().unwrap();
    let revision = "24242424242424242424242424242424";
    let resource = format!("/snapshots/{snapshot}/split-revisions/{revision}");
    let input = json!({"policy":{"overlap":"disjoint","coverage":"allow_unassigned"},"splits":[]});
    let prepared = call(client, Method::PUT, &resource, Some(&input), 200).await;
    assert_eq!(call(client, Method::GET, &resource, None, 200).await, prepared);
    restart().await;
    assert_eq!(
        call(client, Method::PUT, &resource, Some(&input), 200).await,
        prepared
    );
    let update = json!({"revision":revision,"expected_revision":old,"expected_head":head["generation"]});
    let head_url = format!("/snapshots/{snapshot}/split-head");
    let current = call(client, Method::PUT, &head_url, Some(&update), 200).await;
    restart().await;
    assert_eq!(
        call(client, Method::PUT, &head_url, Some(&update), 200).await,
        current
    );
    assert_eq!(call(client, Method::GET, "/head", None, 200).await, data_head);
    assert_eq!(
        call(
            client,
            Method::GET,
            "/commits/25252525252525252525252525252525",
            None,
            200
        )
        .await["receipt"],
        Value::Null
    );
    assert_eq!(
        call(
            client,
            Method::GET,
            &format!("/snapshots/{snapshot}/split-revisions/{old}/splits/train/members"),
            None,
            200
        )
        .await["entries"],
        json!(["AAH/"])
    );
}
