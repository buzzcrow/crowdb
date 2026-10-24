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
    let operation = "21212121212121212121212121212121";
    let draft = format!("/compositions/{operation}");
    let definitions = json!([{"name":"train","definition":{"kind":"selection","predicate":{"kind":"all"}},"origin":{"kind":"evaluated"}}]);
    let begin = json!({"operation":operation,"parent":null,"source":{"kind":"all"},"schema":{"version":1,"fields":[{"name":"value","required":true}]},"inherit_parent_splits":false,"parent_split_revision":null,"splits":definitions,"policy":null});
    call(&client, Method::POST, "/compositions", Some(&begin), 200).await;
    let edit = json!({"edit":"22222222222222222222222222222222","expected_revision":"1","samples":[{"kind":"upsert","id":"AAH/","fields":[{"name":"value","value":{"kind":"inline","data":"ZGF0YQ=="}}]}]});
    call(&client, Method::PUT, &draft, Some(&edit), 200).await;
    restart().await;
    assert_eq!(
        call(&client, Method::PUT, &draft, Some(&edit), 200).await["revision"],
        "2"
    );
    let prepared = prepare(&client, &draft).await;
    let snapshot = prepared["preparation"]["snapshot"].as_str().unwrap();
    call(
        &client,
        Method::GET,
        &format!("/snapshots/{snapshot}/summary"),
        None,
        404,
    )
    .await;
    restart().await;
    let publish =
        json!({"attempt":"23232323232323232323232323232323","snapshot":snapshot,"expected_head":null});
    let receipt = call(
        &client,
        Method::POST,
        &format!("{draft}/publish"),
        Some(&publish),
        200,
    )
    .await;
    restart().await;
    assert_eq!(
        call(
            &client,
            Method::POST,
            &format!("{draft}/publish"),
            Some(&publish),
            200
        )
        .await,
        receipt
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

async fn prepare(client: &Client, draft: &str) -> Value {
    let mut page = json!("0");
    let mut done = false;
    for _ in 0..100 {
        let p = call(
            client,
            Method::POST,
            &format!("{draft}/evaluate"),
            Some(&json!({"expected_revision":"2","expected_page":page})),
            200,
        )
        .await;
        if p["complete"] == true {
            done = true;
            break;
        }
        page = p["next_page"].clone();
    }
    assert!(done, "evaluation did not finish");
    for _ in 0..100 {
        let p = call(
            client,
            Method::POST,
            &format!("{draft}/prepare"),
            Some(&json!({"expected_revision":"2"})),
            200,
        )
        .await;
        if p["uploads"]["publication_ready"] == true {
            return p;
        }
    }
    panic!("preparation did not finish")
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
    let operation = "24242424242424242424242424242424";
    let draft = format!("/split-drafts/{operation}");
    let begin = json!({"operation":operation,"expected_revision":old,"policy":{"overlap":"disjoint","coverage":"allow_unassigned"},"splits":[]});
    call(
        client,
        Method::POST,
        &format!("/snapshots/{snapshot}/split-drafts"),
        Some(&begin),
        200,
    )
    .await;
    evaluate_split(client, &draft).await;
    let prepared = call(
        client,
        Method::POST,
        &format!("{draft}/prepare"),
        Some(&json!({"expected_revision":"1"})),
        200,
    )
    .await;
    assert_eq!(
        call(
            client,
            Method::GET,
            &format!(
                "/snapshots/{snapshot}/split-revisions/{}",
                prepared["revision"].as_str().unwrap()
            ),
            None,
            200
        )
        .await,
        prepared
    );
    restart().await;
    let publish = json!({"attempt":"25252525252525252525252525252525","revision":prepared["revision"],"expected_head":head["generation"]});
    let receipt = call(
        client,
        Method::POST,
        &format!("{draft}/publish"),
        Some(&publish),
        200,
    )
    .await;
    restart().await;
    assert_eq!(
        call(
            client,
            Method::POST,
            &format!("{draft}/publish"),
            Some(&publish),
            200
        )
        .await,
        receipt
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

async fn evaluate_split(client: &Client, draft: &str) {
    let mut page = json!("0");
    for _ in 0..100 {
        let preview = call(
            client,
            Method::POST,
            &format!("{draft}/evaluate"),
            Some(&json!({"expected_revision":"1","expected_page":page})),
            200,
        )
        .await;
        if preview["complete"] == true {
            assert_eq!(preview["validation"]["scanned"], "1");
            assert_eq!(preview["validation"]["unassigned"], "1");
            return;
        }
        assert_ne!(preview["next_page"], page, "evaluation must advance");
        page = preview["next_page"].clone();
    }
    panic!("Split evaluation did not finish");
}
