// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use base64::{engine::general_purpose::STANDARD, Engine};
use reqwest::Client;
use serde_json::{json, Value};

pub async fn response(request: reqwest::RequestBuilder, expected: u16) -> Value {
    let response = request.bearer_auth("secret").send().await.unwrap();
    let status = response.status().as_u16();
    let body: Value = response.json().await.unwrap();
    assert_eq!(status, expected, "{body}");
    body
}
pub async fn fixture(client: &Client, origin: &str) -> (String, String) {
    let v1 = format!("{origin}/api/dataset/v1/namespaces/dataset-ns/datasets/splits");
    response(client.put(&v1), 200).await;
    let samples: Vec<_> = (0..45u8).map(|id| json!({"id":STANDARD.encode([id, 0, 255]), "fields":{"value":{"kind":"inline", "data":STANDARD.encode([id])}}})).collect();
    let publication = json!({"operation":"01010101010101010101010101010101", "parent":null, "manifest":{"schema":[{"name":"value", "required":true}], "samples":samples}});
    let published = response(
        client.post(format!("{v1}/snapshots/publish")).json(&publication),
        200,
    )
    .await;
    (
        v1.replace("/v1/", "/v2/"),
        published["id"].as_str().unwrap().into(),
    )
}
pub fn definitions() -> Value {
    json!([
        {"name":"train/a b", "definition":{"kind":"weighted_partition", "seed":"18446744073709551615", "weights":[{"name":"train/a b", "weight":8},{"name":"test", "weight":2}], "index":0}, "origin":{"kind":"evaluated"}},
        {"name":"test", "definition":{"kind":"weighted_partition", "seed":"18446744073709551615", "weights":[{"name":"train/a b", "weight":8},{"name":"test", "weight":2}], "index":1}, "origin":{"kind":"evaluated"}}
    ])
}
pub fn begin(operation: &str, expected: &Value) -> Value {
    json!({"operation":operation, "expected_revision":expected, "policy":{"overlap":"disjoint", "coverage":"require_complete"}, "splits":definitions()})
}
pub async fn prepare(client: &Client, draft: &str, revision: &str) -> Value {
    for page in 0..10 {
        let p = response(
            client
                .post(format!("{draft}/evaluate"))
                .json(&json!({"expected_revision":revision,"expected_page":page.to_string()})),
            200,
        )
        .await;
        if p["complete"] == true {
            assert_eq!(p["validation"]["scanned"], "45");
            return response(
                client
                    .post(format!("{draft}/prepare"))
                    .json(&json!({"expected_revision":revision})),
                200,
            )
            .await;
        }
    }
    panic!("Split evaluation did not finish");
}
pub async fn publish(client: &Client, draft: &str, prepared: &Value, attempt: &str, head: Value) -> Value {
    response(
        client
            .post(format!("{draft}/publish"))
            .json(&json!({"attempt":attempt,"revision":prepared["revision"],"expected_head":head})),
        200,
    )
    .await
}
