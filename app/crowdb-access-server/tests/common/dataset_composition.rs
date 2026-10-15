// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crate::common::response;
use reqwest::Client;
use serde_json::{json, Value};
pub fn begin(operation: &str, parent: &Value) -> Value {
    json!({"operation":operation,"parent":parent,"source":{"kind":"all"},"schema":{"version":1,"fields":[{"name":"value","required":true}]},"inherit_parent_splits":false,"parent_split_revision":null,"splits":[],"policy":null})
}
pub async fn prepare(client: &Client, draft: &str, revision: &str, members: &str) -> Value {
    let mut done = false;
    for page in 0..100 {
        let preview = response(
            client
                .post(format!("{draft}/evaluate"))
                .json(&json!({"expected_revision":revision,"expected_page":page.to_string()})),
            200,
        )
        .await;
        if preview["complete"] == true {
            assert_eq!(preview["members"], members);
            done = true;
            break;
        }
    }
    assert!(done, "composition evaluation did not finish");
    for _ in 0..100 {
        let prepared = response(
            client
                .post(format!("{draft}/prepare"))
                .json(&json!({"expected_revision":revision})),
            200,
        )
        .await;
        if prepared["uploads"]["publication_ready"] == true {
            return prepared;
        }
    }
    panic!("composition preparation did not finish");
}
pub async fn publish(client: &Client, root: &str, draft: &str, prepared: &Value, attempt: &str) -> Value {
    let head = response(client.get(format!("{root}/head")), 200).await;
    response(client.post(format!("{draft}/publish")).json(&json!({"attempt":attempt,"snapshot":prepared["preparation"]["snapshot"],"expected_head":head["generation"]})), 200).await
}
pub async fn cleanup(client: &Client, draft: &str) {
    for _ in 0..100 {
        if response(client.post(format!("{draft}/cleanup")), 200).await["complete"] == true {
            return;
        }
    }
    panic!("composition cleanup did not finish");
}
