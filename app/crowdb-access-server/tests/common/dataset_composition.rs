// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crate::common::response;
use reqwest::Client;
use serde_json::{json, Value};
pub fn input(parent: &Value) -> Value {
    json!({"parent":parent,"source":{"kind":"all"},"schema":{"version":1,"fields":[{"name":"value","required":true}]},"samples":[],"splits":[],"policy":{"overlap":"disjoint","coverage":"allow_unassigned"}})
}
pub async fn prepare(client: &Client, root: &str, snapshot: &str, input: &Value) -> Value {
    response(
        client.put(format!("{root}/snapshots/{snapshot}")).json(input),
        200,
    )
    .await
}
pub async fn publish(client: &Client, root: &str, prepared: &Value, expected_head: &Value) -> Value {
    response(
        client
            .put(format!("{root}/head"))
            .json(&json!({"snapshot":prepared["snapshot"],"expected_head":expected_head})),
        200,
    )
    .await
}
