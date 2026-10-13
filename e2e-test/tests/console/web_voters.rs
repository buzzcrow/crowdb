// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::management::{addresses, client, request};
use reqwest::{Client, Method};
use serde_json::{json, Value};

pub(super) async fn setup(count: usize) -> (Client, String, Vec<String>) {
    let ips = addresses(count + 1);
    let http = client();
    let base = format!("http://{}:9090", ips[count]);
    request(&http, &base, Method::POST, "/api/racks", json!({"id":1}), 201).await;
    for (index, ip) in ips[..count].iter().enumerate() {
        request(
            &http,
            &base,
            Method::POST,
            "/api/nodes",
            json!({"id":index+1,"rack_id":1,"host":ip,"ssh_user":""}),
            201,
        )
        .await;
    }
    let nodes: Vec<usize> = (1..=count).collect();
    request(
        &http,
        &base,
        Method::POST,
        "/api/cluster/init",
        json!({"nodes":nodes}),
        201,
    )
    .await;
    (http, base, ips[..count].to_vec())
}

pub(super) async fn replicas(http: &Client, base: &str) -> Value {
    request(
        http,
        base,
        Method::GET,
        "/api/stores/7/groups/70/replicas",
        Value::Null,
        200,
    )
    .await
}
