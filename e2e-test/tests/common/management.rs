// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use reqwest::{Client, Method};
use serde_json::Value;
use std::time::{Duration, Instant};

pub(crate) fn addresses(count: usize) -> Vec<String> {
    let addresses: Vec<String> =
        serde_json::from_str(&std::env::var("CROWDB_E2E_RPC").expect("fixture IPs")).unwrap();
    assert_eq!(addresses.len(), count, "case needs its declared node profile");
    addresses
}

pub(crate) fn client() -> Client {
    Client::builder().no_proxy().build().unwrap()
}

pub(crate) fn origin(address: &str) -> String {
    format!("http://{address}:7000")
}

pub(crate) async fn request(
    client: &Client,
    base: &str,
    method: Method,
    path: &str,
    body: Value,
    status: u16,
) -> Value {
    let response = client
        .request(method, format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let actual = response.status().as_u16();
    let text = response.text().await.unwrap();
    assert_eq!(actual, status, "{path}: {text}");
    if text.is_empty() || status >= 400 {
        Value::Null
    } else {
        serde_json::from_str(&text).expect("management response is JSON")
    }
}

pub(crate) async fn wait_for_leader(client: &Client, base: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let topology = request(client, base, Method::GET, "/topology", Value::Null, 200).await;
        if topology["stores"][0]["groups"][0]["leader_id"]
            .as_u64()
            .unwrap_or(0)
            != 0
        {
            return topology;
        }
        assert!(Instant::now() < deadline, "no leader: {topology}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
