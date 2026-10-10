// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::management::{addresses, client, origin, request};
use reqwest::{Client, Method};
use serde_json::{json, Value};

const REMOTES: &str = "/stores/0/groups/1/remotes";

#[tokio::test]
#[ignore = "requires the release-container fixture"]
async fn replica_lifecycle() {
    let ip = &addresses(1)[0];
    let base = origin(ip);
    let client = client();
    let initial = request(&client, &base, Method::GET, REMOTES, Value::Null, 200).await;
    assert!(initial["remotes"].as_array().unwrap().is_empty());
    request(
        &client,
        &base,
        Method::POST,
        REMOTES,
        json!([{ "replica_id": 1, "endpoint": format!("{ip}:7001") }]),
        400,
    )
    .await;
    for (replica, status) in [(1, 400), (99, 404)] {
        request(
            &client,
            &base,
            Method::DELETE,
            &format!("{REMOTES}/{replica}"),
            Value::Null,
            status,
        )
        .await;
    }
    request(
        &client,
        &base,
        Method::POST,
        REMOTES,
        json!([
            {"replica_id": 2, "endpoint": format!("{ip}:7009")},
            {"replica_id": 3, "endpoint": format!("{ip}:7010")}
        ]),
        200,
    )
    .await;
    let added = request(&client, &base, Method::GET, REMOTES, Value::Null, 200).await;
    assert_eq!(added["remotes"].as_array().unwrap().len(), 2);
    request(
        &client,
        &base,
        Method::DELETE,
        &format!("{REMOTES}/2"),
        Value::Null,
        200,
    )
    .await;
    let remaining = request(&client, &base, Method::GET, REMOTES, Value::Null, 200).await;
    assert_eq!(remaining["remotes"].as_array().unwrap().len(), 1);
    assert_eq!(remaining["remotes"][0]["replica_id"], 3);
}

async fn epoch_write(client: &Client, base: &str, path: &str, epoch: u64, body: &Value, status: u16) {
    let response = client
        .post(format!("{base}{path}"))
        .header("x-crowdb-membership-epoch", epoch.to_string())
        .json(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status().as_u16(),
        status,
        "epoch {epoch}: {}",
        response.text().await.unwrap()
    );
}

#[tokio::test]
#[ignore = "requires the release-container fixture"]
async fn epoch_replay_and_authority_bootstrap() {
    let ip = &addresses(1)[0];
    let base = origin(ip);
    let client = client();
    let remote = json!([{ "replica_id": 2, "endpoint": format!("{ip}:7009"), "voting": true }]);
    epoch_write(&client, &base, REMOTES, 99, &remote, 409).await;
    let rejected = request(&client, &base, Method::GET, REMOTES, Value::Null, 200).await;
    assert!(rejected["remotes"].as_array().unwrap().is_empty());
    epoch_write(&client, &base, REMOTES, 1, &remote, 200).await;
    epoch_write(&client, &base, REMOTES, 1, &remote, 200).await;
    let conflicting = json!([{ "replica_id": 2, "endpoint": format!("{ip}:7010"), "voting": true }]);
    epoch_write(&client, &base, REMOTES, 1, &conflicting, 409).await;
    let persisted = request(&client, &base, Method::GET, REMOTES, Value::Null, 200).await;
    assert_eq!(persisted["remotes"][0]["endpoint"], format!("{ip}:7009"));
    request(
        &client,
        &base,
        Method::POST,
        "/stores/0/groups",
        json!({"group_id": 2, "replica_id": 1}),
        201,
    )
    .await;
    // A fresh local group must accept an authority already at epoch two.
    epoch_write(&client, &base, "/stores/0/groups/2/remotes", 2, &remote, 200).await;
}
