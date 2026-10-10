// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::management::{addresses, client, origin, request};
use reqwest::Method;
use serde_json::{json, Value};

async fn create_layout(client: &reqwest::Client, base: &str) {
    for store in 1..3 {
        request(
            client,
            base,
            Method::POST,
            "/stores",
            json!({"store_id": store}),
            201,
        )
        .await;
        request(
            client,
            base,
            Method::POST,
            &format!("/stores/{store}/groups"),
            json!({"group_id": 1, "replica_id": store + 1}),
            201,
        )
        .await;
    }
    for store in 0..3 {
        for group in [2, 3] {
            request(
                client,
                base,
                Method::POST,
                &format!("/stores/{store}/groups"),
                json!({"group_id": group, "replica_id": store + 1}),
                201,
            )
            .await;
        }
        let detail = request(
            client,
            base,
            Method::GET,
            &format!("/stores/{store}"),
            Value::Null,
            200,
        )
        .await;
        assert_eq!(detail["groups"].as_array().unwrap().len(), 3);
    }
}

#[tokio::test]
#[ignore = "requires two independent release-container nodes"]
async fn topology_wiring_and_progressive_setup() {
    let ips = addresses(2);
    let base = origin(&ips[0]);
    let client = client();
    let mut peer = request(
        &client,
        &origin(&ips[1]),
        Method::GET,
        "/topology",
        Value::Null,
        200,
    )
    .await;
    let address = peer["stores"][0]["listen_addr"].as_str().unwrap();
    peer["stores"][0]["listen_addr"] = json!(format!("{}:{}", ips[1], address.rsplit(':').next().unwrap()));
    request(
        &client,
        &base,
        Method::POST,
        "/stores/0/groups/1/remotes/batch",
        peer,
        200,
    )
    .await;
    let remotes = request(
        &client,
        &base,
        Method::GET,
        "/stores/0/groups/1/remotes",
        Value::Null,
        200,
    )
    .await;
    assert_eq!(remotes["remotes"].as_array().unwrap().len(), 1);
    assert_eq!(remotes["remotes"][0]["replica_id"], 2);
    assert_eq!(remotes["remotes"][0]["endpoint"], format!("{}:7001", ips[1]));

    create_layout(&client, &base).await;
    let mut topology = request(&client, &base, Method::GET, "/topology", Value::Null, 200).await;
    assert_eq!(topology["stores"].as_array().unwrap().len(), 3);
    for store in topology["stores"].as_array_mut().unwrap() {
        let address = store["listen_addr"].as_str().unwrap();
        store["listen_addr"] = json!(format!("{}:{}", ips[0], address.rsplit(':').next().unwrap()));
    }
    request(
        &client,
        &base,
        Method::POST,
        "/stores/0/groups/1/remotes/batch",
        topology,
        200,
    )
    .await;
    let remotes = request(
        &client,
        &base,
        Method::GET,
        "/stores/0/groups/1/remotes",
        Value::Null,
        200,
    )
    .await;
    assert_eq!(remotes["remotes"].as_array().unwrap().len(), 2);
    request(&client, &base, Method::DELETE, "/stores/2", Value::Null, 200).await;
    let remaining = request(&client, &base, Method::GET, "/stores", Value::Null, 200).await;
    assert_eq!(remaining["stores"].as_array().unwrap().len(), 2);
}
