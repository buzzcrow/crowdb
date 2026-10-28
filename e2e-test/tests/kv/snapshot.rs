// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::cluster::{leader, topology};
use crate::common::management::{addresses, client, origin, request};
use crowdb_paxos_kv::rpc::ReadMode;
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient, PaxosKvRpcTransport};
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

async fn peers(http: &Client, ip: &str, store: usize, body: Value) {
    request(
        http,
        &origin(ip),
        Method::POST,
        &format!("/stores/{store}/groups/1/remotes"),
        body,
        200,
    )
    .await;
}

async fn local_value(
    transport: &PaxosKvRpcTransport,
    ip: &str,
    key: &[u8],
    expected: &[u8],
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let response = transport
            .send_get(
                &format!("http://{ip}:7001"),
                key,
                9101,
                9101,
                1,
                ReadMode::MinSlot,
                0,
            )
            .await
            .unwrap();
        if !response.not_found {
            assert!(response.ok, "local get failed: {}", response.error);
            assert_eq!(response.value.as_ref(), expected);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "local replica has not received expected value"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore = "requires two voting release-container nodes and one bare store"]
async fn snapshot_import_precedes_wiring_and_catches_up_tail() {
    let ips = addresses(3);
    let http = client();
    for index in 0..2 {
        let other = 1 - index;
        peers(
            &http,
            &ips[index],
            index,
            json!([{"replica_id": other + 1, "endpoint": format!("{}:7001", ips[other]), "voting": true}]),
        )
        .await;
    }
    let leader = leader(&http, &ips[..2], 1).await;
    let kv = PaxosKvClient::new(ClientConfig::new(ips[..2].iter().map(|ip| origin(ip)).collect()));
    for (key, value) in [(b"k1", b"v1"), (b"k2", b"v2")] {
        kv.put(0, 1, key, value, None).await.unwrap();
    }
    let bare = topology(&http, &ips[2]).await;
    assert!(bare["stores"][0]["groups"].as_array().is_none_or(Vec::is_empty));
    request(
        &http,
        &origin(&ips[2]),
        Method::POST,
        "/stores/2/groups/1/join",
        json!({"replica_id": 9, "peer_endpoint": format!("{}:7001", ips[leader])}),
        201,
    )
    .await;
    let transport = PaxosKvRpcTransport::new();
    for (key, value) in [(b"k1", b"v1"), (b"k2", b"v2")] {
        local_value(&transport, &ips[2], key, value, Duration::from_secs(1)).await;
    }
    let remotes = request(
        &http,
        &origin(&ips[2]),
        Method::GET,
        "/stores/2/groups/1/remotes",
        Value::Null,
        200,
    )
    .await;
    assert!(
        remotes["remotes"].as_array().unwrap().is_empty(),
        "snapshot state must precede wiring"
    );
    peers(
        &http,
        &ips[2],
        2,
        json!([
        {"replica_id": 1, "endpoint": format!("{}:7001", ips[0]), "voting": true},
        {"replica_id": 2, "endpoint": format!("{}:7001", ips[1]), "voting": true}]),
    )
    .await;
    for (index, ip) in ips[..2].iter().enumerate() {
        peers(
            &http,
            ip,
            index,
            json!([{"replica_id": 9, "endpoint": format!("{}:7001", ips[2]), "voting": false}]),
        )
        .await;
    }
    kv.put(0, 1, b"k3", b"v3", None).await.unwrap();
    local_value(&transport, &ips[2], b"k3", b"v3", Duration::from_secs(20)).await;
}
