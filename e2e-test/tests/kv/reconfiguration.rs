// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::cluster::{leader, topology};
use crate::common::management::{addresses, client, origin, request};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, GetOutcome, ReadMode};
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

fn kv(ips: &[String]) -> CrowdbKvClient {
    CrowdbKvClient::new(ClientConfig::new(ips.iter().map(|ip| origin(ip)).collect()))
}

async fn wire_initial(http: &Client, ips: &[String]) {
    let mut stores = Vec::new();
    for ip in &ips[..3] {
        let mut topo = topology(http, ip).await;
        topo["stores"][0]["listen_addr"] = json!(format!("{ip}:7001"));
        stores.push(topo["stores"][0].clone());
    }
    for (index, ip) in ips[..3].iter().enumerate() {
        request(
            http,
            &origin(ip),
            Method::POST,
            &format!("/stores/{index}/groups/1/remotes/batch"),
            json!({"stores": stores}),
            200,
        )
        .await;
    }
}

async fn expand(http: &Client, ips: &[String]) {
    for (index, ip) in ips.iter().enumerate() {
        let peers = if index < 3 { 3..5 } else { 0..5 };
        for peer in peers.filter(|peer| *peer != index) {
            request(
                http,
                &origin(ip),
                Method::POST,
                &format!("/stores/{index}/groups/1/remotes"),
                json!([{"replica_id": peer + 1, "endpoint": format!("{}:7001", ips[peer])}]),
                200,
            )
            .await;
        }
    }
}

async fn shrink(http: &Client, ips: &[String]) {
    for (index, ip) in ips.iter().enumerate() {
        let removed = if index < 2 { 2..5 } else { 0..2 };
        for peer in removed {
            request(
                http,
                &origin(ip),
                Method::DELETE,
                &format!("/stores/{index}/groups/1/remotes/{}", peer + 1),
                Value::Null,
                200,
            )
            .await;
        }
    }
}

async fn exact(kv: &CrowdbKvClient, store: u64, key: &[u8], value: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match kv.get(store, 1, key, ReadMode::Linearizable, None).await.unwrap() {
            GetOutcome::Found { value: found, .. } => {
                assert_eq!(found.as_ref(), value);
                return;
            }
            GetOutcome::NotFound => assert!(Instant::now() < deadline, "acknowledged key lost"),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
#[ignore = "requires five release-container nodes"]
async fn quorum_expands_and_shrinks_without_losing_writes() {
    let ips = addresses(5);
    let http = client();
    wire_initial(&http, &ips).await;
    leader(&http, &ips[..3], 1).await;
    let initial = kv(&ips[..3]);
    initial.put(0, 1, b"k1", b"v1", None).await.unwrap();
    exact(&initial, 0, b"k1", b"v1").await;
    expand(&http, &ips).await;
    leader(&http, &ips, 1).await;
    let expanded = kv(&ips);
    exact(&expanded, 0, b"k1", b"v1").await;
    expanded.put(0, 1, b"k2", b"v2", None).await.unwrap();
    shrink(&http, &ips).await;
    leader(&http, &ips[2..], 1).await;
    let surviving = kv(&ips[2..]);
    exact(&surviving, 2, b"k2", b"v2").await;
    surviving.delete(2, 1, b"k1", None).await.unwrap();
    assert!(matches!(
        surviving
            .get(2, 1, b"k1", ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::NotFound
    ));
    surviving.put(2, 1, b"k3", b"v3", None).await.unwrap();
    exact(&surviving, 2, b"k3", b"v3").await;
}
