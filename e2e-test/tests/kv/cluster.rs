// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::management::{addresses, client, origin, request};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, GetOutcome, KvRpcTransport, ReadMode};
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

pub(super) async fn topology(client: &Client, ip: &str) -> Value {
    request(client, &origin(ip), Method::GET, "/topology", Value::Null, 200).await
}

pub(super) async fn leader(client: &Client, ips: &[String], group: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut stable = None;
    loop {
        let mut leaders = Vec::new();
        for (index, ip) in ips.iter().enumerate() {
            let topo = topology(client, ip).await;
            let groups = topo["stores"][0]["groups"].as_array().unwrap();
            let state = groups.iter().find(|state| state["group_id"] == group).unwrap();
            if state["local_replica"]["role"] == "leader" {
                leaders.push(index);
            }
        }
        if leaders.len() == 1 {
            match stable {
                Some((index, since)) if index == leaders[0] => {
                    if Instant::now().duration_since(since) >= Duration::from_millis(800) {
                        return index;
                    }
                }
                _ => stable = Some((leaders[0], Instant::now())),
            }
        } else {
            stable = None;
        }
        assert!(Instant::now() < deadline, "no unique leader for group {group}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore = "requires three release-container nodes with stores ten, twenty and thirty"]
async fn topology_and_follower_hint() {
    let ips = addresses(3);
    let http = client();
    for (index, ip) in ips.iter().enumerate() {
        let topo = topology(&http, ip).await;
        let stores = topo["stores"].as_array().unwrap();
        assert_eq!(stores.len(), 1);
        assert_eq!(stores[0]["store_id"], (index + 1) * 10);
        let groups = stores[0]["groups"].as_array().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0]["group_id"], 1);
        assert_eq!(groups[0]["local_replica_id"], index + 1);
        assert!(stores[0]["listen_addr"].as_str().unwrap().ends_with(":7001"));
    }
    let leader = leader(&http, &ips, 1).await;
    let follower = (leader + 1) % ips.len();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let topo = topology(&http, &ips[follower]).await;
        if topo["stores"][0]["groups"][0]["leader_id"] == leader + 1 {
            break;
        }
        assert!(Instant::now() < deadline, "follower lacks leader hint: {topo}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let response = KvRpcTransport::new()
        .send_put(
            &format!("http://{}:7001", ips[follower]),
            b"k",
            b"v",
            200,
            1,
            2001,
            20001,
            1,
        )
        .await
        .unwrap();
    assert!(!response.ok);
    assert_eq!(response.error, "not leader");
    assert_eq!(response.not_leader_hint, format!("{}:7001", ips[leader]));
}

async fn add_group(http: &Client, ips: &[String]) {
    let mut stores = Vec::new();
    for (index, ip) in ips.iter().enumerate() {
        request(
            http,
            &origin(ip),
            Method::POST,
            &format!("/stores/{index}/groups"),
            json!({"group_id": 2, "replica_id": index + 1}),
            201,
        )
        .await;
        let mut topo = topology(http, ip).await;
        assert_eq!(topo["stores"][0]["groups"].as_array().unwrap().len(), 2);
        topo["stores"][0]["listen_addr"] = json!(format!("{ip}:7001"));
        stores.push(topo["stores"][0].clone());
    }
    for (index, ip) in ips.iter().enumerate() {
        request(
            http,
            &origin(ip),
            Method::POST,
            &format!("/stores/{index}/groups/2/remotes/batch"),
            json!({"stores": stores}),
            200,
        )
        .await;
    }
}

async fn exact_value(kv: &CrowdbKvClient, group: u64, value: &[u8]) {
    assert!(
        matches!(kv.get(0, group, b"shared-key", ReadMode::Linearizable, None).await.unwrap(),
        GetOutcome::Found {value: found, ..} if found.as_ref() == value)
    );
}

#[tokio::test]
#[ignore = "requires three release-container nodes"]
async fn dynamic_groups_isolate_data() {
    let ips = addresses(3);
    let http = client();
    add_group(&http, &ips).await;
    leader(&http, &ips, 1).await;
    leader(&http, &ips, 2).await;
    let kv = CrowdbKvClient::new(ClientConfig::new(ips.iter().map(|ip| origin(ip)).collect()));
    for (group, value) in [(1, b"value-from-A"), (2, b"value-from-B")] {
        kv.put(0, group, b"shared-key", value, None).await.unwrap();
        exact_value(&kv, group, value).await;
    }
    kv.delete(0, 1, b"shared-key", None).await.unwrap();
    assert!(matches!(
        kv.get(0, 1, b"shared-key", ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::NotFound
    ));
    exact_value(&kv, 2, b"value-from-B").await;
    for (index, ip) in ips.iter().enumerate() {
        request(
            &http,
            &origin(ip),
            Method::DELETE,
            &format!("/stores/{index}/groups/2"),
            Value::Null,
            200,
        )
        .await;
        let topo = topology(&http, ip).await;
        assert_eq!(topo["stores"][0]["groups"].as_array().unwrap().len(), 1);
        assert_eq!(topo["stores"][0]["groups"][0]["group_id"], 1);
    }
    kv.put(0, 1, b"shared-key", b"still-works", None).await.unwrap();
    exact_value(&kv, 1, b"still-works").await;
}
