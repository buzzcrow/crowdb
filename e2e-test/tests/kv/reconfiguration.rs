// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::cluster::{leader, topology};
use crate::common::management::{addresses, client, origin, request};
use crowdb_paxos_kv_client::{ClientConfig, GetOutcome, PaxosKvClient, PaxosKvRpcTransport, ReadMode};
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

fn kv(ips: &[String]) -> PaxosKvClient {
    PaxosKvClient::new(ClientConfig::new(ips.iter().map(|ip| origin(ip)).collect()))
}

pub(super) async fn wire_initial(http: &Client, ips: &[String]) {
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

async fn membership(http: &Client, ip: &str, method: Method, path: &str, body: Value, epoch: u64) {
    let response = http
        .request(method, format!("{}{path}", origin(ip)))
        .header("x-crowdb-membership-epoch", epoch.to_string())
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(status.as_u16(), 200, "{path}: {text}");
}

async fn applied(http: &Client, ip: &str) -> u64 {
    topology(http, ip).await["stores"][0]["groups"][0]["read_state"]["contiguous_applied"]
        .as_u64()
        .expect("group applied frontier")
}

async fn imported_value(ip: &str, required: u64) {
    let response = PaxosKvRpcTransport::new()
        .send_get(
            &format!("http://{ip}:7001"),
            b"k1",
            9201,
            9201,
            1,
            crowdb_paxos_kv::rpc::ReadMode::MinSlot,
            required,
        )
        .await
        .unwrap();
    assert!(response.ok, "snapshot local read failed: {}", response.error);
    assert!(!response.not_found, "snapshot omitted acknowledged k1");
    assert_eq!(response.value.as_ref(), b"v1");
}

async fn add_member(http: &Client, ips: &[String], index: usize, epoch: u64) {
    let source = leader(http, &ips[..index], 1).await;
    let required = applied(http, &ips[source]).await;
    request(
        http,
        &origin(&ips[index]),
        Method::POST,
        &format!("/stores/{index}/groups/1/join"),
        json!({"replica_id": index + 1, "peer_endpoint": format!("{}:7001", ips[source])}),
        201,
    )
    .await;
    // Snapshot state must exist before this replica participates in any quorum.
    assert!(applied(http, &ips[index]).await >= required);
    imported_value(&ips[index], required).await;
    for (peer, ip) in ips[..index].iter().enumerate() {
        request(
            http,
            &origin(ip),
            Method::POST,
            &format!("/stores/{peer}/groups/1/remotes"),
            json!([{"replica_id": index + 1, "endpoint": format!("{}:7001", ips[index]), "voting": false}]),
            200,
        )
        .await;
    }
    let remotes: Vec<_> = ips[..index]
        .iter()
        .enumerate()
        .map(|(peer, ip)| json!({"replica_id": peer + 1, "endpoint": format!("{ip}:7001"), "voting": true}))
        .collect();
    membership(
        http,
        &ips[index],
        Method::POST,
        &format!("/stores/{index}/groups/1/remotes"),
        json!(remotes),
        epoch,
    )
    .await;
    let required = applied(http, &ips[source]).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while applied(http, &ips[index]).await < required {
        assert!(
            Instant::now() < deadline,
            "joining member did not catch up before promotion"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for (peer, ip) in ips[..index].iter().enumerate() {
        membership(
            http,
            ip,
            Method::POST,
            &format!("/stores/{peer}/groups/1/remotes"),
            json!([{"replica_id": index + 1, "endpoint": format!("{}:7001", ips[index]), "voting": true}]),
            epoch,
        )
        .await;
    }
    leader(http, &ips[..=index], 1).await;
}

async fn shrink(http: &Client, ips: &[String], mut epoch: u64) {
    for removed in 0..2 {
        let current = leader(http, &ips[removed..], 1).await + removed;
        if current == removed {
            let stepped = request(
                http,
                &origin(&ips[removed]),
                Method::POST,
                &format!("/stores/{removed}/groups/1/step-down?sync=true"),
                json!({"reason": "decommission"}),
                200,
            )
            .await;
            assert_eq!(stepped["accepted"], true);
        }
        epoch += 1;
        // Retire while its old epoch is still current. The remaining members
        // retain the old quorum until the single-member removal reaches them.
        membership(
            http,
            &ips[removed],
            Method::DELETE,
            &format!("/stores/{removed}/groups/1"),
            Value::Null,
            epoch,
        )
        .await;
        for (index, ip) in ips.iter().enumerate().skip(removed + 1) {
            membership(
                http,
                ip,
                Method::DELETE,
                &format!("/stores/{index}/groups/1/remotes/{}", removed + 1),
                Value::Null,
                epoch,
            )
            .await;
        }
        leader(http, &ips[removed + 1..], 1).await;
    }
}

async fn exact(kv: &PaxosKvClient, store: u64, key: &[u8], value: &[u8]) {
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
    eprintln!("reconfiguration: acknowledged k1 before expansion");
    // First wiring bootstraps the original members at epoch zero.
    let mut epoch = 0;
    for index in 3..5 {
        epoch += 1;
        add_member(&http, &ips, index, epoch).await;
    }
    leader(&http, &ips, 1).await;
    let expanded = kv(&ips);
    exact(&expanded, 0, b"k1", b"v1").await;
    expanded.put(0, 1, b"k2", b"v2", None).await.unwrap();
    eprintln!("reconfiguration: expansion preserves k1; acknowledged k2 before shrink");
    shrink(&http, &ips, epoch).await;
    leader(&http, &ips[2..], 1).await;
    let surviving = kv(&ips[2..]);
    exact(&surviving, 2, b"k1", b"v1").await;
    exact(&surviving, 2, b"k2", b"v2").await;
    eprintln!("reconfiguration: shrink preserves k1 and k2");
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
