// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::cluster::leader;
use super::reconfiguration::wire_initial;
use crate::common::{
    control,
    management::{addresses, client, origin, request},
};
use crowdb_paxos_kv_client::{ClientConfig, GetOutcome, PaxosKvClient, ReadMode};
use reqwest::{Client, Method};
use serde_json::{json, Value};

fn kv(ips: &[String]) -> PaxosKvClient {
    PaxosKvClient::new(ClientConfig::new(ips.iter().map(|ip| origin(ip)).collect()))
}

async fn exact(kv: &PaxosKvClient, store: u64, key: &[u8], expected: &[u8]) {
    assert!(
        matches!(kv.get(store, 1, key, ReadMode::Linearizable, None).await.unwrap(),
        GetOutcome::Found {value, ..} if value.as_ref() == expected)
    );
}

async fn remove(http: &Client, ips: &[String], removed: usize, active: &[usize]) {
    for &index in active {
        request(
            http,
            &origin(&ips[index]),
            Method::DELETE,
            &format!("/stores/{index}/groups/1/remotes/{}", removed + 1),
            Value::Null,
            200,
        )
        .await;
    }
}

async fn shutdown(index: usize) {
    let state = control::node("stop", index).await;
    assert_eq!(state["Running"], false);
    assert_eq!(
        state["ExitCode"], 0,
        "graceful shutdown must finish without SIGKILL"
    );
}

#[tokio::test]
#[ignore = "requires three release-container nodes and owned SIGTERM control"]
async fn graceful_leader_shutdown() {
    let ips = addresses(3);
    let http = client();
    let leader = leader(&http, &ips, 1).await;
    let initial = kv(&ips);
    for index in 1..=5 {
        initial
            .put(
                0,
                1,
                format!("shutdown-{index}").as_bytes(),
                format!("val-{index}").as_bytes(),
                None,
            )
            .await
            .unwrap();
    }
    shutdown(leader).await;
    let survivors: Vec<_> = ips
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != leader)
        .map(|(_, ip)| ip.clone())
        .collect();
    super::cluster::leader(&http, &survivors, 1).await;
    let store = u64::try_from((0..3).find(|index| *index != leader).unwrap()).unwrap();
    let surviving = kv(&survivors);
    for index in 1..=5 {
        exact(
            &surviving,
            store,
            format!("shutdown-{index}").as_bytes(),
            format!("val-{index}").as_bytes(),
        )
        .await;
    }
    surviving
        .put(store, 1, b"after-shutdown", b"still-writable", None)
        .await
        .unwrap();
    exact(&surviving, store, b"after-shutdown", b"still-writable").await;
}

#[tokio::test]
#[ignore = "requires three release-container nodes and owned decommission control"]
async fn remove_leader_and_continue() {
    let ips = addresses(3);
    let http = client();
    let leader = leader(&http, &ips, 1).await;
    let initial = kv(&ips);
    initial.put(0, 1, b"rmldr-before", b"val-1", None).await.unwrap();
    let stepped = request(
        &http,
        &origin(&ips[leader]),
        Method::POST,
        &format!("/stores/{leader}/groups/1/step-down?sync=true"),
        json!({"reason": "decommission"}),
        200,
    )
    .await;
    assert_eq!(stepped["accepted"], true);
    let active: Vec<_> = (0..3).filter(|index| *index != leader).collect();
    remove(&http, &ips, leader, &active).await;
    shutdown(leader).await;
    let survivors: Vec<_> = active.iter().map(|index| ips[*index].clone()).collect();
    super::cluster::leader(&http, &survivors, 1).await;
    let surviving = kv(&survivors);
    let store = u64::try_from(active[0]).unwrap();
    surviving
        .put(store, 1, b"rmldr-after", b"val-2", None)
        .await
        .unwrap();
    exact(&surviving, store, b"rmldr-before", b"val-1").await;
    exact(&surviving, store, b"rmldr-after", b"val-2").await;
}

#[tokio::test]
#[ignore = "requires four release-container nodes and owned decommission control"]
async fn unavailable_voter_and_follower_removal() {
    let ips = addresses(4);
    let http = client();
    // The old host fixture closed the fourth RPC listener on a reachable
    // loopback IP. A bare store preserves that connection-refused fault;
    // stopping its container would instead remove the network interface.
    request(&http, &origin(&ips[3]), Method::GET, "/health", Value::Null, 200).await;
    wire_initial(&http, &ips).await;
    leader(&http, &ips[..3], 1).await;
    let initial = kv(&ips[..3]);
    initial.put(0, 1, b"rc-before", b"val-1", None).await.unwrap();
    for (index, ip) in ips[..3].iter().enumerate() {
        request(
            &http,
            &origin(ip),
            Method::POST,
            &format!("/stores/{index}/groups/1/remotes"),
            json!([{"replica_id": 4, "endpoint": format!("{}:7001", ips[3])}]),
            200,
        )
        .await;
    }
    let leader = leader(&http, &ips[..3], 1).await;
    initial.put(0, 1, b"rc-after-add", b"val-2", None).await.unwrap();
    let removed = usize::from(leader == 0);
    let active: Vec<_> = (0..3).filter(|index| *index != removed).collect();
    remove(&http, &ips, removed, &active).await;
    shutdown(removed).await;
    let survivors: Vec<_> = active.iter().map(|index| ips[*index].clone()).collect();
    super::cluster::leader(&http, &survivors, 1).await;
    let surviving = kv(&survivors);
    let store = u64::try_from(active[0]).unwrap();
    surviving
        .put(store, 1, b"rc-after-remove", b"val-3", None)
        .await
        .unwrap();
    for (key, value) in [
        (b"rc-before".as_slice(), b"val-1"),
        (b"rc-after-add", b"val-2"),
        (b"rc-after-remove", b"val-3"),
    ] {
        exact(&surviving, store, key, value).await;
    }
}
