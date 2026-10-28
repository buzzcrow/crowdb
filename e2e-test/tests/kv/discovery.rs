// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::{
    control,
    management::{addresses, client, origin, request},
};
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient, ServiceRegistryClient};
use crowdb_protocol::common::KvServerIdentity;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

#[tokio::test]
#[ignore = "requires a bare release-container node with registration enabled"]
async fn paused_bootstrap_admits_no_registration_writes() {
    let base = origin(&addresses(1)[0]);
    let http = client();
    request(
        &http,
        &base,
        Method::POST,
        "/system/init",
        json!({"replica_id": 2, "start_election": false}),
        201,
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let topology = request(&http, &base, Method::GET, "/topology", Value::Null, 200).await;
        let group = &topology["stores"][0]["groups"][0];
        assert_eq!(group["leader_id"], 0);
        assert_eq!(group["local_replica"]["role"], "follower");
        assert_eq!(group["local_replica"]["election"]["current_term"], 0);
        assert_eq!(group["inflight"]["total_enqueued"], 0);
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn registered(service: &ServiceRegistryClient, endpoint: &str) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(instances) = service.read_all_kv_server_instances().await {
            let matches: Vec<_> = instances
                .iter()
                .filter(|(_, value)| {
                    value
                        .extra
                        .as_ref()
                        .and_then(|extra| extra.kv_server.as_ref())
                        .and_then(|identity| identity.node_id)
                        == Some(2)
                })
                .collect();
            if matches.len() == 1 && matches[0].1.rpc_endpoint == endpoint {
                return matches[0].0;
            }
        }
        assert!(
            Instant::now() < deadline,
            "nonmember registration did not converge to {endpoint}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore = "requires two bare release-container nodes and owned lifecycle control"]
async fn nonmember_recovers_discovery_and_registration_identity() {
    let ips = addresses(2);
    let leader = origin(&ips[0]);
    let nonmember = origin(&ips[1]);
    let http = client();
    request(
        &http,
        &leader,
        Method::POST,
        "/system/init",
        json!({"replica_id": 1, "start_election": true}),
        201,
    )
    .await;
    let service = ServiceRegistryClient::new(PaxosKvClient::new(ClientConfig::new(vec![leader.clone()])));
    for seeds in [
        json!([]),
        json!(["127.0.0.1:1"]),
        json!(["http://user:password@localhost:1"]),
    ] {
        request(
            &http,
            &nonmember,
            Method::POST,
            "/system/group0-discovery",
            json!({"management_seeds": seeds}),
            400,
        )
        .await;
    }
    for _ in 0..2 {
        request(
            &http,
            &nonmember,
            Method::POST,
            "/system/group0-discovery",
            json!({"management_seeds": [leader]}),
            200,
        )
        .await;
    }
    let identity = registered(&service, &nonmember).await;
    let topology = request(&http, &nonmember, Method::GET, "/topology", Value::Null, 200).await;
    assert!(
        topology["stores"].as_array().unwrap().is_empty(),
        "discovery must not create membership"
    );
    let stopped = control::node("stop", 1).await;
    assert_eq!(stopped["Running"], false);
    service
        .register_kv_server(
            KvServerIdentity {
                instance_id: identity,
                node_id: Some(2),
            },
            "http://127.0.0.1:1",
            &[],
            &[],
            "ok",
            "/opt/crowdb/data/kv",
        )
        .await
        .unwrap();
    let started = control::node("start", 1).await;
    assert_eq!(started["Running"], true);
    assert_eq!(
        registered(&service, &nonmember).await,
        identity,
        "registration identity must survive restart"
    );
}
