// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::clients::http::ServerClient;
use crowdb_console_shared::ConsoleConfig;
use serde_json::json;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

struct Upstream {
    node_id: u64,
    mgmt_url: String,
    rpc_url: String,
}

struct Cluster {
    nodes: BTreeMap<u64, Upstream>,
    web: String,
}

impl Cluster {
    const fn sid() -> u64 {
        1
    }
    const fn gid() -> u64 {
        1
    }
    fn base_url(&self) -> String {
        self.web.clone()
    }
    fn mgmt_url(&self, node_id: u64) -> String {
        self.nodes[&node_id].mgmt_url.clone()
    }
}

async fn spawn_five_node_cluster(_name: &str) -> Cluster {
    let (_, web, ips) = super::web_voters::setup(5).await;
    let nodes = ips
        .iter()
        .enumerate()
        .map(|(index, ip)| {
            let node_id = u64::try_from(index + 1).unwrap();
            (
                node_id,
                Upstream {
                    node_id,
                    mgmt_url: format!("http://{ip}:7000"),
                    rpc_url: format!("{ip}:7001"),
                },
            )
        })
        .collect();
    Cluster { nodes, web }
}

async fn create_five_node_group(cluster: &Cluster) {
    let base = cluster.base_url();
    let http = reqwest::Client::new();
    let sid = Cluster::sid();
    let gid = Cluster::gid();

    let resp = http
        .post(format!("{base}/api/stores"))
        .json(&json!({"store_id": sid, "nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create store: {:?}", resp.text().await.ok());

    let resp = http
        .post(format!("{base}/api/stores/{sid}/groups"))
        .json(&json!({"group_id": gid, "replica_id": 1, "nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create group: {:?}", resp.text().await.ok());

    for node_id in 2u64..=5 {
        let resp = http
            .post(format!("{base}/api/stores/{sid}/groups/{gid}/replicas"))
            .json(&json!({"node_id": node_id, "replica_id": node_id}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            201,
            "add replica {node_id}: {:?}",
            resp.text().await.ok()
        );
    }
}

async fn wait_for_leader(cluster: &Cluster, timeout: Duration, excluded: Option<u64>) -> Option<(u64, u64)> {
    let http = crate::common::management::client();
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let group: serde_json::Value = http
            .get(format!("{}/api/stores/1/groups/1", cluster.base_url()))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        if let Some(leader) = group["replicas"]
            .as_array()?
            .iter()
            .find(|r| r["role"] == "leader" && r["replica_id"].as_u64() != excluded)
        {
            return Some((leader["replica_id"].as_u64()?, leader["node_id"].as_u64()?));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

async fn assert_removed_absent_from_all(cluster: &Cluster, removed_rid: u64) {
    let sid = Cluster::sid();
    let gid = Cluster::gid();
    for u in cluster.nodes.values() {
        // The removed node no longer hosts the group (or is dead); querying
        // it for remotes is meaningless and would 404.
        if let Ok(client) = ServerClient::new(u.mgmt_url.clone()) {
            if let Ok(remotes) = client.list_remote_replicas(sid, gid).await {
                assert!(
                    !remotes.iter().any(|r| r.replica_id == removed_rid),
                    "node {} should not list removed replica {removed_rid} as remote: {remotes:?}",
                    u.node_id
                );
            }
        }
    }
}

async fn wait_for_replica_count(
    base: &str,
    http: &reqwest::Client,
    sid: u64,
    gid: u64,
    expected: usize,
    timeout: Duration,
) -> Vec<serde_json::Value> {
    let deadline = Instant::now() + timeout;
    loop {
        let list: Vec<serde_json::Value> = http
            .get(format!("{base}/api/stores/{sid}/groups/{gid}/replicas"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if list.len() == expected {
            return list;
        }
        assert!(
            Instant::now() < deadline,
            "expected {expected} replicas, got {list:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn role_on_node(cluster: &Cluster, node_id: u64, rid: u64) -> Option<String> {
    let client = ServerClient::new(cluster.mgmt_url(node_id)).unwrap();
    let topology = client.topology().await.ok()?;
    topology
        .iter()
        .find(|s| s.store_id == Cluster::sid())
        .and_then(|s| s.groups.iter().find(|g| g.group_id == Cluster::gid()))
        .and_then(|g| {
            if g.local_replica_id == rid {
                Some(g.local_replica.role.clone())
            } else {
                None
            }
        })
}

#[tokio::test]
#[ignore = "requires five owned release KV nodes and writable Web"]
async fn remove_leader_from_five_node_group_elects_new_leader() {
    let cluster = spawn_five_node_cluster("remove_leader_from_five_node_group_elects_new_leader").await;
    create_five_node_group(&cluster).await;

    let (leader_rid, _leader_node) = wait_for_leader(&cluster, Duration::from_secs(10), None)
        .await
        .expect("leader should be elected in 5-node group");

    let base = cluster.base_url();
    let http = reqwest::Client::new();
    let sid = Cluster::sid();
    let gid = Cluster::gid();

    let resp = http
        .delete(format!(
            "{base}/api/stores/{sid}/groups/{gid}/replicas/{leader_rid}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        204,
        "delete leader replica should succeed: {:?}",
        resp.text().await.ok()
    );

    let (new_leader_rid, new_leader_node) =
        wait_for_leader(&cluster, Duration::from_secs(10), Some(leader_rid))
            .await
            .expect("a new leader should be elected among survivors within timeout");
    assert_ne!(
        new_leader_rid, leader_rid,
        "new leader must not be the removed replica"
    );
    assert!(
        cluster.nodes.contains_key(&new_leader_node),
        "new leader should be one of the surviving nodes"
    );

    let list = wait_for_replica_count(&base, &http, sid, gid, 4, Duration::from_secs(10)).await;
    assert!(
        !list.iter().any(|r| r["replica_id"] == leader_rid),
        "removed replica should not appear in list: {list:?}"
    );

    assert_removed_absent_from_all(&cluster, leader_rid).await;

    let role = role_on_node(&cluster, new_leader_node, new_leader_rid)
        .await
        .expect("new leader node should report its role");
    assert_eq!(role, "leader", "survivor {new_leader_node} should be leader");
}

#[tokio::test]
#[ignore = "requires five owned release KV nodes and writable Web"]
async fn remove_unreachable_leader_retains_group0_membership() {
    let cluster = spawn_five_node_cluster("remove_unreachable_leader_retains_group0_membership").await;
    create_five_node_group(&cluster).await;

    let (leader_rid, leader_node) = wait_for_leader(&cluster, Duration::from_secs(10), None)
        .await
        .expect("leader should be elected in 5-node group");

    // Kill the leader process before asking the console to remove it.
    // Physical removal must fail closed and preserve Group-0 membership.
    let index = usize::try_from(leader_node - 1).unwrap();
    crate::common::control::node("kill", index).await;
    let dead = Instant::now() + Duration::from_secs(5);
    loop {
        let state = crate::common::control::node("status", index).await;
        if state["Running"] == false {
            break;
        }
        assert!(Instant::now() < dead, "leader container should be dead: {state}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let base = cluster.base_url();
    let http = reqwest::Client::new();
    let sid = Cluster::sid();
    let gid = Cluster::gid();

    let resp = http
        .delete(format!(
            "{base}/api/stores/{sid}/groups/{gid}/replicas/{leader_rid}"
        ))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let error = resp.text().await.unwrap();
    assert_eq!(
        status, 500,
        "delete must fail closed when the target is unreachable: {error}"
    );
    assert!(
        error.contains("error sending request"),
        "unexpected error: {error}"
    );

    let survivor = cluster
        .nodes
        .values()
        .find(|node| node.node_id != leader_node)
        .unwrap();
    let context = crowdb_console_shared::ops::OpContext::new(
        survivor.rpc_url.trim_start_matches("http://").to_string(),
        vec![survivor.mgmt_url.clone()],
        ConsoleConfig::default(),
    );
    let replicas = context.sysmd().list_replicas_in_group(sid, gid).await.unwrap();
    assert!(replicas.iter().any(|replica| replica.replica_id == leader_rid));

    let response = http
        .get(format!("{base}/api/stores/{sid}/groups/{gid}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let group: serde_json::Value = response.json().await.unwrap();
    let dead_replica = group["replicas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|replica| replica["replica_id"] == leader_rid)
        .expect("Group 0 member remains visible when unobserved");
    assert_eq!(dead_replica["role"], "unknown");
    assert_eq!(dead_replica["state"], "unknown");
}
