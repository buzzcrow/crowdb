// Copyright 2026-present Gian <crow.db@outlook.com>

//! Deployment-level graceful shutdown and reconfig via management API.
//!
//! These tests boot real `crowdb-kv-server` processes and exercise the HTTP
//! management API for:
//! - Graceful shutdown of a leader node under write load (SIGTERM path).
//! - Add/remove replica via `POST /remotes` and `DELETE /remotes/:rid`.

mod common;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crowdb_kv::rpc::{KvGetRequest, KvSetRequest};
use crowdb_kv_client::KvRpcTransport;
use serde_json::Value;

use common::process::{start_test_server, ServerHandle};
use common::test_client::TestKvClient;

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

// Each test boots 3 real crowdb-kv-server processes. Running them in
// parallel (3 tests × 3 servers = 9 processes) saturates CI's 2-core
// runners, causing peer-RPC timeouts, election failures, and lost
// commits. The guard serializes the tests within this binary so each
// cluster has the full CPU budget.
static PROCESS_TEST_GUARD: Mutex<()> = Mutex::new(());

/// Acquire the serialization guard. Uses `into_inner` on poison so a
/// panic in one test does not cascade-fail the remaining tests.
fn acquire_guard() -> std::sync::MutexGuard<'static, ()> {
    PROCESS_TEST_GUARD
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Send a request builder, retrying on transient HTTP errors for up
/// to 10 s. Returns the response on success. Panics on deadline.
async fn http_send_with_retry(req: reqwest::RequestBuilder, label: &str) -> reqwest::Response {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match req.try_clone().expect("clonable request").send().await {
            Ok(r) => return r,
            Err(e) => {
                assert!(Instant::now() <= deadline, "{label} failed within 10 s: {e}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

struct ServerNode {
    handle: Option<ServerHandle>,
    node_id: u64,
    replica_id: u64,
}

impl ServerNode {
    fn mgmt_base(&self) -> &str {
        self.handle.as_ref().expect("server alive").base_url()
    }
}

async fn start_cluster(node_ids: &[u64], group_id: u64) -> Vec<ServerNode> {
    let mut nodes = Vec::new();
    for (idx, &nid) in node_ids.iter().enumerate() {
        let replica_id = u64::try_from(idx + 1).expect("replica id");
        let handle = start_test_server(&[
            "--stores",
            &nid.to_string(),
            "--groups",
            &group_id.to_string(),
            "--replica",
            &replica_id.to_string(),
        ])
        .await
        .unwrap_or_else(|e| panic!("start crowdb-kv-server node {nid}: {e}"));
        nodes.push(ServerNode {
            handle: Some(handle),
            node_id: nid,
            replica_id,
        });
    }
    nodes
}

/// Fetch the topology from a node's management API. Returns `Err` on
/// any HTTP or parse failure so callers with their own deadline-based
/// retry loop (e.g. `wait_for_leader_ref`, `kv_put_nodes`) can skip
/// the transient failure instead of panicking.
async fn topology(node: &ServerNode) -> Result<Value, String> {
    let url = format!("{}/topology", node.mgmt_base());
    let resp = client()
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;
    resp.json()
        .await
        .map_err(|e| format!("json parse from {url}: {e}"))
}

/// Like [`topology`] but panics on failure. Used only at setup time
/// (e.g. `wire_topology`) when the servers are freshly started and
/// should be responsive.
async fn topology_or_panic(node: &ServerNode) -> Value {
    topology(node)
        .await
        .unwrap_or_else(|e| panic!("topology for node {}: {e}", node.node_id))
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint
        .strip_prefix("0.0.0.0:")
        .map_or_else(|| endpoint.to_string(), |port| format!("127.0.0.1:{port}"))
}

fn normalize_topology(mut topo: Value) -> Value {
    if let Some(stores) = topo["stores"].as_array_mut() {
        for store in stores {
            if let Some(addr) = store["listen_addr"].as_str() {
                store["listen_addr"] = Value::String(normalize_endpoint(addr));
            }
        }
    }
    topo
}

fn node_endpoint(topo: &Value) -> Option<String> {
    topo["stores"][0]["listen_addr"].as_str().map(normalize_endpoint)
}

async fn combined_topology(nodes: &[ServerNode]) -> Value {
    let mut combined_stores = Vec::new();
    for node in nodes {
        let topo = normalize_topology(topology_or_panic(node).await);
        for store in topo["stores"].as_array().unwrap() {
            combined_stores.push(store.clone());
        }
    }
    serde_json::json!({ "stores": combined_stores })
}

async fn wire_topology(nodes: &[ServerNode], group_id: u64) {
    let combined = combined_topology(nodes).await;
    for node in nodes {
        let url = format!(
            "{}/stores/{}/groups/{group_id}/remotes/batch",
            node.mgmt_base(),
            node.node_id
        );
        let resp = http_send_with_retry(client().post(&url).json(&combined), "batch wiring").await;
        assert_eq!(
            resp.status(),
            200,
            "batch wiring failed for node {}",
            node.node_id
        );
    }
}

async fn wait_for_leader(nodes: &[ServerNode], group_id: u64, timeout: Duration) -> usize {
    wait_for_leader_ref(&nodes.iter().collect::<Vec<_>>(), group_id, timeout).await
}

async fn wait_for_leader_ref(nodes: &[&ServerNode], group_id: u64, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut leaders: Vec<usize> = Vec::new();
        for (idx, node) in nodes.iter().enumerate() {
            // Skip nodes whose management API is transiently unreachable
            // instead of panicking; the deadline-protected loop will
            // retry on the next iteration.
            let Ok(topo) = topology(node).await else {
                continue;
            };
            let role = topo["stores"][0]["groups"]
                .as_array()
                .and_then(|g| g.iter().find(|gg| gg["group_id"].as_u64() == Some(group_id)))
                .and_then(|gg| gg["local_replica"]["role"].as_str())
                .unwrap_or("");
            if role == "leader" {
                leaders.push(idx);
            }
        }
        if leaders.len() == 1 {
            return leaders[0];
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("no unique leader for group {group_id} within {timeout:?}");
}

async fn kv_put(nodes: &[ServerNode], group_id: u64, key: &[u8], val: &[u8], req_id: u64) -> bool {
    kv_put_nodes(&nodes.iter().collect::<Vec<_>>(), group_id, key, val, req_id).await
}

async fn kv_put_nodes(nodes: &[&ServerNode], group_id: u64, key: &[u8], val: &[u8], req_id: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(15);
    let transport = Arc::new(KvRpcTransport::new());
    let mut last_err = String::new();
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let leader_idx = wait_for_leader_ref(nodes, group_id, remaining).await;
        let topo = match topology(nodes[leader_idx]).await {
            Ok(t) => t,
            Err(e) => {
                last_err = e;
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let Some(addr) = node_endpoint(&topo) else {
            last_err = "topology missing listen_addr".to_string();
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        };
        let client = TestKvClient::with_transport(Arc::clone(&transport), format!("http://{addr}"));
        match client
            .put(KvSetRequest {
                version: 1,
                key: Bytes::copy_from_slice(key),
                value: Bytes::copy_from_slice(val),
                ttl_ms: 0,
                request_id: req_id,
                request_create_ms: req_id,
                client_id: 0,
                seq: 0,
                group_id,
            })
            .await
        {
            Ok(resp) => return resp.into_inner().ok,
            // Retry on any transport error (Timeout, ConnectionClosed,
            // etc.) — the transport layer already invalidates stale
            // connections, so the next iteration re-resolves the leader
            // and establishes a fresh connection. Matches the retry
            // pattern in cluster_e2e_test::run_kv_op_with_retry.
            Err(status) => {
                last_err = status.message().to_string();
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    panic!("kv put timed out waiting for leader: {last_err}");
}

#[allow(dead_code)]
async fn kv_get(nodes: &[ServerNode], group_id: u64, key: &[u8]) -> Option<Vec<u8>> {
    kv_get_nodes(&nodes.iter().collect::<Vec<_>>(), group_id, key).await
}

async fn kv_get_nodes(nodes: &[&ServerNode], group_id: u64, key: &[u8]) -> Option<Vec<u8>> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let transport = Arc::new(KvRpcTransport::new());
    let mut last_err = String::new();
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let leader_idx = wait_for_leader_ref(nodes, group_id, remaining).await;
        let topo = match topology(nodes[leader_idx]).await {
            Ok(t) => t,
            Err(e) => {
                last_err = e;
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let Some(addr) = node_endpoint(&topo) else {
            last_err = "topology missing listen_addr".to_string();
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        };
        let client = TestKvClient::with_transport(Arc::clone(&transport), format!("http://{addr}"));
        match client
            .get(KvGetRequest {
                version: 1,
                key: Bytes::copy_from_slice(key),
                request_id: 9001,
                request_create_ms: 9001,
                group_id,
                read_mode: 0,
                min_slot: 0,
            })
            .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                return if resp.ok && !resp.not_found {
                    Some(resp.value.to_vec())
                } else {
                    None
                };
            }
            Err(status) => {
                last_err = status.message().to_string();
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    panic!("kv get timed out waiting for leader: {last_err}");
}

/// Graceful shutdown of the leader process under write load:
/// write data, kill the leader via SIGTERM, verify the surviving
/// nodes re-elect and all committed data is still readable.
#[tokio::test]
#[allow(clippy::await_holding_lock)] // serializes real process stacks that share topology ports
async fn graceful_shutdown_leader_under_load() {
    let _guard = acquire_guard();
    let group_id = 10;
    let mut nodes = start_cluster(&[1001, 1002, 1003], group_id).await;
    wire_topology(&nodes, group_id).await;

    let leader_idx = wait_for_leader(&nodes, group_id, Duration::from_secs(10)).await;

    // Write 5 keys through the leader.
    for i in 1u64..=5 {
        let key = format!("shutdown-{i}");
        let val = format!("val-{i}");
        assert!(
            kv_put(&nodes, group_id, key.as_bytes(), val.as_bytes(), i).await,
            "write {i} should commit"
        );
    }

    // Send SIGTERM to the leader process by dropping its handle.
    // The `ServerHandle` Drop impl sends SIGTERM and waits for exit.
    drop(nodes[leader_idx].handle.take());

    // Give the surviving nodes time to detect the leader is gone and
    // re-elect.
    // Collect surviving node indices.
    let remaining_indices: Vec<usize> = (0..nodes.len()).filter(|i| *i != leader_idx).collect();

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(Instant::now() <= deadline, "no leader elected after shutdown");
        let mut leaders = Vec::new();
        for &i in &remaining_indices {
            let Ok(topo) = topology(&nodes[i]).await else {
                continue;
            };
            let role = topo["stores"][0]["groups"]
                .as_array()
                .and_then(|g| g.iter().find(|gg| gg["group_id"].as_u64() == Some(group_id)))
                .and_then(|gg| gg["local_replica"]["role"].as_str())
                .unwrap_or("");
            if role == "leader" {
                leaders.push(nodes[i].node_id);
            }
        }
        if leaders.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Build a slice of surviving nodes for kv_get.
    let remaining_nodes: Vec<&ServerNode> = remaining_indices.iter().map(|&i| &nodes[i]).collect();

    // Verify all 5 keys survived the leader shutdown.
    for i in 1u64..=5 {
        let key = format!("shutdown-{i}");
        let val = format!("val-{i}");
        let result = kv_get_nodes(&remaining_nodes, group_id, key.as_bytes()).await;
        assert_eq!(
            result.as_deref(),
            Some(val.as_bytes()),
            "key {key:?} should survive leader shutdown"
        );
    }
}

/// Add a 4th replica via the management API, then remove a non-leader
/// replica. Writes must continue to commit through both reconfig operations.
#[tokio::test]
#[allow(clippy::too_many_lines)]
#[allow(clippy::await_holding_lock)] // serializes real process stacks that share topology ports
async fn reconfig_via_api_add_then_remove() {
    let _guard = acquire_guard();
    let group_id = 20;
    let nodes = start_cluster(&[2001, 2002, 2003], group_id).await;
    wire_topology(&nodes, group_id).await;

    let _leader_idx = wait_for_leader(&nodes, group_id, Duration::from_secs(10)).await;

    // Write initial data.
    assert!(
        kv_put(&nodes, group_id, b"rc-before", b"val-1", 1).await,
        "initial write should commit"
    );

    // Start a 4th node.
    let server4 = start_test_server(&[
        "--stores",
        "2004",
        "--groups",
        &group_id.to_string(),
        "--replica",
        "4",
    ])
    .await
    .expect("start node 4");
    let node4_id = 2004u64;

    // Get the 4th node's endpoint from its topology.
    let topo4 = topology_or_panic(&ServerNode {
        handle: Some(server4),
        node_id: node4_id,
        replica_id: 4,
    })
    .await;
    let node4_endpoint =
        node_endpoint(&topo4).expect("node 4 topology should have listen_addr after wait_for_ready");

    // Add the 4th node as a remote replica to all existing nodes.
    let add_payload = serde_json::json!([{
        "replica_id": 4,
        "endpoint": node4_endpoint,
    }]);
    for node in &nodes {
        let resp = http_send_with_retry(
            client()
                .post(format!(
                    "{}/stores/{}/groups/{group_id}/remotes",
                    node.mgmt_base(),
                    node.node_id
                ))
                .json(&add_payload),
            "add remote",
        )
        .await;
        assert_eq!(
            resp.status(),
            200,
            "add remote should succeed for node {}",
            node.node_id
        );
    }

    // Give the cluster time to stabilize with the new member.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write after add — should commit with 4-member cluster.
    assert!(
        kv_put(&nodes, group_id, b"rc-after-add", b"val-2", 2).await,
        "write after add-replica should commit"
    );

    // Find a non-leader to remove (among the original 3).
    let leader_idx = wait_for_leader(&nodes, group_id, Duration::from_secs(10)).await;
    let remove_idx = usize::from(leader_idx == 0);
    let remove_replica_id = nodes[remove_idx].replica_id;

    // Remove the non-leader from all other nodes.
    for (i, node) in nodes.iter().enumerate() {
        if i == remove_idx {
            continue;
        }
        let resp = http_send_with_retry(
            client().delete(format!(
                "{}/stores/{}/groups/{group_id}/remotes/{}",
                node.mgmt_base(),
                node.node_id,
                remove_replica_id
            )),
            "remove remote",
        )
        .await;
        assert_eq!(
            resp.status(),
            200,
            "remove remote should succeed for node {}",
            node.node_id
        );
    }

    // Give the cluster time to stabilize with reduced membership.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write after remove — should commit with reduced quorum.
    let remaining_nodes: Vec<&ServerNode> = (0..nodes.len())
        .filter(|i| *i != remove_idx)
        .map(|i| &nodes[i])
        .collect();
    assert!(
        kv_put_nodes(&remaining_nodes, group_id, b"rc-after-remove", b"val-3", 3).await,
        "write after remove-replica should commit"
    );

    // All data survives.
    assert_eq!(
        kv_get_nodes(&remaining_nodes, group_id, b"rc-before")
            .await
            .as_deref(),
        Some(b"val-1".as_slice()),
    );
    assert_eq!(
        kv_get_nodes(&remaining_nodes, group_id, b"rc-after-add")
            .await
            .as_deref(),
        Some(b"val-2".as_slice()),
    );
    assert_eq!(
        kv_get_nodes(&remaining_nodes, group_id, b"rc-after-remove")
            .await
            .as_deref(),
        Some(b"val-3".as_slice()),
    );
}

/// Remove the current leader via the management API: step it down, remove it
/// from the membership, decommission its process, then verify a new leader is
/// elected on the survivors and CRUD still works through the client. Combines
/// the step-down API (`server_api_test`) with the remove-replica API in a
/// single end-to-end workflow.
#[tokio::test]
#[allow(clippy::await_holding_lock)] // serializes real process stacks that share topology ports
async fn reconfig_via_api_remove_leader() {
    let _guard = acquire_guard();
    let group_id = 30;
    let mut nodes = start_cluster(&[3001, 3002, 3003], group_id).await;
    wire_topology(&nodes, group_id).await;

    let leader_idx = wait_for_leader(&nodes, group_id, Duration::from_secs(10)).await;

    // Write initial data through the leader.
    assert!(
        kv_put(&nodes, group_id, b"rmldr-before", b"val-1", 1).await,
        "initial write should commit"
    );

    // 1. Step the leader down via the management API.
    let leader_node_id = nodes[leader_idx].node_id;
    let leader_replica_id = nodes[leader_idx].replica_id;
    let step_url = format!(
        "{}/stores/{}/groups/{}/step-down?sync=true",
        nodes[leader_idx].mgmt_base(),
        leader_node_id,
        group_id,
    );
    let step_resp: Value = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let resp = http_send_with_retry(
                client()
                    .post(&step_url)
                    .json(&serde_json::json!({"reason": "remove-leader reconfig"})),
                "step-down",
            )
            .await;
            match resp.json().await {
                Ok(v) => break v,
                Err(e) => {
                    assert!(
                        Instant::now() <= deadline,
                        "step-down JSON parse from {step_url} failed within 10 s: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    };
    assert_eq!(
        step_resp["accepted"], true,
        "leader should accept its own step-down: {step_resp}"
    );

    // 2. Remove the stepped-down node from every other node's membership.
    for (i, node) in nodes.iter().enumerate() {
        if i == leader_idx {
            continue;
        }
        let resp = http_send_with_retry(
            client().delete(format!(
                "{}/stores/{}/groups/{}/remotes/{}",
                node.mgmt_base(),
                node.node_id,
                group_id,
                leader_replica_id,
            )),
            "remove remote (remove-leader)",
        )
        .await;
        assert_eq!(
            resp.status(),
            200,
            "remove remote should succeed for node {}",
            node.node_id
        );
    }

    // 3. Decommission the removed leader's process so it can no longer
    //    disrupt the survivors' election via higher-term RequestVote rounds
    //    (the vote path has no membership-epoch fence).
    drop(nodes[leader_idx].handle.take());

    // Collect surviving nodes.
    let remaining_nodes: Vec<&ServerNode> = (0..nodes.len())
        .filter(|i| *i != leader_idx)
        .map(|i| &nodes[i])
        .collect();

    // 4. A new leader is elected on the survivors and CRUD still works.
    assert!(
        kv_put_nodes(&remaining_nodes, group_id, b"rmldr-after", b"val-2", 2).await,
        "write after leader removal should commit"
    );

    // All data survives.
    assert_eq!(
        kv_get_nodes(&remaining_nodes, group_id, b"rmldr-before")
            .await
            .as_deref(),
        Some(b"val-1".as_slice()),
    );
    assert_eq!(
        kv_get_nodes(&remaining_nodes, group_id, b"rmldr-after")
            .await
            .as_deref(),
        Some(b"val-2".as_slice()),
    );
}
