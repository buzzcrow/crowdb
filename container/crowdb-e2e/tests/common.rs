// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod test_client;

use bytes::Bytes;
use crowdb_kv::rpc::{KvBatchWriteRequest, KvDeleteRequest, KvErrorCode, KvGetRequest, KvSetRequest};
use crowdb_kv_client::KvRpcTransport;
use serde_json::Value;
use std::sync::Arc;
use test_client::TestKvClient;

pub struct ServerNode {
    pub node_id: u64,
    origin: String,
    rpc_ip: String,
}
impl ServerNode {
    fn mgmt_base(&self) -> &str {
        &self.origin
    }
}

pub fn nodes() -> Vec<ServerNode> {
    let addresses: Vec<String> =
        serde_json::from_str(&std::env::var("CROWDB_E2E_RPC").expect("fixture endpoints"))
            .expect("fixture addresses");
    assert_eq!(addresses.len(), 3);
    addresses
        .into_iter()
        .enumerate()
        .map(|(index, rpc_ip)| ServerNode {
            node_id: u64::try_from(index).unwrap(),
            origin: format!("http://node{index}:7000"),
            rpc_ip,
        })
        .collect()
}
fn client() -> reqwest::Client {
    reqwest::Client::new()
}
async fn topology(node: &ServerNode) -> Value {
    client()
        .get(format!("{}/topology", node.mgmt_base()))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}
async fn node_endpoint(node: &ServerNode) -> String {
    let topo = topology(node).await;
    let address = topo["stores"][0]["listen_addr"].as_str().expect("store endpoint");
    let (_, port) = address.rsplit_once(':').expect("RPC port");
    format!("{}:{port}", node.rpc_ip)
}
pub async fn remotes(node: &ServerNode, group_id: u64) -> Value {
    client()
        .get(format!(
            "{}/stores/{}/groups/{group_id}/remotes",
            node.mgmt_base(),
            node.node_id
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}
pub enum KvOp {
    Put(KvSetRequest),
    Get(KvGetRequest),
    Delete(KvDeleteRequest),
    BatchWrite(KvBatchWriteRequest),
}

/// Execute a KV operation against the current leader, refreshing the leader
/// and retrying on transient failures. This lets tests keep the aggressive
/// `test` election profile while tolerating leader churn on the same physical
/// host. Retries on:
/// - transport errors (Timeout, `ConnectionClosed`, etc.) — the resolved
///   leader may have stepped down to candidate (no KV response) or be
///   briefly unreachable;
/// - `NotLeader` / `Unavailable` response codes — the leader changed or
///   quorum was not yet reached.
///
/// Deterministic errors (`Internal`, unknown codes) fail fast. A legitimate
/// `not_found` (Get on a missing key) is returned immediately.
pub async fn run_kv_op_with_retry(
    nodes: &[ServerNode],
    group_id: u64,
    op: &KvOp,
) -> crowdb_kv::rpc::KvResponse {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let transport = Arc::new(KvRpcTransport::new());
    let mut last_err = String::new();
    while std::time::Instant::now() < deadline {
        let leader_idx = wait_for_leader(nodes, group_id, std::time::Duration::from_secs(10)).await;
        let addr = node_endpoint(&nodes[leader_idx]).await;
        let client = TestKvClient::with_transport(Arc::clone(&transport), format!("http://{addr}"));
        let result = match op {
            KvOp::Put(req) => client.put(req.clone()).await,
            KvOp::Get(req) => client.get(req.clone()).await,
            KvOp::Delete(req) => client.delete(req.clone()).await,
            KvOp::BatchWrite(req) => client.batch_write(req.clone()).await,
        };
        match result {
            Ok(resp) => {
                let resp = resp.into_inner();
                if resp.ok || resp.not_found {
                    return resp;
                }
                // ok=false without not_found: classify by error_code.
                let code = KvErrorCode::try_from(resp.error_code).unwrap_or(KvErrorCode::KvErrorInternal);
                if matches!(
                    code,
                    KvErrorCode::KvErrorNotLeader | KvErrorCode::KvErrorUnavailable
                ) {
                    last_err = resp.error.clone();
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
                panic!("kv rpc failed (error_code={}): {}", resp.error_code, resp.error);
            }
            Err(status) => {
                last_err = status.message().to_string();
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    panic!("kv operation timed out waiting for leader: {last_err}");
}

async fn wait_for_leader(nodes: &[ServerNode], group_id: u64, timeout: std::time::Duration) -> usize {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let mut leaders: Vec<usize> = Vec::new();
        for (idx, node) in nodes.iter().enumerate() {
            let topo = topology(node).await;
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
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!("no unique leader elected for group {group_id} within {timeout:?}");
}

pub async fn write_witness(nodes: &[ServerNode]) {
    let token = std::env::var("CROWDB_E2E_TOKEN").expect("fixture token");
    let response = run_kv_op_with_retry(
        nodes,
        1,
        &KvOp::Put(KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"isolation"),
            value: Bytes::from(token),
            seq: 4,
            ttl_ms: 0,
            client_id: 100,
            request_id: 1004,
            request_create_ms: 10004,
            group_id: 1,
        }),
    )
    .await;
    assert!(response.ok);
    verify_persisted(nodes).await;
}
pub async fn verify_persisted(nodes: &[ServerNode]) {
    for (key, expected) in [
        ("hello", None),
        ("foo", Some("bar".to_owned())),
        (
            "isolation",
            Some(std::env::var("CROWDB_E2E_TOKEN").expect("fixture token")),
        ),
    ] {
        let response = run_kv_op_with_retry(
            nodes,
            1,
            &KvOp::Get(KvGetRequest {
                version: 1,
                key: Bytes::copy_from_slice(key.as_bytes()),
                request_id: 2000,
                request_create_ms: 20000,
                group_id: 1,
                read_mode: 0,
                min_slot: 0,
            }),
        )
        .await;
        if let Some(value) = expected {
            assert!(response.ok);
            assert_eq!(response.value.as_ref(), value.as_bytes());
        } else {
            assert!(response.not_found);
            assert!(response.value.is_empty());
        }
    }
}
