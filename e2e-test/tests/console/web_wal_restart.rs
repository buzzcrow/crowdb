// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod http;
mod recovery;
mod state;
mod wal;

use http::*;
use recovery::{restart_recovery, GroupSpec, REPLAY_PUTS};
use serde_json::json;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use wal::*;

struct Cluster {
    base: String,
    client: reqwest::Client,
    dir: PathBuf,
    node_ids: Vec<u64>,
    wal_dirs: Vec<PathBuf>,
}

async fn setup_cluster() -> Cluster {
    let client = reqwest::Client::new();
    let ips: Vec<String> = serde_json::from_str(&std::env::var("CROWDB_E2E_RPC").unwrap()).unwrap();
    if ips.len() > 1 {
        return setup_remote_cluster(client, &ips).await;
    }
    let base = format!("http://{}:9090", ips[0]);
    create_rack(&client, &base, 1).await;
    create_node(&client, &base, 1, 1, "127.0.0.1").await;
    let (status, body) = json_post(
        &client,
        &format!("{base}/api/nodes/1/server/deploy"),
        json!({"rest_port":7000,"rpc_port":7001,"election_profile":"test"}),
    )
    .await;
    assert_eq!(status.as_u16(), 201, "{body}");
    let artifacts = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let mounts: serde_json::Value =
        serde_json::from_slice(&std::fs::read(artifacts.join("mounts.json")).unwrap()).unwrap();
    let dir = PathBuf::from(mounts["host_data"][0].as_str().unwrap()).join("persistent/console/default");
    Cluster {
        base,
        client,
        dir: dir.clone(),
        node_ids: vec![1],
        wal_dirs: vec![dir.join("N-1/waldata")],
    }
}

async fn setup_remote_cluster(client: reqwest::Client, ips: &[String]) -> Cluster {
    let dir = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let mounts: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("mounts.json")).unwrap()).unwrap();
    let count = ips.len() - 1;
    let wal_dirs = mounts["host_data"].as_array().unwrap()[..count]
        .iter()
        .map(|data| PathBuf::from(data.as_str().unwrap()).join("kv/waldata"))
        .collect();
    let base = format!("http://{}:9090", ips[count]);
    create_rack(&client, &base, 1).await;
    if count > 3 {
        create_rack(&client, &base, 2).await;
    }
    for (index, ip) in ips[..count].iter().enumerate() {
        create_node(
            &client,
            &base,
            u64::try_from(index + 1).unwrap(),
            if index < 3 { 1 } else { 2 },
            ip,
        )
        .await;
    }
    Cluster {
        base,
        client,
        dir,
        node_ids: (1..=u64::try_from(count).unwrap()).collect(),
        wal_dirs,
    }
}

impl Cluster {
    fn node_wal_dir(&self, node_id: u64) -> PathBuf {
        self.wal_dirs[usize::try_from(node_id - 1).unwrap()].clone()
    }

    async fn stop_all(&self) {
        if self.node_ids.len() > 1 {
            crate::common::control::node("stop-voters", 0).await;
            return;
        }
        let futs: Vec<_> = self
            .node_ids
            .iter()
            .map(|&nid| stop_server(&self.client, &self.base, nid))
            .collect();
        futures::future::join_all(futs).await;
    }

    async fn restart_all(&self) {
        if self.node_ids.len() > 1 {
            crate::common::control::node("start-voters", 0).await;
            return;
        }
        let ids = self.node_ids.clone();
        let futs: Vec<_> = ids
            .iter()
            .map(|&nid| restart_server(&self.client, &self.base, nid))
            .collect();
        futures::future::join_all(futs).await;
    }

    async fn verify_wal_for_nodes(
        &self,
        label: &str,
        store_id: u64,
        group_id: u64,
        nodes: &[u64],
    ) -> Vec<WalSummary> {
        let mut summaries = Vec::new();
        for node_id in nodes {
            let wal_dir = self.node_wal_dir(*node_id);
            let records = read_wal_records(&wal_dir, store_id, group_id).await;
            let summary = summarize_wal(&records);
            print_wal_summary(
                &format!("{label} node={node_id} store={store_id} group={group_id}"),
                &summary,
            );
            summaries.push(summary);
        }
        summaries
    }

    /// Wait until all nodes in a group have converged to the same WAL
    /// `max_slot`. This prevents a lagging follower from catching up
    /// between the pre-restart WAL snapshot and `stop_all`, which would
    /// cause a false "WAL changed after stop" assertion.
    async fn wait_for_wal_convergence(&self, store_id: u64, group_id: u64, nodes: &[u64], timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            let summaries = self
                .verify_wal_for_nodes("convergence", store_id, group_id, nodes)
                .await;
            let max_slots: Vec<u64> = summaries.iter().map(|s| s.max_slot).collect();
            let min = *max_slots.iter().min().unwrap_or(&0);
            let max = *max_slots.iter().max().unwrap_or(&0);
            if min == max && min > 0 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "WAL did not converge for store {store_id} group {group_id} \
                 within {timeout:?}: slots={max_slots:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[tokio::test]
#[ignore = "requires normal standalone Web and packaged KV with mounted WAL"]
async fn restart_1node_1group() {
    restart_recovery(
        "restart-1n-1g",
        &[GroupSpec {
            store_id: 10,
            group_id: 1,
            replica_id: 1000,
            nodes: vec![1],
            n_puts: REPLAY_PUTS,
            deleted_keys: vec![1, 10, 20, 30, 40, 50],
        }],
    )
    .await;
}

#[tokio::test]
#[ignore = "requires separately owned release KV containers and writable Web"]
async fn restart_3node_1group() {
    restart_recovery(
        "restart-3n-1g",
        &[GroupSpec {
            store_id: 10,
            group_id: 1,
            replica_id: 1000,
            nodes: vec![1, 2, 3],
            n_puts: REPLAY_PUTS,
            deleted_keys: vec![1, 10, 20, 30, 40, 50],
        }],
    )
    .await;
}

#[tokio::test]
#[ignore = "requires separately owned release KV containers and writable Web"]
async fn restart_5node_1group() {
    restart_recovery(
        "restart-5n-1g",
        &[GroupSpec {
            store_id: 10,
            group_id: 1,
            replica_id: 1000,
            nodes: vec![1, 2, 3, 4, 5],
            n_puts: REPLAY_PUTS,
            deleted_keys: vec![1, 5, 10, 20, 30, 40],
        }],
    )
    .await;
}

#[tokio::test]
#[ignore = "requires separately owned release KV containers and writable Web"]
async fn restart_5node_2group() {
    restart_recovery(
        "restart-5n-2g",
        &[
            GroupSpec {
                store_id: 11,
                group_id: 1,
                replica_id: 1000,
                nodes: vec![1, 2, 3, 4, 5],
                n_puts: REPLAY_PUTS,
                deleted_keys: vec![1, 5, 10, 20, 30, 40],
            },
            GroupSpec {
                store_id: 11,
                group_id: 2,
                replica_id: 2000,
                nodes: vec![1, 2, 3],
                n_puts: REPLAY_PUTS,
                deleted_keys: vec![2, 8, 15, 22],
            },
        ],
    )
    .await;
}

#[tokio::test]
#[ignore = "requires separately owned release KV containers and writable Web"]
async fn restart_6node_2group_overlap() {
    restart_recovery(
        "restart-6n-2g-overlap",
        &[
            GroupSpec {
                store_id: 11,
                group_id: 1,
                replica_id: 1000,
                nodes: vec![1, 2, 3, 4, 5],
                n_puts: REPLAY_PUTS,
                deleted_keys: vec![1, 10, 15, 20, 25],
            },
            GroupSpec {
                store_id: 11,
                group_id: 2,
                replica_id: 2000,
                nodes: vec![2, 5, 6],
                n_puts: REPLAY_PUTS,
                deleted_keys: vec![2, 10, 15, 25],
            },
        ],
    )
    .await;
}
