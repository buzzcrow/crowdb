// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{http::*, setup_cluster, state::wait_for_kv_state};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Description of a single group within a restart test.
pub(super) const REPLAY_PUTS: u64 = 50;

pub(super) struct GroupSpec {
    pub(super) store_id: u64,
    pub(super) group_id: u64,
    pub(super) replica_id: u64,
    pub(super) nodes: Vec<u64>,
    pub(super) n_puts: u64,
    pub(super) deleted_keys: Vec<u64>,
}

impl GroupSpec {
    fn expected(&self) -> BTreeMap<String, Option<String>> {
        (1..=self.n_puts)
            .map(|i| {
                let key = format!("k{i}");
                if self.deleted_keys.contains(&i) {
                    (key, None)
                } else {
                    (key, Some(format!("v{i}")))
                }
            })
            .collect()
    }
}

/// Run a full restart-recovery scenario with explicit verification steps:
/// 1. Seed KV data and wait for convergence
/// 2. Verify WAL contents (Accepted records, slot count, watermark)
/// 3. Verify KV state before restart
/// 4. Stop all nodes
/// 5. Verify WAL after stop (unchanged)
/// 6. Restart all nodes
/// 7. Verify WAL after restart (unchanged)
/// 8. Verify KV state after restart
#[allow(clippy::too_many_lines)]
pub(super) async fn restart_recovery(tag: &str, groups: &[GroupSpec]) {
    let cluster = setup_cluster().await;
    eprintln!("runtime logs: {}", cluster.dir.display());

    // Step 0: Initialize the system group so non-zero stores can be created.
    // Preserve the original bounded bootstrap readiness observation.
    let init_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (status, body) = json_post(
            &cluster.client,
            &format!("{}/api/cluster/init", cluster.base),
            json!({ "nodes": cluster.node_ids }),
        )
        .await;
        if status.as_u16() == 201 {
            break;
        }
        if Instant::now() >= init_deadline {
            assert_eq!(status.as_u16(), 201, "cluster init: {body}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Step 1a: Create all stores + groups first (concurrent startup)
    // Collect the union of nodes for each store_id so the store is created
    // on every node that hosts any of its groups.
    let mut store_nodes: std::collections::BTreeMap<u64, Vec<u64>> = std::collections::BTreeMap::new();
    for g in groups {
        store_nodes
            .entry(g.store_id)
            .or_default()
            .extend(g.nodes.iter().copied());
    }
    for (sid, nodes) in &mut store_nodes {
        nodes.sort_unstable();
        nodes.dedup();
        create_store(&cluster.client, &cluster.base, *sid, nodes).await;
    }
    for g in groups {
        create_group(
            &cluster.client,
            &cluster.base,
            g.store_id,
            g.group_id,
            g.replica_id,
            &g.nodes,
        )
        .await;
    }

    // Step 1b: Wait for all groups to elect a leader
    for g in groups {
        wait_for_group_leader(
            &cluster.client,
            &cluster.base,
            g.store_id,
            g.group_id,
            g.nodes.len(),
            Duration::from_secs(3),
        )
        .await;
    }

    // Step 1c: Seed data into all groups (interleaved round-robin so
    // both groups make progress simultaneously without aggressive
    // concurrency that could trigger election churn).
    let max_puts = groups.iter().map(|g| g.n_puts).max().unwrap_or(0);
    for i in 1..=max_puts {
        for g in groups {
            if i <= g.n_puts {
                kv_put(
                    &cluster.client,
                    &cluster.base,
                    g.store_id,
                    g.group_id,
                    &format!("k{i}"),
                    &format!("v{i}"),
                )
                .await;
            }
        }
    }
    for g in groups {
        for &del in &g.deleted_keys {
            kv_delete(
                &cluster.client,
                &cluster.base,
                g.store_id,
                g.group_id,
                &format!("k{del}"),
            )
            .await;
        }
    }

    // Step 2: Verify WAL before restart
    eprintln!("\n=== Step 2: Verify WAL before restart ===");
    let mut pre_wal = Vec::new();
    for g in groups {
        eprintln!(
            "\n-- store {} group {} ({} replicas) --",
            g.store_id,
            g.group_id,
            g.nodes.len()
        );
        // Wait for all replicas to converge to the same max_slot before
        // snapshotting. Without this, a lagging follower can catch up
        // between the snapshot and stop_all, causing a false "WAL changed
        // after stop" failure.
        cluster
            .wait_for_wal_convergence(g.store_id, g.group_id, &g.nodes, Duration::from_secs(5))
            .await;
        let summaries = cluster
            .verify_wal_for_nodes("pre-restart", g.store_id, g.group_id, &g.nodes)
            .await;
        for summary in &summaries {
            assert!(
                summary.max_slot > 0,
                "WAL should have Accepted records before restart"
            );
        }
        pre_wal.push(summaries);
    }

    // Step 3: Verify KV state before restart
    eprintln!("\n=== Step 3: Verify KV state before restart ===");
    for g in groups {
        let expected = g.expected();
        wait_for_kv_state(
            &cluster.client,
            &cluster.base,
            g.store_id,
            g.group_id,
            &expected,
            Duration::from_secs(3),
        )
        .await;
    }

    // Step 4: Stop all nodes
    eprintln!("\n=== Step 4: Stop all nodes ===");
    cluster.stop_all().await;

    // Step 5: Verify WAL after stop (unchanged)
    eprintln!("\n=== Step 5: Verify WAL after stop ===");
    for (gi, g) in groups.iter().enumerate() {
        let post = cluster
            .verify_wal_for_nodes("post-stop", g.store_id, g.group_id, &g.nodes)
            .await;
        for (i, summary) in post.iter().enumerate() {
            assert_eq!(
                summary.max_slot, pre_wal[gi][i].max_slot,
                "WAL max_slot should not change after stop (store {} group {} node {})",
                g.store_id, g.group_id, g.nodes[i]
            );
        }
    }

    // Step 6: Restart all nodes
    eprintln!("\n=== Step 6: Restart all nodes ===");
    cluster.restart_all().await;

    // Count groups per store so we wait for the correct group count.
    let mut store_group_count: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
    for g in groups {
        *store_group_count.entry(g.store_id).or_default() += 1;
    }
    let mut waited_stores: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for g in groups {
        if waited_stores.insert(g.store_id) {
            wait_for_store(
                &cluster.client,
                &cluster.base,
                g.store_id,
                store_group_count[&g.store_id],
                Duration::from_secs(3),
            )
            .await;
        }
        wait_for_group_leader(
            &cluster.client,
            &cluster.base,
            g.store_id,
            g.group_id,
            g.nodes.len(),
            Duration::from_secs(3),
        )
        .await;
    }

    // Confirm leaders are still stable after all nodes are up (no blind sleep).
    for g in groups {
        wait_for_group_leader(
            &cluster.client,
            &cluster.base,
            g.store_id,
            g.group_id,
            g.nodes.len(),
            Duration::from_secs(3),
        )
        .await;
    }

    // Step 7: Verify WAL after restart (unchanged)
    eprintln!("\n=== Step 7: Verify WAL after restart ===");
    for (gi, g) in groups.iter().enumerate() {
        let post = cluster
            .verify_wal_for_nodes("post-restart", g.store_id, g.group_id, &g.nodes)
            .await;
        for (i, summary) in post.iter().enumerate() {
            assert_eq!(
                summary.max_slot, pre_wal[gi][i].max_slot,
                "WAL max_slot should not change after restart (store {} group {} node {})",
                g.store_id, g.group_id, g.nodes[i]
            );
        }
    }

    // Step 8: Verify KV state after restart
    eprintln!("\n=== Step 8: Verify KV state after restart ===");
    for g in groups {
        eprintln!("\n-- store {} group {} --", g.store_id, g.group_id);
        let expected = g.expected();
        wait_for_kv_state(
            &cluster.client,
            &cluster.base,
            g.store_id,
            g.group_id,
            &expected,
            Duration::from_secs(3),
        )
        .await;
    }
    eprintln!("\n=== PASSED: {tag} ===");
}
