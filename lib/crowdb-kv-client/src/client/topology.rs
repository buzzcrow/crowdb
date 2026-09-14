// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Topology cache: `(store_id, group_id) -> leader_endpoint`, sourced from
//! `crowdb-kv-server`'s HTTP management API (`GET /topology`). There is no crowdb-rpc
//! `DescribeCluster` RPC — this is the only discovery mechanism.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use tokio::sync::Mutex as AsyncMutex;

use crowdb_protocol::mgmt::TopologyResponse;

use crate::error::{Error, Result};
use crate::ReadEndpointPolicy;

type GroupKey = (u64, u64);

/// Per-endpoint read statistics owned by one published route generation.
#[derive(Debug, Default)]
pub(super) struct EndpointStats {
    in_flight: AtomicI64,
    rtt_ewma_us: AtomicU64,
}

impl EndpointStats {
    pub(super) fn increment_in_flight(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn decrement_in_flight(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    fn in_flight_count(&self) -> i64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    fn rtt_ewma(&self) -> u64 {
        self.rtt_ewma_us.load(Ordering::Relaxed)
    }

    pub(super) fn record_rtt(&self, rtt_us: u64) {
        let mut old = self.rtt_ewma_us.load(Ordering::Relaxed);
        loop {
            let new = if old == 0 {
                rtt_us
            } else {
                old / 4 * 3 + rtt_us / 4
            };
            match self
                .rtt_ewma_us
                .compare_exchange_weak(old, new, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(actual) => old = actual,
            }
        }
    }
}

#[derive(Debug)]
struct RouteEndpoint {
    endpoint: String,
    stats: Arc<EndpointStats>,
}

#[derive(Debug, Default)]
struct GroupRouteState {
    read_cursor: AtomicU64,
    write_slot_highwater: AtomicU64,
}

#[derive(Debug)]
struct GroupRoute {
    leader: Option<Arc<RouteEndpoint>>,
    replicas: Vec<Arc<RouteEndpoint>>,
    state: Arc<GroupRouteState>,
}

#[derive(Clone, Debug, Default)]
struct TopologySnapshot {
    generation: u64,
    groups: HashMap<GroupKey, Arc<GroupRoute>>,
}

struct FreshRoute {
    key: GroupKey,
    leader: Option<String>,
    replicas: Vec<String>,
}

pub struct TopologyCache {
    seeds: RwLock<Vec<String>>,
    http: reqwest::Client,
    snapshot: ArcSwap<TopologySnapshot>,
    next_generation: AtomicU64,
    min_refresh_interval: Duration,
    /// Single-flight guard: while held, a fetch is either in flight or was
    /// just completed within `min_refresh_interval`. Concurrent `refresh`
    /// callers queue on this lock rather than each issuing their own HTTP
    /// request (: "not a storm").
    refresh_gate: AsyncMutex<Instant>,
}

impl TopologyCache {
    #[must_use]
    pub fn new(seeds: Vec<String>, min_refresh_interval: Duration) -> Self {
        Self {
            seeds: RwLock::new(seeds),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            snapshot: ArcSwap::from_pointee(TopologySnapshot::default()),
            next_generation: AtomicU64::new(1),
            min_refresh_interval,
            // Far enough in the past that the first `refresh` always fetches.
            refresh_gate: AsyncMutex::new(
                Instant::now()
                    .checked_sub(Duration::from_secs(3600))
                    .unwrap_or_else(Instant::now),
            ),
        }
    }

    /// Cached leader endpoint for a group, if known. Never performs I/O.
    #[must_use]
    pub fn leader(&self, store_id: u64, group_id: u64) -> Option<String> {
        self.snapshot
            .load()
            .groups
            .get(&(store_id, group_id))
            .and_then(|route| route.leader.as_ref())
            .map(|leader| leader.endpoint.clone())
    }

    /// Test-only: get the current seed list.
    #[cfg(test)]
    pub fn seeds_for_test(&self) -> Vec<String> {
        self.seeds.read().unwrap().clone()
    }

    /// Test-only: replace the seed list.
    #[cfg(test)]
    pub fn set_seeds_for_test(&self, seeds: Vec<String>) {
        *self.seeds.write().unwrap() = seeds;
    }

    /// Cached full replica endpoint list for a group (local + remotes),
    /// if known. Never performs I/O. Used by the `AnyReplica`
    /// read-endpoint selector; returns `None` until the first
    /// `refresh()` lands a `/topology` body that includes this group.
    #[must_use]
    pub fn replicas(&self, store_id: u64, group_id: u64) -> Option<Vec<String>> {
        self.snapshot
            .load()
            .groups
            .get(&(store_id, group_id))
            .map(|route| {
                route
                    .replicas
                    .iter()
                    .map(|replica| replica.endpoint.clone())
                    .collect()
            })
    }

    /// Directly seed the cache with a leader endpoint learned from a
    /// `NotLeaderHint` on a KV response. Cheaper and more precise than a
    /// full `/topology` refresh since the hint is already the answer.
    pub fn set_leader(&self, store_id: u64, group_id: u64, endpoint: &str) {
        let key = (store_id, group_id);
        loop {
            let current = self.snapshot.load_full();
            let old_route = current.groups.get(&key);
            let leader = Self::reuse_endpoint(old_route, endpoint);
            let route = Arc::new(GroupRoute {
                leader: Some(leader),
                replicas: old_route.map_or_else(Vec::new, |route| route.replicas.clone()),
                state: old_route.map_or_else(
                    || Arc::new(GroupRouteState::default()),
                    |route| Arc::clone(&route.state),
                ),
            });
            let mut groups = current.groups.clone();
            groups.insert(key, route);
            if self.publish(&current, groups) {
                return;
            }
        }
    }

    /// Replace the seed list used for future `/topology` fetches. Lets a
    /// long-lived cache track a growing/changing set of nodes (e.g.
    /// `crowdb-console` adding a server at runtime) without rebuilding the
    /// cache or losing already-learned leader endpoints.
    ///
    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn set_seeds(&self, seeds: Vec<String>) {
        *self.seeds.write().unwrap() = seeds;
    }

    /// Refresh the cache from `/topology` on the first reachable seed.
    /// Coalesces concurrent callers into a single HTTP fetch when they land
    /// within `min_refresh_interval` of each other.
    ///
    /// # Errors
    /// Returns `Error::Topology` only if every seed is unreachable.
    pub async fn refresh(&self) -> Result<()> {
        let mut last = self.refresh_gate.lock().await;
        if last.elapsed() < self.min_refresh_interval {
            return Ok(());
        }
        let base_generation = self.snapshot.load().generation;
        let result = self.fetch_and_merge(base_generation).await;
        *last = Instant::now();
        result
    }

    async fn fetch_and_merge(&self, base_generation: u64) -> Result<()> {
        let seeds = self.seeds.read().unwrap().clone();
        if seeds.is_empty() {
            return Err(Error::NoSeeds);
        }
        let mut last_err = None;
        for seed in &seeds {
            let url = format!("{}/topology", seed.trim_end_matches('/'));
            match self.http.get(&url).send().await {
                Ok(resp) => match resp.json::<TopologyResponse>().await {
                    Ok(body) => {
                        self.merge_from_generation(body, base_generation);
                        return Ok(());
                    }
                    Err(e) => last_err = Some(format!("{seed}: decode error: {e}")),
                },
                Err(e) => last_err = Some(format!("{seed}: request error: {e}")),
            }
        }
        Err(Error::Topology(
            last_err.unwrap_or_else(|| "all seeds returned errors".to_string()),
        ))
    }

    #[cfg(test)]
    fn merge(&self, body: TopologyResponse) {
        let base_generation = self.snapshot.load().generation;
        self.merge_from_generation(body, base_generation);
    }

    fn merge_from_generation(&self, body: TopologyResponse, base_generation: u64) {
        let fresh_stores: HashSet<u64> = body.stores.iter().map(|s| s.store_id).collect();
        let mut fresh_routes = Vec::new();
        for store in body.stores {
            let local_endpoint = store.listen_addr.clone();
            for group in store.groups {
                let leader_id = group.leader_id;
                let key = (store.store_id, group.group_id);
                let leader = if leader_id == 0 {
                    None
                } else if group.local_replica.id == leader_id {
                    local_endpoint.clone()
                } else {
                    group
                        .remotes
                        .iter()
                        .find(|r| r.id == leader_id)
                        .map(|r| r.endpoint.clone())
                };
                let mut replicas: Vec<String> = Vec::with_capacity(group.remotes.len() + 1);
                if let Some(addr) = &local_endpoint {
                    replicas.push(addr.clone());
                }
                for r in &group.remotes {
                    replicas.push(r.endpoint.clone());
                }
                fresh_routes.push(FreshRoute {
                    key,
                    leader,
                    replicas,
                });
            }
        }

        loop {
            let current = self.snapshot.load_full();
            let stale_refresh = current.generation != base_generation;
            let mut groups = current.groups.clone();
            if !stale_refresh {
                groups.retain(|(store_id, _), _| !fresh_stores.contains(store_id));
            }
            for fresh in &fresh_routes {
                let old_route = current.groups.get(&fresh.key);
                let leader_endpoint = if stale_refresh {
                    old_route.map_or_else(
                        || fresh.leader.clone(),
                        |route| route.leader.as_ref().map(|leader| leader.endpoint.clone()),
                    )
                } else {
                    fresh.leader.clone()
                };
                let (leader, replicas) =
                    Self::build_endpoints(old_route, leader_endpoint.as_deref(), &fresh.replicas);
                let state = old_route.map_or_else(
                    || Arc::new(GroupRouteState::default()),
                    |route| Arc::clone(&route.state),
                );
                groups.insert(
                    fresh.key,
                    Arc::new(GroupRoute {
                        leader,
                        replicas,
                        state,
                    }),
                );
            }
            if self.publish(&current, groups) {
                return;
            }
        }
    }

    pub(super) fn select_replica(
        &self,
        store_id: u64,
        group_id: u64,
        policy: ReadEndpointPolicy,
    ) -> Option<String> {
        let snapshot = self.snapshot.load();
        let route = snapshot.groups.get(&(store_id, group_id))?;
        if route.replicas.is_empty() {
            return None;
        }
        let cursor = route.state.read_cursor.fetch_add(1, Ordering::Relaxed);
        let rr_index = usize::try_from(cursor).unwrap_or(0) % route.replicas.len();
        let index = match policy {
            ReadEndpointPolicy::Leader | ReadEndpointPolicy::AnyReplica => rr_index,
            ReadEndpointPolicy::LeastConnections => Self::least_loaded(route, rr_index),
            ReadEndpointPolicy::Latency => Self::lowest_latency(route, rr_index),
        };
        Some(route.replicas[index].endpoint.clone())
    }

    pub(super) fn endpoint_stats(&self, store_id: u64, group_id: u64, endpoint: &str) -> Arc<EndpointStats> {
        let snapshot = self.snapshot.load();
        snapshot
            .groups
            .get(&(store_id, group_id))
            .and_then(|route| Self::find_endpoint(route, endpoint))
            .map_or_else(
                || Arc::new(EndpointStats::default()),
                |route| Arc::clone(&route.stats),
            )
    }

    pub(super) fn record_endpoint_rtt(&self, store_id: u64, group_id: u64, endpoint: &str, rtt_us: u64) {
        let snapshot = self.snapshot.load();
        if let Some(route) = snapshot
            .groups
            .get(&(store_id, group_id))
            .and_then(|route| Self::find_endpoint(route, endpoint))
        {
            route.stats.record_rtt(rtt_us);
        }
    }

    pub(super) fn record_write(&self, store_id: u64, group_id: u64, revision: u64) {
        if let Some(route) = self.snapshot.load().groups.get(&(store_id, group_id)) {
            route
                .state
                .write_slot_highwater
                .fetch_max(revision, Ordering::Relaxed);
        }
    }

    pub(super) fn write_slot_highwater(&self, store_id: u64, group_id: u64) -> u64 {
        self.snapshot
            .load()
            .groups
            .get(&(store_id, group_id))
            .map_or(0, |route| {
                route.state.write_slot_highwater.load(Ordering::Relaxed)
            })
    }

    fn least_loaded(route: &GroupRoute, rr_index: usize) -> usize {
        let mut best_index = rr_index;
        let mut best_count = route.replicas[rr_index].stats.in_flight_count();
        for (index, endpoint) in route.replicas.iter().enumerate() {
            let count = endpoint.stats.in_flight_count();
            if count < best_count {
                best_count = count;
                best_index = index;
            }
        }
        best_index
    }

    fn lowest_latency(route: &GroupRoute, rr_index: usize) -> usize {
        let mut best_index = rr_index;
        let mut best_rtt = route.replicas[rr_index].stats.rtt_ewma();
        for (index, endpoint) in route.replicas.iter().enumerate() {
            let rtt = endpoint.stats.rtt_ewma();
            if rtt > 0 && best_rtt > 0 && rtt < best_rtt {
                best_rtt = rtt;
                best_index = index;
            }
        }
        best_index
    }

    fn find_endpoint<'a>(route: &'a GroupRoute, endpoint: &str) -> Option<&'a Arc<RouteEndpoint>> {
        route
            .leader
            .iter()
            .chain(route.replicas.iter())
            .find(|candidate| candidate.endpoint == endpoint)
    }

    fn reuse_endpoint(old_route: Option<&Arc<GroupRoute>>, endpoint: &str) -> Arc<RouteEndpoint> {
        old_route
            .and_then(|route| Self::find_endpoint(route, endpoint))
            .cloned()
            .unwrap_or_else(|| {
                Arc::new(RouteEndpoint {
                    endpoint: endpoint.to_string(),
                    stats: Arc::new(EndpointStats::default()),
                })
            })
    }

    fn build_endpoints(
        old_route: Option<&Arc<GroupRoute>>,
        leader: Option<&str>,
        replicas: &[String],
    ) -> (Option<Arc<RouteEndpoint>>, Vec<Arc<RouteEndpoint>>) {
        let mut endpoints = HashMap::<String, Arc<RouteEndpoint>>::new();
        let mut resolve = |endpoint: &str| {
            endpoints
                .entry(endpoint.to_string())
                .or_insert_with(|| Self::reuse_endpoint(old_route, endpoint))
                .clone()
        };
        let leader = leader.map(&mut resolve);
        let replicas = replicas.iter().map(|endpoint| resolve(endpoint)).collect();
        (leader, replicas)
    }

    fn publish(&self, current: &Arc<TopologySnapshot>, groups: HashMap<GroupKey, Arc<GroupRoute>>) -> bool {
        let replacement = Arc::new(TopologySnapshot {
            generation: self.next_generation(),
            groups,
        });
        let previous = self.snapshot.compare_and_swap(current, replacement);
        Arc::ptr_eq(&previous, current)
    }

    fn next_generation(&self) -> u64 {
        loop {
            let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
            if generation != 0 {
                return generation;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use axum::extract::State;
    use axum::routing::get;
    use axum::{Json, Router};

    use super::*;

    /// Spawns a minimal axum server serving `/topology` from a fixed body,
    /// counting hits via `counter`. Returns the bound `http://host:port`.
    async fn spawn_topology_server(body: serde_json::Value, counter: Arc<AtomicUsize>) -> String {
        async fn handler(
            State((body, counter)): State<(serde_json::Value, Arc<AtomicUsize>)>,
        ) -> Json<serde_json::Value> {
            counter.fetch_add(1, Ordering::SeqCst);
            Json(body)
        }
        let app = Router::new()
            .route("/topology", get(handler))
            .with_state((body, counter));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    fn sample_topology(store_id: u64, group_id: u64, leader_endpoint: &str) -> serde_json::Value {
        serde_json::json!({
            "stores": [{
                "store_id": store_id,
                "listen_addr": leader_endpoint,
                "status": "ok",
                "groups": [{
                    "group_id": group_id,
                    "leader_id": 1,
                    "local_replica_id": 1,
                    "force_classic": false,
                    "status": "ok",
                    "local_replica": {
                        "id": 1,
                        "role": "leader",
                        "voting": true,
                        "status": "ok",
                        "kv_store": { "engine_healthy": true }
                    },
                    "remotes": []
                }]
            }]
        })
    }

    #[tokio::test]
    async fn refresh_populates_leader_from_local_replica() {
        let counter = Arc::new(AtomicUsize::new(0));
        let seed = spawn_topology_server(sample_topology(7, 42, "http://10.0.0.1:9001"), counter).await;
        let cache = TopologyCache::new(vec![seed], Duration::from_millis(50));

        cache.refresh().await.unwrap();

        assert_eq!(cache.leader(7, 42), Some("http://10.0.0.1:9001".to_string()));
    }

    /// A topology refresh that reports `leader_id == 0` (mid-election)
    /// must evict a previously-cached leader endpoint. Without this, the
    /// client keeps sending to a replica that stepped down and never
    /// discovers the new leader once election completes.
    #[tokio::test]
    async fn refresh_with_no_leader_evicts_stale_cached_leader() {
        let counter = Arc::new(AtomicUsize::new(0));
        let seed_leader =
            spawn_topology_server(sample_topology(1, 1, "http://10.0.0.1:9001"), counter.clone()).await;
        let cache = TopologyCache::new(vec![seed_leader], Duration::from_millis(50));

        // First refresh: leader is present.
        cache.refresh().await.unwrap();
        assert_eq!(cache.leader(1, 1), Some("http://10.0.0.1:9001".to_string()));

        // Swap seed to a server reporting leader_id == 0 (mid-election).
        let no_leader_body = serde_json::json!({
            "stores": [{
                "store_id": 1,
                "listen_addr": "http://10.0.0.1:9001",
                "status": "ok",
                "groups": [{
                    "group_id": 1,
                    "leader_id": 0,
                    "local_replica_id": 1,
                    "force_classic": false,
                    "status": "ok",
                    "local_replica": {
                        "id": 1,
                        "role": "follower",
                        "voting": true,
                        "status": "ok",
                        "kv_store": { "engine_healthy": true }
                    },
                    "remotes": []
                }]
            }]
        });
        let seed_no_leader = spawn_topology_server(no_leader_body, Arc::new(AtomicUsize::new(0))).await;
        {
            let mut seeds = cache.seeds_for_test();
            seeds[0] = seed_no_leader;
            cache.set_seeds(seeds);
        }

        // Wait past the coalescing window so the next refresh actually fetches.
        tokio::time::sleep(Duration::from_millis(60)).await;
        cache.refresh().await.unwrap();

        // Stale leader must be gone — the client should see "no leader"
        // and retry until a new leader is elected.
        assert_eq!(cache.leader(1, 1), None);
    }

    #[tokio::test]
    async fn concurrent_refresh_coalesces_into_one_fetch() {
        let counter = Arc::new(AtomicUsize::new(0));
        let seed = spawn_topology_server(sample_topology(1, 1, "http://x:1"), counter.clone()).await;
        let cache = Arc::new(TopologyCache::new(vec![seed], Duration::from_secs(5)));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            handles.push(tokio::spawn(async move { cache.refresh().await }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }

        // All 8 concurrent refreshes land within the 5s coalescing window,
        // so exactly one HTTP fetch should have happened.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn set_leader_overrides_cache_without_io() {
        let cache = TopologyCache::new(vec!["http://unused:1".to_string()], Duration::from_secs(60));
        cache.set_leader(3, 9, "http://leader:8080");
        assert_eq!(cache.leader(3, 9), Some("http://leader:8080".to_string()));
    }

    /// Build a topology body with multiple groups in one store.
    fn multi_group_topology(store_id: u64, group_ids: &[u64], leader_endpoint: &str) -> serde_json::Value {
        let groups: Vec<serde_json::Value> = group_ids
            .iter()
            .map(|gid| {
                serde_json::json!({
                    "group_id": gid,
                    "leader_id": 1,
                    "local_replica_id": 1,
                    "force_classic": false,
                    "status": "ok",
                    "local_replica": {
                        "id": 1,
                        "role": "leader",
                        "voting": true,
                        "status": "ok",
                        "kv_store": { "engine_healthy": true }
                    },
                    "remotes": []
                })
            })
            .collect();
        serde_json::json!({
            "stores": [{
                "store_id": store_id,
                "listen_addr": leader_endpoint,
                "status": "ok",
                "groups": groups
            }]
        })
    }

    #[tokio::test]
    async fn merge_evicts_groups_absent_from_fresh_body() {
        let counter = Arc::new(AtomicUsize::new(0));
        // First server: 3 groups (1, 2, 3).
        let seed_full = spawn_topology_server(
            multi_group_topology(1, &[1, 2, 3], "http://10.0.0.1:9001"),
            counter.clone(),
        )
        .await;
        // Second server: 2 groups (1, 2) — group 3 is gone.
        let seed_partial =
            spawn_topology_server(multi_group_topology(1, &[1, 2], "http://10.0.0.1:9001"), counter).await;

        let cache = TopologyCache::new(vec![seed_full], Duration::from_millis(50));
        cache.refresh().await.unwrap();
        assert_eq!(cache.leader(1, 1), Some("http://10.0.0.1:9001".to_string()));
        assert_eq!(cache.leader(1, 2), Some("http://10.0.0.1:9001".to_string()));
        assert_eq!(cache.leader(1, 3), Some("http://10.0.0.1:9001".to_string()));

        // Swap the seed to the partial server and wait past the refresh
        // interval so the next refresh fetches from it.
        {
            let mut seeds = cache.seeds_for_test();
            seeds[0] = seed_partial.clone();
            cache.set_seeds_for_test(seeds);
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        cache.refresh().await.unwrap();

        // Groups 1 and 2 survive; group 3 is evicted.
        assert_eq!(cache.leader(1, 1), Some("http://10.0.0.1:9001".to_string()));
        assert_eq!(cache.leader(1, 2), Some("http://10.0.0.1:9001".to_string()));
        assert_eq!(cache.leader(1, 3), None);
    }

    #[tokio::test]
    async fn merge_eviction_retires_group_highwater() {
        let counter = Arc::new(AtomicUsize::new(0));
        let seed_full = spawn_topology_server(
            multi_group_topology(1, &[1, 2, 3], "http://10.0.0.1:9001"),
            counter.clone(),
        )
        .await;
        let seed_partial =
            spawn_topology_server(multi_group_topology(1, &[1, 2], "http://10.0.0.1:9001"), counter).await;

        let cache = TopologyCache::new(vec![seed_full], Duration::from_millis(50));
        cache.refresh().await.unwrap();
        cache.record_write(1, 3, 99);
        assert_eq!(cache.write_slot_highwater(1, 3), 99);

        {
            let mut seeds = cache.seeds_for_test();
            seeds[0] = seed_partial.clone();
            cache.set_seeds_for_test(seeds);
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        cache.refresh().await.unwrap();

        assert_eq!(cache.write_slot_highwater(1, 3), 0);
        cache.set_leader(1, 3, "http://replacement:1");
        assert_eq!(cache.write_slot_highwater(1, 3), 0);
    }

    #[test]
    fn stale_refresh_does_not_overwrite_newer_leader_hint() {
        let cache = TopologyCache::new(Vec::new(), Duration::from_secs(1));
        let first: TopologyResponse = serde_json::from_value(sample_topology(1, 1, "http://old:1")).unwrap();
        cache.merge(first);
        let stale_base = cache.snapshot.load().generation;
        cache.set_leader(1, 1, "http://hint:2");

        let stale: TopologyResponse =
            serde_json::from_value(sample_topology(1, 1, "http://stale:3")).unwrap();
        cache.merge_from_generation(stale, stale_base);

        assert_eq!(cache.leader(1, 1).as_deref(), Some("http://hint:2"));
    }

    #[test]
    fn leader_and_replicas_are_read_from_one_snapshot() {
        let cache = TopologyCache::new(Vec::new(), Duration::from_secs(1));
        let old: TopologyResponse = serde_json::from_value(sample_topology(1, 1, "http://old:1")).unwrap();
        cache.merge(old);
        let snapshot = cache.snapshot.load_full();
        let route = snapshot.groups.get(&(1, 1)).unwrap();
        let leader = route.leader.as_ref().unwrap().endpoint.clone();
        let replicas: Vec<_> = route
            .replicas
            .iter()
            .map(|endpoint| endpoint.endpoint.clone())
            .collect();

        assert_eq!(leader, "http://old:1");
        assert_eq!(replicas, vec!["http://old:1"]);
    }
}
