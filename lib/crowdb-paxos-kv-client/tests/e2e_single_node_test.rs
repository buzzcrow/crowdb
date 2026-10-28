// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Deterministic client watermark, push handler and scan pagination checks.
//! Public CRUD, CAS, counting and deadlines are owned by container acceptance.

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};

use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_paxos_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv::cluster::paxos_kv_server::PaxosKvServer;
use crowdb_paxos_kv::cluster::px_kv_store::PxKvStore;
use crowdb_paxos_kv::metrics::MetricsRegistry;

use crowdb_paxos_kv_client::{ClientConfig, GetOutcome, PaxosKvClient, ReadMode};

const STORE_ID: u64 = 1;
const GROUP_ID: u64 = 1;

#[tokio::test]
async fn concurrent_watch_subscriptions_keep_independent_push_handlers() {
    let store = Arc::new(PxKvStore::new(STORE_ID, "127.0.0.1:0".parse().unwrap()));
    for group in [1, 2] {
        store.add_group(PxGroup::new(
            group,
            PxLocalReplica::new(STORE_ID, PxLocalReplicaRole::Leader),
        ));
    }
    store.start().await.unwrap();
    let seed = spawn_topology_server(Arc::clone(&store)).await;
    let client = Arc::new(PaxosKvClient::new(ClientConfig::new(vec![seed])));
    let watches = crowdb_paxos_kv_client::WatchNotifyClient::from_shared(Arc::clone(&client));
    let mut first = watches.subscribe(STORE_ID, 1, b"catalog/").unwrap();
    let mut second = watches.subscribe(STORE_ID, 2, b"grant/").unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(10));
        while ![1, 2]
            .into_iter()
            .all(|id| store.get_group(id).unwrap().watch_registry.has_watchers())
        {
            tick.tick().await;
        }
        client
            .put(STORE_ID, 1, b"catalog/head", b"one", None)
            .await
            .unwrap();
        client
            .put(STORE_ID, 2, b"grant/owner", b"two", None)
            .await
            .unwrap();
        let observed = first.notify_rx.recv().await.unwrap();
        assert_eq!(observed.group_id, 1);
        assert_eq!(observed.prefix, b"catalog/");
        assert_eq!(observed.keys, [b"catalog/head".to_vec()]);
        let observed = second.notify_rx.recv().await.unwrap();
        assert_eq!(observed.group_id, 2);
        assert_eq!(observed.prefix, b"grant/");
        assert_eq!(observed.keys, [b"grant/owner".to_vec()]);
    })
    .await
    .expect("both subscriptions must receive their own pushed update");
    drop((first, second));
    store.stop();
    store.join().await;
}

/// Starts a single-node, single-replica group already believing itself
/// leader (mirrors `crowdb_paxos_kv/tests/testkit/cluster.rs::start_cluster_inner`
/// for a 1-node cluster — no election driver needed).
async fn start_single_node_store() -> Arc<PxKvStore> {
    let replica = PxLocalReplica::new(STORE_ID, PxLocalReplicaRole::Leader);
    let store = PxKvStore::new(STORE_ID, "127.0.0.1:0".parse().unwrap());
    let server = Arc::new(store);

    let group = PxGroup::new(GROUP_ID, replica);
    server.add_group(group);
    server.start().await.expect("failed to start KvStore");
    server
}

/// Serves `GET /topology` returning the live `store.status` each time,
/// so the client's cache reflects whatever this store currently knows.
async fn spawn_topology_server(store: Arc<PxKvStore>) -> String {
    async fn handler(State(store): State<Arc<PxKvStore>>) -> Json<serde_json::Value> {
        Json(serde_json::json!({ "stores": [store.status()] }))
    }
    let app = Router::new().route("/topology", get(handler)).with_state(store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn reverse_scan_paginates_without_repeating_boundaries() {
    let replica = PxLocalReplica::new(STORE_ID, PxLocalReplicaRole::Leader);
    let mut store = PxKvStore::new(STORE_ID, "127.0.0.1:0".parse().unwrap());
    store.set_scan_byte_budget(12);
    let store = Arc::new(store);
    store.add_group(PxGroup::new(GROUP_ID, replica));
    store.start().await.expect("start store");
    let seed = spawn_topology_server(store.clone()).await;
    let client = PaxosKvClient::new(ClientConfig::new(vec![seed]));

    for i in 0..8 {
        let key = format!("r:{i}");
        client
            .put(STORE_ID, GROUP_ID, key.as_bytes(), b"value", None)
            .await
            .unwrap();
    }
    let scanned = client
        .scan_reverse(
            STORE_ID,
            GROUP_ID,
            b"r:",
            &[],
            &[],
            0,
            ReadMode::Linearizable,
            None,
            true,
            None,
        )
        .await
        .unwrap();
    let keys: Vec<_> = scanned.items.iter().map(|(key, _)| key.to_vec()).collect();
    assert_eq!(
        keys,
        (0..8)
            .rev()
            .map(|i| format!("r:{i}").into_bytes())
            .collect::<Vec<_>>()
    );
    assert!(scanned.items.iter().all(|(_, value)| value.is_empty()));

    store.stop();
    store.join().await;
}

#[tokio::test]
async fn read_your_writes_uses_auto_tracked_watermark() {
    let store = start_single_node_store().await;
    let seed = spawn_topology_server(store.clone()).await;
    let client = PaxosKvClient::new(ClientConfig::new(vec![seed]));

    assert_eq!(client.read_your_writes_slot(STORE_ID, GROUP_ID), 0);

    let write = client
        .put(STORE_ID, GROUP_ID, b"session:42", b"active", None)
        .await
        .expect("put");
    assert_eq!(client.read_your_writes_slot(STORE_ID, GROUP_ID), write.revision);

    // No explicit `min_slot` -- the client auto-attaches its own
    // last-write watermark for `MinSlot`.
    match client
        .get(STORE_ID, GROUP_ID, b"session:42", ReadMode::MinSlot, None)
        .await
        .unwrap()
    {
        GetOutcome::Found { value, .. } => assert_eq!(value.as_ref(), b"active"),
        GetOutcome::NotFound => panic!("expected to observe our own write"),
    }

    store.stop();
    store.join().await;
}

/// A multi-page `Linearizable` scan pays the leader read barrier once
/// (page 1) then switches subsequent pages to `MinSlot` with page-1's
/// `read_slot` as the freshness floor — skipping the per-page barrier.
/// Verified by checking the store's `read.barrier.l` summary count
/// is 1 (not N) after an N-page scan.
#[tokio::test]
async fn linearizable_multi_page_scan_pays_barrier_once() {
    let registry = Arc::new(Mutex::new(MetricsRegistry::new()));
    let replica = PxLocalReplica::new(STORE_ID, PxLocalReplicaRole::Leader);
    let mut store = PxKvStore::new(STORE_ID, "127.0.0.1:0".parse().unwrap());
    store.set_metrics_registry(Arc::clone(&registry));
    // Small byte budget: 12 keys × (3B key + 20B value) = 23B/entry;
    // budget=100 → 4 entries/page → 3 pages → 3 barriers without the carry.
    store.set_scan_byte_budget(100);
    let server = Arc::new(store);

    let group = PxGroup::new(GROUP_ID, replica);
    server.add_group(group);
    server.start().await.expect("failed to start KvStore");

    let seed = spawn_topology_server(server.clone()).await;
    let client = PaxosKvClient::new(ClientConfig::new(vec![seed]));

    // Write 12 keys: "k00".."k11", each with a 20-byte value.
    for i in 0..12u32 {
        let key = format!("k{i:02}");
        client
            .put(STORE_ID, GROUP_ID, key.as_bytes(), b"v0123456789abcdefxxx", None)
            .await
            .expect("put");
    }

    let scanned = client
        .scan(
            STORE_ID,
            GROUP_ID,
            b"k",
            &[],
            &[],
            0,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .expect("scan");

    // All 12 keys returned in order — no duplicates, no gaps.
    assert_eq!(scanned.items.len(), 12, "all 12 keys returned");
    for (i, (key, _)) in scanned.items.iter().enumerate() {
        assert_eq!(key.as_ref(), format!("k{i:02}").as_bytes(), "key {i} in order");
    }

    // Only page 1 paid the leader barrier; pages 2..3 used MinSlot.
    let snap = registry.lock().unwrap().snapshot("s.1.g.1.read.");
    let barrier_count = snap
        .iter()
        .find(|(n, _)| n.ends_with("read.barrier.l"))
        .and_then(|(_, v)| v.strip_prefix("l:"))
        .and_then(|v| v.split(':').next())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    assert_eq!(
        barrier_count, 1,
        "multi-page Linearizable scan should pay 1 barrier (page 1 only), got {barrier_count}"
    );

    server.stop();
    server.join().await;
}
