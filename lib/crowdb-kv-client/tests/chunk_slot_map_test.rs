// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::cluster::px_kv_store::PxKvStore;
use crowdb_kv_client::{ChunkSlotMapClient, ClientConfig, CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_protocol::chunk_slot::{
    ChunkSlot, ChunkSlotBinding, ChunkSlotBitmap, ChunkSlotMap, ChunkSlotMapHead, ChunkStorageGroup,
    CHUNK_SLOT_LAYOUT_VERSION,
};
use crowdb_protocol::key::{ChunkServiceSlotsKey, ChunkSlotMapHeadKey, TextKey};

struct TestGroup0 {
    store: Arc<PxKvStore>,
    kv: Arc<CrowdbKvClient>,
}

impl TestGroup0 {
    async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new())));
        kv.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
        Self { store, kv }
    }
}

impl Drop for TestGroup0 {
    fn drop(&mut self) {
        self.store.stop();
    }
}

fn services(owner: u64) -> ChunkSlotMap<u64> {
    ChunkSlotMap::new(
        ChunkSlotMapHead {
            layout_version: CHUNK_SLOT_LAYOUT_VERSION,
            generation: 1,
            owner_count: 2,
        },
        vec![
            ChunkSlotBinding {
                generation: 1,
                owner,
                slots: ChunkSlot::all().collect(),
            },
            ChunkSlotBinding {
                generation: 1,
                owner: owner + 1,
                slots: ChunkSlotBitmap::default(),
            },
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn initialize_is_atomic_idempotent_and_layer_independent() {
    let test = TestGroup0::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let service = services(10);
    client.initialize_service(&service).await.unwrap();
    client.initialize_service(&service).await.unwrap();
    assert!(client.read_storage().await.is_err());
    let loaded = client.read_service().await.unwrap();
    assert_eq!(loaded.bindings(), service.bindings());
    let prefix = ChunkServiceSlotsKey::prefix_all();
    let page = test
        .kv
        .scan_bounded_at(0, 0, prefix.as_bytes(), &[], &[], 256, false, None, 0)
        .await
        .unwrap();
    let key = ChunkSlotMapHeadKey::Service.to_path();
    let GetOutcome::Found { revision, .. } = test
        .kv
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("missing head");
    };
    assert_eq!(page.items.len(), 2);
    assert!(page.commit_slots.iter().all(|slot| *slot == revision));
    let storage = ChunkSlotMap::new(
        ChunkSlotMapHead {
            generation: 19,
            owner_count: 3,
            ..service.head().clone()
        },
        (1..=3)
            .map(|group_id| ChunkSlotBinding {
                generation: 19,
                owner: ChunkStorageGroup {
                    store_id: 0,
                    group_id,
                },
                slots: ChunkSlot::all()
                    .filter(|slot| u64::from(slot.value()) % 3 + 1 == group_id)
                    .collect(),
            })
            .collect(),
    )
    .unwrap();
    client.initialize_storage(&storage).await.unwrap();
    let loaded_storage = client.read_storage().await.unwrap();
    for slot in ChunkSlot::all() {
        assert_eq!(loaded_storage.owner(slot), storage.owner(slot));
    }
    assert_eq!(client.read_service().await.unwrap().head().generation, 1);
    assert!(client.initialize_service(&services(30)).await.is_err());
    assert_eq!(
        client.read_service().await.unwrap().bindings(),
        service.bindings()
    );
}

#[tokio::test]
async fn concurrent_initializers_cannot_combine_different_assignments() {
    let test = TestGroup0::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let first = services(10);
    let second = services(30);
    let (a, b) = tokio::join!(
        client.initialize_service(&first),
        client.initialize_service(&second)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let actual = client.read_service().await.unwrap();
    assert_eq!(
        actual.bindings(),
        if a.is_ok() {
            first.bindings()
        } else {
            second.bindings()
        }
    );
}

#[tokio::test]
async fn legacy_records_are_never_overwritten() {
    let test = TestGroup0::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    test.kv
        .put(0, 0, b"/chunkdb/range_bind/0", b"legacy", None)
        .await
        .unwrap();
    assert!(client.initialize_service(&services(10)).await.is_err());
    assert!(client.read_service().await.is_err());
    assert!(matches!(
        test.kv
            .get(
                0,
                0,
                ChunkSlotMapHeadKey::Service.to_path().as_bytes(),
                ReadMode::Linearizable,
                None
            )
            .await
            .unwrap(),
        GetOutcome::NotFound
    ));
}

#[tokio::test]
async fn corrupt_owner_and_partial_generation_fail_closed() {
    let test = TestGroup0::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    client.initialize_service(&services(10)).await.unwrap();
    let key = ChunkServiceSlotsKey { instance_id: 10 }.to_path();
    let mut binding = services(10).bindings()[0].clone();
    binding.generation = 2;
    test.kv
        .put(0, 0, key.as_bytes(), &serde_json::to_vec(&binding).unwrap(), None)
        .await
        .unwrap();
    assert!(client.read_service().await.is_err());
    binding.generation = 1;
    binding.owner = 12;
    test.kv
        .put(0, 0, key.as_bytes(), &serde_json::to_vec(&binding).unwrap(), None)
        .await
        .unwrap();
    assert!(client.read_service().await.is_err());
}

#[tokio::test]
async fn paginated_owner_records_preserve_empty_instances() {
    let test = TestGroup0::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let map = ChunkSlotMap::new(
        ChunkSlotMapHead {
            layout_version: CHUNK_SLOT_LAYOUT_VERSION,
            generation: 1,
            owner_count: 300,
        },
        (1..=300)
            .map(|owner| ChunkSlotBinding {
                generation: 1,
                owner,
                slots: if owner == 1 {
                    ChunkSlot::all().collect()
                } else {
                    ChunkSlotBitmap::default()
                },
            })
            .collect(),
    )
    .unwrap();
    client.initialize_service(&map).await.unwrap();
    let actual = client.read_service().await.unwrap();
    assert_eq!(actual.bindings().len(), 300);
    assert_eq!(
        actual.bindings().iter().filter(|b| b.slots.is_empty()).count(),
        299
    );
    assert!(ChunkSlot::all().all(|slot| actual.owner(slot) == 1));
}

#[tokio::test]
async fn orphan_binding_is_preserved_without_creating_a_head() {
    let test = TestGroup0::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let map = services(10);
    let key = ChunkServiceSlotsKey { instance_id: 10 }.to_path();
    let bytes = serde_json::to_vec(&map.bindings()[0]).unwrap();
    test.kv.put(0, 0, key.as_bytes(), &bytes, None).await.unwrap();
    assert!(client.initialize_service(&map).await.is_err());
    let GetOutcome::Found { value, .. } = test
        .kv
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("orphan record was lost");
    };
    assert_eq!(value.as_ref(), bytes.as_slice());
    assert!(client.read_service().await.is_err());
}
