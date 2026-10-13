// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_kv_client::{ChunkSlotMapClient, CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_kv_client::{RangeBindingClient, ServiceRegistryClient};
use crowdb_protocol::chunk_slot::{
    ChunkSlot, ChunkSlotBinding, ChunkSlotBitmap, ChunkSlotMap, ChunkSlotMapHead, ChunkStorageGroup,
    CHUNK_SLOT_LAYOUT_VERSION,
};
use crowdb_protocol::key::{ChunkServiceSlotsKey, ChunkSlotMapHeadKey, TextKey};

pub(super) struct TestGroup0 {
    pub(super) kv: Arc<CrowdbKvClient>,
}
impl TestGroup0 {
    pub(super) async fn start() -> Self {
        Self {
            kv: Arc::new(super::client_data::fixture_client().await),
        }
    }
}

pub(super) fn services(owner: u64) -> ChunkSlotMap<u64> {
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
#[ignore = "requires the owned release-container Group-0 slot map profile"]
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
#[ignore = "requires the owned release-container Group-0 slot map profile"]
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
#[ignore = "requires the owned release-container Group-0 slot map profile"]
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
#[ignore = "requires the owned release-container Group-0 slot map profile"]
async fn service_client_needs_no_storage_map_and_refreshes_same_owner_endpoints() {
    let test = TestGroup0::start().await;
    let maps = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    maps.initialize_service(&services(10)).await.unwrap();
    let registry = ServiceRegistryClient::from_shared(Arc::clone(&test.kv));
    registry.register_chunkdb(10, "127.0.0.1:17010").await.unwrap();
    registry.register_chunkdb(11, "127.0.0.1:17011").await.unwrap();
    let routes = RangeBindingClient::from_shared(Arc::clone(&test.kv));
    routes.refresh().await.unwrap();
    assert!(maps.read_storage().await.is_err());
    for slot in ChunkSlot::all() {
        let route = routes.route_slot(slot).unwrap();
        assert_eq!(route.instance_id, 10);
        assert_eq!(route.rpc_endpoint, "127.0.0.1:17010");
    }
    assert_eq!(routes.snapshot().len(), 2);
    assert!(routes.snapshot()[1].slots.is_empty());
    registry.register_chunkdb(10, "127.0.0.1:18010").await.unwrap();
    routes.refresh().await.unwrap();
    assert_eq!(
        routes
            .route_slot(ChunkSlot::try_from(0).unwrap())
            .unwrap()
            .rpc_endpoint,
        "127.0.0.1:18010"
    );
    registry.unregister("chunkdb", 10).await.unwrap();
    routes.refresh().await.unwrap();
    assert!(routes.route_slot(ChunkSlot::try_from(0).unwrap()).is_err());
    assert_eq!(
        maps.read_service().await.unwrap().bindings(),
        services(10).bindings()
    );
}
