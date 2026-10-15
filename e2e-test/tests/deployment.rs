// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, GetOutcome, ReadMode};
mod anonymous;
mod balance;
mod bootstrap;
mod clients;
mod failures;
mod hardware_bootstrap;
mod kv_bootstrap;
mod listeners;
mod storage;
mod supervision;
mod supplied_media;
mod validation;

#[tokio::test]
#[ignore = "requires the normal release-monitor container fixture"]
async fn selected_kv_services_recover_acknowledged_data() {
    witness(false).await;
}

#[tokio::test]
#[ignore = "requires the normal release-monitor container fixture"]
async fn selected_storage_services_recover_acknowledged_data() {
    witness(true).await;
}

#[tokio::test]
#[ignore = "requires the normal release-monitor container fixture"]
async fn complete_release_services_recover_acknowledged_data() {
    witness(true).await;
}

async fn witness(storage: bool) {
    witness_with_write(
        storage,
        std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("write"),
    )
    .await;
}

async fn witness_with_write(storage: bool, write: bool) {
    let addresses: Vec<String> = serde_json::from_str(&std::env::var("CROWDB_E2E_RPC").unwrap()).unwrap();
    let client = CrowdbKvClient::new(ClientConfig::new(vec![format!("http://{}:10000", addresses[0])]));
    let token = std::env::var("CROWDB_E2E_TOKEN").unwrap();
    if write {
        let outcome = client
            .put(0, 1, b"selected-node-witness", token.as_bytes(), None)
            .await
            .unwrap();
        assert!(outcome.revision > 0);
    }
    let hardware =
        crowdb_kv_client::HardwareClient::new(CrowdbKvClient::new(ClientConfig::new(vec![format!(
            "http://{}:10000",
            addresses[0]
        )])));
    let disks = hardware.list_all_disks().await.unwrap();
    if storage {
        assert_eq!(disks.len(), 4);
        assert!(disks
            .iter()
            .all(|disk| disk.value.zone_count == 1 && disk.capacity_bytes() == 16 * 1024 * 1024 * 1024));
        let owner = hardware.get_owner(1, 1, 101).await.unwrap().unwrap();
        assert_eq!(owner.instance_id, 1);
        let binding = hardware.get_bind(1, 1, 101).await.unwrap().unwrap();
        assert_eq!((binding.store_id, binding.group_id), (0, 1));
    } else {
        assert!(disks.is_empty());
    }
    match client
        .get(0, 1, b"selected-node-witness", ReadMode::Linearizable, None)
        .await
        .unwrap()
    {
        GetOutcome::Found { value, .. } => assert_eq!(value.as_ref(), token.as_bytes()),
        GetOutcome::NotFound => panic!("acknowledged selected-node witness is missing"),
    }
}
