// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::sysdata::DiskdbOwnerEntry;
use crowdb_web::owner_assignment::pick_least_loaded_instance;

fn owner(dg_id: u64, instance_id: u64) -> DiskdbOwnerEntry {
    DiskdbOwnerEntry {
        rack_id: 1,
        node_id: 10,
        dg_id,
        instance_id,
        lease_expiry_ms: 0,
    }
}

#[test]
fn empty_instance_set_has_no_owner() {
    assert_eq!(pick_least_loaded_instance(&[], &[]), None);
}

#[test]
fn selection_uses_least_loaded_then_lowest_id() {
    let owners = vec![owner(1, 1), owner(2, 1), owner(3, 2)];
    assert_eq!(pick_least_loaded_instance(&[1, 2, 3], &owners), Some(3));
    let tied = vec![owner(1, 1), owner(2, 2)];
    assert_eq!(pick_least_loaded_instance(&[1, 2], &tied), Some(1));
}

#[test]
fn thirteen_serial_assignments_across_three_instances_balance_five_four_four() {
    let instances = [1, 2, 3];
    let mut owners = Vec::new();
    for dg_id in 1..=13 {
        let selected = pick_least_loaded_instance(&instances, &owners).expect("eligible owner");
        owners.push(owner(dg_id, selected));
    }
    let mut counts: Vec<_> = instances
        .iter()
        .map(|instance_id| {
            owners
                .iter()
                .filter(|owner| owner.instance_id == *instance_id)
                .count()
        })
        .collect();
    counts.sort_unstable();
    assert_eq!(counts, [4, 4, 5]);
}

#[tokio::test]
async fn new_owner_requires_data_binding_and_preserves_existing_destination() {
    use crowdb_kv_client::{ClientConfig, CrowdbKvClient, CrowdbSysmdClient, HardwareClient};
    use crowdb_web::owner_assignment::ensure_data_binding;
    let cluster = crowdb_test_harness::cluster::KvCluster::start().await;
    let kv = std::sync::Arc::new(CrowdbKvClient::new(ClientConfig::new(
        cluster.mgmt_endpoints.clone(),
    )));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    let hw = HardwareClient::from_shared(kv.clone());
    let sysmd = CrowdbSysmdClient::from_shared(kv);
    assert!(ensure_data_binding(&hw, 1, 1, 1).await.is_err());
    assert!(hw.get_bind(1, 1, 1).await.unwrap().is_none());
    sysmd.add_store(1, &[1]).await.unwrap();
    sysmd.add_group(1, 1).await.unwrap();
    ensure_data_binding(&hw, 1, 1, 1).await.unwrap();
    let binding = hw.get_bind(1, 1, 1).await.unwrap().unwrap();
    assert_eq!((binding.store_id, binding.group_id), (1, 1));
    hw.set_bind(1, 1, 1, 9, 12).await.unwrap();
    ensure_data_binding(&hw, 1, 1, 1).await.unwrap();
    let binding = hw.get_bind(1, 1, 1).await.unwrap().unwrap();
    assert_eq!((binding.store_id, binding.group_id), (9, 12));
}
