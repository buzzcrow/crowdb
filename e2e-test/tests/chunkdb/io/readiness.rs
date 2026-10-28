// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_diskio_client::{DiskioClient, DiskioClientConfig};
use crowdb_paxos_kv_client::{HardwareClient, ServiceRegistryClient};
use crowdb_protocol::common::InstanceValue;

fn current_owners(instances: &[(u64, InstanceValue)], started_at_ms: u64) -> bool {
    (0..6).all(|offset| {
        instances.iter().any(|(id, value)| {
            *id == 8000 + offset
                && value.last_heartbeat_ms >= started_at_ms
                && value
                    .extra
                    .as_ref()
                    .and_then(|extra| extra.diskdb.as_ref())
                    .is_some_and(|extra| extra.owned_dg_ids == [1000 + offset])
        })
    })
}

pub(super) async fn probe(
    service: &ServiceRegistryClient,
    hardware: &HardwareClient,
    started_at_ms: u64,
) -> Result<(), String> {
    let instances = service
        .read_all_diskio_instances()
        .await
        .map_err(|error| error.to_string())?;
    // A retained registration can pass TTL filtering before the restarted
    // process publishes its initial empty ownership. Require this boot's
    // complete ownership heartbeat before starting dependent services.
    if !current_owners(&instances, started_at_ms) {
        return Err(format!(
            "waiting for DiskIO ownership since {started_at_ms}: {instances:?}"
        ));
    }
    let client =
        DiskioClient::connect_with_clients(service.clone(), hardware.clone(), DiskioClientConfig::default())
            .await
            .map_err(|error| error.to_string())?;
    assert_eq!(client.status().disks, 18);
    Ok(())
}

fn observations(heartbeat_ms: u64) -> Vec<(u64, InstanceValue)> {
    use crowdb_protocol::common::{DiskdbExtra, ServiceExtra};
    (0..6)
        .map(|offset| {
            let id = 8000 + offset;
            (
                id,
                InstanceValue {
                    instance_id: id,
                    rpc_endpoint: format!("10.20.30.{}:7200", offset + 7),
                    last_heartbeat_ms: heartbeat_ms,
                    extra: Some(ServiceExtra {
                        diskdb: Some(DiskdbExtra {
                            owned_dg_ids: vec![1000 + offset],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                },
            )
        })
        .collect()
}

#[test]
fn retained_live_registration_cannot_satisfy_restart_readiness() {
    let before = observations(1000);
    // All records are still within the 15-second liveness TTL.
    assert!(!current_owners(&before, 1001));
    let mut restarting = observations(1002);
    restarting[5]
        .1
        .extra
        .as_mut()
        .unwrap()
        .diskdb
        .as_mut()
        .unwrap()
        .owned_dg_ids
        .clear();
    assert!(!current_owners(&restarting, 1001));
    assert!(current_owners(&observations(1003), 1001));
}

#[test]
fn every_expected_instance_must_publish_its_group_after_restart() {
    let mut missing = observations(1002);
    missing.pop();
    assert!(!current_owners(&missing, 1001));
    let mut wrong_owner = observations(1002);
    wrong_owner[5].0 = 9005;
    assert!(!current_owners(&wrong_owner, 1001));
}
