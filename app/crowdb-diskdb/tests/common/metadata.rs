// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Pinned in-memory metadata dependency for deterministic Diskdb state tests.
//! Election, server packaging and persisted KV recovery belong to container E2E.

use crowdb_diskdb::ddb_kv_client::DdbKvClient;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient, ServiceRegistryClient};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[allow(dead_code)]
pub struct TestMetadata {
    store: Arc<PxKvStore>,
    pub group0_leader_endpoint: String,
    pub group1_leader_endpoint: String,
}

impl TestMetadata {
    pub async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        for id in [0, 1] {
            store.add_group(PxGroup::new(
                id,
                PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
            ));
        }
        store
            .start()
            .await
            .expect("start pinned memory metadata dependency");
        let endpoint = store.listen_addr().unwrap().to_string();
        Self {
            store,
            group0_leader_endpoint: endpoint.clone(),
            group1_leader_endpoint: endpoint,
        }
    }

    fn client(&self) -> CrowdbKvClient {
        let client = CrowdbKvClient::new(ClientConfig::new(vec![]));
        client.seed_leader(0, 0, self.group0_leader_endpoint.clone());
        client.seed_leader(0, 1, self.group1_leader_endpoint.clone());
        client
    }

    pub fn make_ddb_kv_client(&self) -> DdbKvClient {
        DdbKvClient::new(self.client())
    }

    #[allow(dead_code)]
    pub fn make_hardware_client(&self) -> HardwareClient {
        HardwareClient::new(self.client())
    }

    #[allow(dead_code)]
    pub fn make_service_registry_client(&self) -> ServiceRegistryClient {
        ServiceRegistryClient::new(self.client())
    }
}

impl Drop for TestMetadata {
    fn drop(&mut self) {
        self.store.stop();
    }
}

/// Wait for all disks in a disk-group to transition from Init to Up
/// and have their zones loaded. Polls every 10ms up to 15s.
///
/// Used by `diskdb_e2e_test.rs` and `recovery_test.rs`; other test
/// binaries include `common` without using it, so `dead_code` is
/// suppressed for this shared helper.
#[allow(dead_code)]
pub async fn wait_for_disks_ready(
    container: &crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer,
    dg_id: u64,
    expected_disks: usize,
    expected_zones: u32,
) {
    use crowdb_protocol::common::HwStatus;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(dg) = container.get_disk_group(dg_id) {
            let disks = dg.disk_snapshot();
            let all_ready = disks.len() == expected_disks
                && disks.iter().all(|d| {
                    d.effective_status() == HwStatus::Up
                        && u32::try_from(d.zones.load().len()).unwrap_or(0) == expected_zones
                });
            if all_ready {
                return;
            }
        }
        if Instant::now() > deadline {
            let dg = container.get_disk_group(dg_id);
            let status = match dg {
                Some(dg) => {
                    let disks = dg.disk_snapshot();
                    disks
                        .iter()
                        .map(|d| {
                            let s = d.effective_status();
                            let zc = d.zones.load().len();
                            format!("{s:?}({zc}z)")
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                }
                None => "no dg".to_string(),
            };
            panic!("disks not ready after 15s: {status}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
