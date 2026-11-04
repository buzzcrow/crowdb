// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_diskdb::{ddb_kv_client::DdbKvClient, recovery::ZoneLoader};
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::common::{DiskId, HwStatus};
use crowdb_protocol::diskdb::rpc::DiskValue;
use std::sync::Arc;

#[path = "common/journal_recovery.rs"]
mod journal_recovery;

#[tokio::test]
async fn restart_with_reclaimed_journal_rebuilds_busy_units_before_allocating() {
    let disk_id = DiskId { high: 10, low: 20 };
    let restored = journal_recovery::TestReclaimedJournal::replica(disk_id).await;
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        0,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    store.add_group(PxGroup::new(1, restored));
    store.start().await.unwrap();
    let client = CrowdbKvClient::new(ClientConfig::new(vec![]));
    let endpoint = store.listen_addr().unwrap().to_string();
    for group in [0, 1] {
        client.seed_leader(0, group, endpoint.clone());
    }
    let loader = ZoneLoader::new(Arc::new(DdbKvClient::new(client)), 1);
    let result = loader
        .load_disk_group(
            1,
            1,
            1,
            (0, 1),
            &[(
                disk_id,
                DiskValue {
                    capacity_units: 128,
                    zone_size_units: 128,
                    unit_size_bytes: 1024 * 1024,
                    zone_count: 1,
                    status: HwStatus::Up as i32,
                    ..Default::default()
                },
            )],
            1,
        )
        .await;
    store.stop();
    let group = result.unwrap();
    group.set_status(HwStatus::Up);
    let disks = group.disk_snapshot();
    let zones = disks[0].zones.load();
    assert!(
        zones[0].usage_bits.is_set(7),
        "reclaimed journal must not erase a durable allocation"
    );
    assert_eq!(zones[0].usage_bits.count_set(), 1);
    for _ in 0..16 {
        let (_, _, allocated) = group.allocate_block(1, &[], 100, 1).unwrap();
        assert_ne!(
            allocated.unit_offset, 7,
            "live tree extent must never be reallocated"
        );
    }
}
