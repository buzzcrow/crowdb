// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/storage.rs"]
mod storage;
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::key::OwnerMapKey;
use storage::TestStorage;

#[tokio::test]
async fn current_boot_is_stable_and_replaced_boot_cannot_publish_or_reclaim() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let svc = ServiceRegistryClient::from_shared(hw.shared_kv());
    let retained = storage::record(5);
    hw.admit_disk_storage(&retained).await.unwrap();
    assert_eq!(svc.diskdb_session_epoch(7).await.unwrap(), 0);
    let first = svc.claim_diskdb_session(7, "first-boot", None, 0).await.unwrap();
    assert_eq!(
        svc.claim_diskdb_session(7, "first-boot", None, 0).await.unwrap(),
        first
    );
    hw.set_owner(1, 2, 3, 7, u64::MAX).await.unwrap();
    let key = OwnerMapKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
    };
    let old_generation = hw.claim_owner_process_epoch(&key, 7, first).await.unwrap();
    assert_eq!(
        hw.claim_owner_process_epoch(&key, 7, first).await.unwrap(),
        old_generation
    );
    svc.heartbeat_diskdb_session(7, "old:7100", &[3], &[], first)
        .await
        .unwrap();
    assert_eq!(svc.diskdb_session_epoch(7).await.unwrap(), first);
    let next = svc
        .claim_diskdb_session(7, "next-boot", Some("first-boot"), first)
        .await
        .unwrap();
    assert!(next > first);
    let generation = hw.claim_owner_process_epoch(&key, 7, next).await.unwrap();
    assert!(generation > old_generation);
    assert!(hw.claim_owner_process_epoch(&key, 7, first).await.is_err());
    assert!(svc.claim_diskdb_session(7, "first-boot", None, 0).await.is_err());
    assert!(svc
        .heartbeat_diskdb_session(7, "old:7100", &[3], &[], first)
        .await
        .is_err());
    svc.heartbeat_diskdb_session(7, "new:7100", &[3], &[], next)
        .await
        .unwrap();
    assert_eq!(svc.diskdb_session_epoch(7).await.unwrap(), next);
    assert_eq!(
        svc.read_instance("diskdb", 7)
            .await
            .unwrap()
            .unwrap()
            .rpc_endpoint,
        "new:7100"
    );
    hw.set_owner(1, 2, 3, 7, u64::MAX).await.unwrap();
    assert!(hw.claim_owner_process_epoch(&key, 7, first).await.is_err());
    assert!(hw.claim_owner_process_epoch(&key, 7, next).await.unwrap() > generation);
    assert_eq!(
        hw.disk_storage(&storage::key(&retained)).await.unwrap(),
        Some(retained)
    );
}

#[tokio::test]
async fn concurrent_boot_claims_share_one_fixed_predecessor() {
    let fixture = TestStorage::start().await;
    let svc = ServiceRegistryClient::from_shared(fixture.hardware.shared_kv());
    let (a, b) = tokio::join!(
        svc.claim_diskdb_session(7, "boot-a", None, 0),
        svc.claim_diskdb_session(7, "boot-b", None, 0)
    );
    assert_ne!(a.is_ok(), b.is_ok(), "a={a:?}, b={b:?}");
}

#[tokio::test]
async fn unsupported_process_session_never_becomes_a_fresh_boot() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let svc = ServiceRegistryClient::from_shared(hw.shared_kv());
    hw.kv()
        .put(
            0,
            0,
            b"/diskdb/session/7",
            br#"{"version":2,"instance_id":7,"boot_id":"foreign"}"#,
            None,
        )
        .await
        .unwrap();
    assert!(svc.diskdb_session_epoch(7).await.is_err());
    assert!(svc.claim_diskdb_session(7, "new", None, 0).await.is_err());
}

#[tokio::test]
async fn a_new_boot_can_supersede_a_crashed_unfinalized_startup_without_reviving_it() {
    let fixture = TestStorage::start().await;
    let hw = &fixture.hardware;
    let svc = ServiceRegistryClient::from_shared(hw.shared_kv());
    hw.kv()
        .put(
            0,
            0,
            b"/diskdb/session/7",
            br#"{"version":1,"instance_id":7,"boot_id":"crashed","epoch":0}"#,
            None,
        )
        .await
        .unwrap();
    let predecessor = svc.diskdb_session_predecessor(7).await.unwrap();
    assert_eq!(predecessor, (Some("crashed".into()), 0));
    let current = svc
        .claim_diskdb_session(7, "new", predecessor.0.as_deref(), predecessor.1)
        .await
        .unwrap();
    assert!(current > 0);
    assert!(svc.claim_diskdb_session(7, "crashed", None, 0).await.is_err());
}
