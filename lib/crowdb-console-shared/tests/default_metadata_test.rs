// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/default_metadata.rs"]
mod default_metadata;
#[path = "common/default_preparation.rs"]
mod default_preparation;

use crowdb_console_shared::deployment::preparation;
use crowdb_kv::cluster::{group::PxGroup, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv_client::{GetOutcome, ReadMode};
use default_metadata::TestDefaultMetadata;

async fn ready(fixture: &TestDefaultMetadata) {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while !fixture.ensure().await.unwrap() {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("accepted metadata group did not become readable");
}

#[tokio::test]
async fn singleton_default_group_uses_owned_native_creation_and_replay_preserves_user_data() {
    let fixture = TestDefaultMetadata::start(&[42]).await;
    ready(&fixture).await;
    let ctx = &fixture.authority.ctx;
    ctx.kv().put(0, 1, b"retained", b"value", None).await.unwrap();
    let before = ctx.membership().read(0, 1).await.unwrap().unwrap();
    let operation = preparation::read(ctx, &fixture.accepted.bootstrap)
        .await
        .unwrap()
        .unwrap();
    assert!(fixture.ensure().await.unwrap());
    assert_eq!(
        ctx.membership().read(0, 1).await.unwrap().unwrap().record(),
        before.record()
    );
    assert_eq!(
        preparation::read(ctx, &fixture.accepted.bootstrap)
            .await
            .unwrap()
            .unwrap(),
        operation
    );
    assert!(
        matches!(ctx.kv().get(0, 1, b"retained", ReadMode::Linearizable, None).await.unwrap(),
        GetOutcome::Found { value, .. } if value.as_ref() == b"value")
    );
}

#[tokio::test]
async fn three_owned_native_replicas_publish_native_listeners_and_allow_linearizable_reads() {
    let fixture = TestDefaultMetadata::start(&[2, 7, 9]).await;
    ready(&fixture).await;
    let membership = fixture
        .authority
        .ctx
        .membership()
        .read(0, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(membership.record().members.len(), 3);
    for ((member, store), management) in membership
        .record()
        .members
        .iter()
        .zip(&fixture.stores)
        .zip(&fixture.endpoints)
    {
        use crowdb_kv::cluster::KvServer;
        assert_eq!(member.endpoint, store.listen_addr().unwrap().to_string());
        assert_ne!(format!("http://{}", member.endpoint), *management);
    }
    fixture
        .authority
        .ctx
        .kv()
        .put(0, 1, b"three", b"preserved", None)
        .await
        .unwrap();
    ready(&fixture).await;
    assert!(
        matches!(fixture.authority.ctx.kv().get(0, 1, b"three", ReadMode::Linearizable, None).await.unwrap(),
        GetOutcome::Found { value, .. } if value.as_ref() == b"preserved")
    );
}

#[tokio::test]
async fn a_matching_unowned_native_replica_cannot_publish_default_membership() {
    let fixture = TestDefaultMetadata::start(&[2]).await;
    fixture.stores[0].add_group_without_election(PxGroup::new(
        1,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    assert!(fixture.ensure().await.is_err());
    assert!(fixture
        .authority
        .ctx
        .sysmd()
        .get_group(0, 1)
        .await
        .unwrap()
        .is_none());
    assert!(fixture
        .authority
        .ctx
        .membership()
        .read(0, 1)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn retired_preparation_cannot_create_native_replicas_or_publish_metadata() {
    let fixture = TestDefaultMetadata::start(&[2]).await;
    preparation::retire(
        &fixture.authority.ctx,
        &fixture.accepted.bootstrap,
        &fixture.accepted.operation_id,
    )
    .await
    .unwrap();
    assert!(fixture.ensure().await.is_err());
    assert!(fixture.stores[0].get_group(1).is_none());
    assert!(fixture
        .authority
        .ctx
        .sysmd()
        .get_group(0, 1)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn operator_removal_retires_the_shared_operation_before_deleting_native_replicas() {
    let fixture = TestDefaultMetadata::start(&[2]).await;
    assert!(
        crowdb_console_shared::ops::kv_logical::add_group(&fixture.authority.ctx, 0, 1, 1, &[2])
            .await
            .is_err()
    );
    assert!(fixture.stores[0].get_group(1).is_none());
    ready(&fixture).await;
    crowdb_console_shared::ops::kv_logical::remove_group(&fixture.authority.ctx, 0, 1)
        .await
        .unwrap();
    let retired = preparation::read(&fixture.authority.ctx, &fixture.accepted.bootstrap)
        .await
        .unwrap()
        .unwrap()
        .0;
    assert!(retired.retired);
    assert_eq!(retired.operation_id, fixture.accepted.operation_id);
    assert!(fixture.ensure().await.is_err());
    assert!(fixture.stores[0].get_group(1).is_none());
    assert!(fixture
        .authority
        .ctx
        .membership()
        .read(0, 1)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn an_established_missing_replica_requires_data_recovery_without_creating_a_replacement() {
    let fixture = TestDefaultMetadata::start(&[2]).await;
    ready(&fixture).await;
    let before = fixture
        .authority
        .ctx
        .membership()
        .read(0, 1)
        .await
        .unwrap()
        .unwrap();
    assert!(fixture.stores[0].remove_group(1));
    assert!(fixture.ensure().await.is_err());
    assert!(fixture.stores[0].get_group(1).is_none());
    assert_eq!(
        fixture
            .authority
            .ctx
            .membership()
            .read(0, 1)
            .await
            .unwrap()
            .unwrap()
            .record(),
        before.record()
    );
}

#[tokio::test]
async fn a_later_operator_membership_change_is_never_replaced_by_initial_defaults() {
    let fixture = TestDefaultMetadata::start(&[2]).await;
    ready(&fixture).await;
    let membership = fixture.authority.ctx.membership();
    let initial = membership.read(0, 1).await.unwrap().unwrap();
    let changed = membership
        .begin_change(0, 1, 1, initial.record().members.clone())
        .await
        .unwrap();
    assert!(fixture.ensure().await.is_err());
    assert_eq!(
        membership.read(0, 1).await.unwrap().unwrap().record(),
        changed.record()
    );
    assert!(fixture.stores[0].get_group(1).is_some());
}
