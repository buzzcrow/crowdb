// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/default_preparation.rs"]
mod common;

use common::TestPreparationAuthority;
use crowdb_console_shared::deployment::{
    preparation::{self, DEFAULT_PREPARATION_KEY},
    CLUSTER_KEY,
};
use crowdb_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::{
    common::GroupValue,
    key::{KvGroupKey, TextKey},
};

async fn snapshot(authority: &TestPreparationAuthority) -> Option<(Vec<u8>, u64)> {
    match authority
        .ctx
        .kv()
        .get(0, 0, DEFAULT_PREPARATION_KEY, ReadMode::Linearizable, None)
        .await
        .unwrap()
    {
        GetOutcome::Found { value, revision } => Some((value.to_vec(), revision)),
        GetOutcome::NotFound => None,
    }
}

#[tokio::test]
async fn simultaneous_runners_converge_on_one_fixed_mapping_and_credential_reference() {
    let authority = TestPreparationAuthority::start(&[2, 7, 9]).await;
    let attempts = futures::future::join_all(
        (0..8).map(|_| preparation::reserve(&authority.ctx, &authority.bootstrap.identity)),
    )
    .await;
    let winner = attempts[0].as_ref().unwrap();
    for attempt in &attempts {
        assert_eq!(attempt.as_ref().unwrap(), winner);
    }
    assert_eq!((winner.metadata.store_id, winner.metadata.group_id), (0, 1));
    assert_eq!(
        winner
            .metadata
            .replicas
            .iter()
            .map(|replica| (replica.node_id, replica.replica_id))
            .collect::<Vec<_>>(),
        vec![(2, 1), (7, 2), (9, 3)]
    );
    assert_eq!(winner.storage.len(), 3);
    for (index, binding) in winner.storage.iter().enumerate() {
        assert_eq!(
            (
                binding.rack_id,
                binding.disk_group_id,
                binding.store_id,
                binding.group_id
            ),
            (4, index as u64 + 1, 0, 1)
        );
    }
    assert!(winner.credential_ref.ends_with(&winner.operation_id));
    assert_eq!(winner.chunk_kv.node_id, 2);
    assert_eq!(winner.chunk_kv.metadata_group_id, winner.metadata.group_id);
    assert_eq!(winner.chunk_kv.owner_epoch, 1);
    assert_ne!(
        winner.chunk_kv.partition_id,
        crowdb_protocol::chunk_kv::Id128::default()
    );
    assert_ne!(
        winner.chunk_kv.stream_name,
        crowdb_protocol::chunk_stream::StreamName::default()
    );
    assert_ne!(winner.chunk_kv.tree_id, 0);
    let before = snapshot(&authority).await.unwrap();
    assert_eq!(
        preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
            .await
            .unwrap(),
        *winner
    );
    assert_eq!(snapshot(&authority).await.unwrap(), before);
    assert!(authority.ctx.sysmd().get_group(0, 1).await.unwrap().is_none());
    assert!(authority
        .ctx
        .sysmd()
        .get_disk_group(4, 2, 1)
        .await
        .unwrap()
        .is_none());
    for binding in &winner.storage {
        authority
            .ctx
            .sysmd()
            .reserve_disk_group_id(&crowdb_protocol::key::DiskGroupKey {
                rack_id: binding.rack_id,
                node_id: binding.node_id,
                disk_group_id: binding.disk_group_id,
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn global_disk_group_reservations_cannot_be_adopted_from_another_node_or_retired_location() {
    let authority = TestPreparationAuthority::start(&[2, 7, 9]).await;
    let foreign = crowdb_protocol::key::DiskGroupKey {
        rack_id: 8,
        node_id: 42,
        disk_group_id: 2,
    };
    authority
        .ctx
        .sysmd()
        .reserve_disk_group_id(&foreign)
        .await
        .unwrap();
    let error = preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("global owner"), "{error}");
    assert!(snapshot(&authority).await.is_none());
    assert!(authority
        .ctx
        .sysmd()
        .get_disk_group(4, 2, 1)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn singleton_uses_admitted_node_identity_and_read_does_not_reserve() {
    let authority = TestPreparationAuthority::start(&[42]).await;
    assert!(preparation::read(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap()
        .is_none());
    assert!(snapshot(&authority).await.is_none());
    let winner = preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap();
    assert_eq!(winner.metadata.replicas[0].node_id, 42);
    assert_eq!(winner.storage[0].node_id, 42);
    assert_eq!(winner.chunk_kv.node_id, 42);
    let before = snapshot(&authority).await.unwrap();
    let recovered = preparation::read(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.0, winner);
    assert_eq!(recovered.1, before.1);
    let other_runner = crowdb_console_shared::ops::OpContext::with_shared_client_preserving_hint(
        authority.ctx.kv_arc().clone(),
        &[],
        crowdb_console_shared::ConsoleConfig::default(),
    );
    assert_eq!(
        preparation::reserve(&other_runner, &authority.bootstrap.identity)
            .await
            .unwrap(),
        winner
    );
    assert_eq!(snapshot(&authority).await.unwrap(), before);
}

#[tokio::test]
async fn existing_membership_or_storage_binding_cannot_be_adopted_by_default_preparation() {
    use crowdb_protocol::{common::BindMapValue, key::BindMapKey, kv_membership::GroupMember};
    let authority = TestPreparationAuthority::start(&[2]).await;
    authority
        .ctx
        .membership()
        .create(
            0,
            1,
            vec![GroupMember {
                replica_id: 1,
                node_id: 2,
                endpoint: "node-2:10100".into(),
                voting: true,
            }],
        )
        .await
        .unwrap();
    assert!(
        preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
            .await
            .is_err()
    );
    assert!(snapshot(&authority).await.is_none());
    let other = TestPreparationAuthority::start(&[2]).await;
    let key = BindMapKey {
        rack_id: 4,
        node_id: 2,
        disk_group_id: 1,
    }
    .to_path();
    let bytes = serde_json::to_vec(&BindMapValue {
        store_id: 0,
        group_id: 8,
    })
    .unwrap();
    other
        .ctx
        .kv()
        .put(0, 0, key.as_bytes(), &bytes, None)
        .await
        .unwrap();
    assert!(preparation::reserve(&other.ctx, &other.bootstrap.identity)
        .await
        .is_err());
    assert!(snapshot(&other).await.is_none());
}

#[tokio::test]
async fn conflicting_or_missing_authority_never_allocates_default_identity() {
    let authority = TestPreparationAuthority::start(&[2]).await;
    let mut other = authority.bootstrap.identity.clone();
    other.cluster_id = uuid::Uuid::new_v4().to_string();
    assert!(preparation::reserve(&authority.ctx, &other).await.is_err());
    assert!(snapshot(&authority).await.is_none());
    authority.ctx.kv().delete(0, 0, CLUSTER_KEY, None).await.unwrap();
    assert!(
        preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
            .await
            .is_err()
    );
    assert!(snapshot(&authority).await.is_none());
}

#[tokio::test]
async fn preexisting_group_conflicts_and_unrelated_groups_are_never_selected() {
    let authority = TestPreparationAuthority::start(&[2]).await;
    let key = KvGroupKey {
        store_id: 0,
        group_id: 8,
    }
    .to_path();
    authority
        .ctx
        .kv()
        .put(
            0,
            0,
            key.as_bytes(),
            &serde_json::to_vec(&GroupValue {
                store_id: 0,
                group_id: 8,
            })
            .unwrap(),
            None,
        )
        .await
        .unwrap();
    let winner = preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap();
    assert_eq!(winner.metadata.group_id, 1);
    let other = TestPreparationAuthority::start(&[2]).await;
    let key = KvGroupKey {
        store_id: 0,
        group_id: 1,
    }
    .to_path();
    let bytes = serde_json::to_vec(&GroupValue {
        store_id: 0,
        group_id: 1,
    })
    .unwrap();
    other
        .ctx
        .kv()
        .put(0, 0, key.as_bytes(), &bytes, None)
        .await
        .unwrap();
    assert!(preparation::reserve(&other.ctx, &other.bootstrap.identity)
        .await
        .is_err());
    assert!(snapshot(&other).await.is_none());
}

#[tokio::test]
async fn corrupt_or_changed_fixed_inputs_are_preserved_and_never_replaced() {
    let authority = TestPreparationAuthority::start(&[2]).await;
    let winner = preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap();
    let mut changed = winner.clone();
    changed.metadata.replicas[0].node_id = 9;
    let mut wrong_version = winner.clone();
    wrong_version.version = 2;
    let mut changed_reference = winner.clone();
    changed_reference.credential_ref = "access/other".into();
    for bytes in [
        serde_json::to_vec(&changed).unwrap(),
        serde_json::to_vec(&wrong_version).unwrap(),
        serde_json::to_vec(&changed_reference).unwrap(),
        b"invalid".to_vec(),
    ] {
        authority
            .ctx
            .kv()
            .put(0, 0, DEFAULT_PREPARATION_KEY, &bytes, None)
            .await
            .unwrap();
        let before = snapshot(&authority).await.unwrap();
        assert!(
            preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
                .await
                .is_err()
        );
        assert_eq!(snapshot(&authority).await.unwrap(), before);
    }
}

#[tokio::test]
async fn explicit_retirement_is_idempotent_and_prevents_automatic_recreation() {
    let authority = TestPreparationAuthority::start(&[2]).await;
    let winner = preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap();
    let before = snapshot(&authority).await.unwrap();
    assert!(preparation::retire(
        &authority.ctx,
        &authority.bootstrap.identity,
        &uuid::Uuid::new_v4().to_string()
    )
    .await
    .is_err());
    assert_eq!(snapshot(&authority).await.unwrap(), before);
    preparation::retire(
        &authority.ctx,
        &authority.bootstrap.identity,
        &winner.operation_id,
    )
    .await
    .unwrap();
    let retired = snapshot(&authority).await.unwrap();
    assert!(
        preparation::read(&authority.ctx, &authority.bootstrap.identity)
            .await
            .unwrap()
            .unwrap()
            .0
            .retired
    );
    preparation::retire(
        &authority.ctx,
        &authority.bootstrap.identity,
        &winner.operation_id,
    )
    .await
    .unwrap();
    assert!(
        preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
            .await
            .is_err()
    );
    assert_eq!(snapshot(&authority).await.unwrap(), retired);
}

#[tokio::test]
async fn five_voter_preparation_retains_every_admitted_identity_and_fixed_mapping() {
    let members = [2, 7, 9, 12, 15];
    let authority = TestPreparationAuthority::start(&members).await;
    let winner = preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap();
    assert_eq!(
        winner
            .metadata
            .replicas
            .iter()
            .map(|r| (r.node_id, r.replica_id))
            .collect::<Vec<_>>(),
        vec![(2, 1), (7, 2), (9, 3), (12, 4), (15, 5)]
    );
    assert_eq!(
        winner
            .storage
            .iter()
            .map(|b| (b.node_id, b.disk_group_id, b.rack_id, b.store_id, b.group_id))
            .collect::<Vec<_>>(),
        vec![
            (2, 1, 4, 0, 1),
            (7, 2, 4, 0, 1),
            (9, 3, 4, 0, 1),
            (12, 4, 4, 0, 1),
            (15, 5, 4, 0, 1)
        ]
    );
    assert_eq!(winner.chunk_kv.node_id, 2);
    let before = snapshot(&authority).await.unwrap();
    let recovered = preparation::read(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.0, winner);
    assert_eq!(recovered.1, before.1);
    assert_eq!(
        preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
            .await
            .unwrap(),
        winner
    );
    assert_eq!(snapshot(&authority).await.unwrap(), before);
}
