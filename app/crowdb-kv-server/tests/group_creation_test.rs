// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/group_creation.rs"]
mod common;

use common::{request, TestGroupCreation};
use crowdb_kv::cluster::{group::PxGroup, PxLocalReplica, PxLocalReplicaRole};
use reqwest::{Method, StatusCode};
use serde_json::Value;

#[tokio::test]
async fn fixed_creation_identity_is_private_and_conflicting_retries_do_not_overwrite_it() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = TestGroupCreation::start(false).await;
    let body = request();
    fixture.create(&body, StatusCode::CREATED).await;
    let before = std::fs::read(fixture.ownership()).unwrap();
    assert_eq!(
        std::fs::metadata(fixture.ownership())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let (status, bytes) = fixture
        .request(Method::GET, "/stores/0/groups/1/creation", None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let accepted: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(accepted["deployment"], body["deployment"]);
    assert_eq!(accepted["retired"], false);
    fixture.create(&body, StatusCode::CONFLICT).await;
    let mut changed_replica = body.clone();
    changed_replica["replica_id"] = 8.into();
    let mut changed_cluster = body.clone();
    changed_cluster["deployment"]["cluster_id"] = uuid::Uuid::new_v4().to_string().into();
    for changed in [request(), changed_replica, changed_cluster] {
        fixture.create(&changed, StatusCode::CONFLICT).await;
        assert_eq!(std::fs::read(fixture.ownership()).unwrap(), before);
    }
    let mut generic = body.clone();
    generic.as_object_mut().unwrap().remove("deployment");
    fixture.create(&generic, StatusCode::CONFLICT).await;
    assert_eq!(fixture.store.get_group(1).unwrap().local_replica().id, 7);
}

#[tokio::test]
async fn an_existing_unowned_replica_is_not_adopted_even_when_its_numeric_id_matches() {
    let fixture = TestGroupCreation::start(false).await;
    fixture.store.add_group_without_election(PxGroup::new(
        1,
        PxLocalReplica::new(7, PxLocalReplicaRole::Leader),
    ));
    fixture.create(&request(), StatusCode::CONFLICT).await;
    let mut generic = request();
    generic.as_object_mut().unwrap().remove("deployment");
    generic["replica_id"] = 99.into();
    fixture.create(&generic, StatusCode::CONFLICT).await;
    assert!(!fixture.ownership().exists());
    assert!(!fixture.registry.config.wal_root.exists());
}

#[tokio::test]
async fn failure_after_acceptance_resumes_the_same_operation_without_allocating_another_owner() {
    let fixture = TestGroupCreation::start(true).await;
    let body = request();
    fixture.create(&body, StatusCode::INTERNAL_SERVER_ERROR).await;
    let before = std::fs::read(fixture.ownership()).unwrap();
    fixture.create(&request(), StatusCode::CONFLICT).await;
    assert_eq!(std::fs::read(fixture.ownership()).unwrap(), before);
    std::fs::remove_file(&fixture.registry.config.data_root).unwrap();
    std::fs::create_dir(&fixture.registry.config.data_root).unwrap();
    fixture.create(&body, StatusCode::CREATED).await;
    assert_eq!(std::fs::read(fixture.ownership()).unwrap(), before);
}

#[tokio::test]
async fn explicit_removal_keeps_the_deployment_tombstone_and_restore_cannot_recreate_it() {
    let fixture = TestGroupCreation::start(false).await;
    let body = request();
    fixture.create(&body, StatusCode::CREATED).await;
    let (status, bytes) = fixture.request(Method::DELETE, "/stores/0/groups/1", None).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&bytes));
    let tombstone = std::fs::read(fixture.ownership()).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&tombstone).unwrap()["retired"],
        true
    );
    fixture.create(&body, StatusCode::CONFLICT).await;
    let restored = crowdb_kv_server::recovery::startup::create_group_with_wal(
        0,
        1,
        7,
        PxLocalReplicaRole::Follower,
        &fixture.registry.config,
        fixture.registry.wal_backend.clone(),
        fixture.registry.crowtree_backend,
    )
    .await;
    assert!(matches!(restored, Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied));
    assert_eq!(std::fs::read(fixture.ownership()).unwrap(), tombstone);
    assert!(fixture.store.get_group(1).is_none());
}

#[tokio::test]
async fn corrupt_owner_cannot_be_replaced_or_open_group_storage() {
    let fixture = TestGroupCreation::start(false).await;
    std::fs::create_dir_all(&fixture.registry.config.config_root).unwrap();
    std::fs::write(fixture.ownership(), b"corrupt").unwrap();
    fixture.create(&request(), StatusCode::CONFLICT).await;
    assert_eq!(std::fs::read(fixture.ownership()).unwrap(), b"corrupt");
    assert!(!fixture.registry.config.wal_root.exists());
    assert!(!fixture.registry.config.data_root.exists());
}
