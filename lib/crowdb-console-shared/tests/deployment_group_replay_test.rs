// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/deployment_group_replay.rs"]
mod common;

use common::{owner, TestDeploymentReplay};

#[tokio::test]
async fn a_lost_creation_reply_is_read_back_without_reissuing_the_mutation() {
    let accepted = owner();
    let deployment = accepted.deployment.clone().unwrap();
    let fixture = TestDeploymentReplay::start(Some(accepted), true).await;
    assert!(!fixture.replay(deployment.clone()).await.unwrap());
    assert_eq!(fixture.posts(), 1);
    assert!(!fixture.replay(deployment).await.unwrap());
    assert_eq!(fixture.posts(), 1);
}

#[tokio::test]
async fn matching_replica_numbers_never_adopt_unknown_foreign_or_retired_creation() {
    let expected = owner();
    let deployment = expected.deployment.clone().unwrap();
    let mut retired = expected.clone();
    retired.retired = true;
    let mut wrong_replica = expected.clone();
    wrong_replica.replica_id = 8;
    for accepted in [None, Some(owner()), Some(retired), Some(wrong_replica)] {
        let fixture = TestDeploymentReplay::start(accepted, false).await;
        assert!(fixture.replay(deployment.clone()).await.is_err());
        assert_eq!(fixture.posts(), 0);
    }
}

#[tokio::test]
async fn a_foreign_owner_after_an_unknown_create_outcome_remains_a_conflict() {
    let deployment = owner().deployment.unwrap();
    let fixture = TestDeploymentReplay::start(Some(owner()), true).await;
    assert!(fixture.replay(deployment).await.is_err());
    assert_eq!(fixture.posts(), 1);
}
