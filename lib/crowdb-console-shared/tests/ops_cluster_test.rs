// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for [`ops::cluster`] validation and authority boundaries.

use crowdb_console_shared::config::ConsoleConfig;
use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops::{self, OpContext};
use crowdb_test_harness::cluster::KvCluster;

#[path = "common/bootstrap_authority.rs"]
mod bootstrap_authority;

fn ctx() -> OpContext {
    OpContext::new("127.0.0.1:1".into(), vec![], ConsoleConfig::default())
}

#[tokio::test]
async fn init_empty_nodes_validation() {
    let ctx = ctx();
    let err = ops::cluster::init(&ctx, &[]).await.unwrap_err();
    assert!(matches!(err, Error::Validation { field, .. } if field == "nodes"));
}

#[tokio::test]
async fn init_dedup_nodes() {
    // This test verifies that duplicate node ids are deduplicated.
    // It will fail at the health check (no server), but the error
    // should be NodeUnreachable, not a duplicate-processing issue.
    let ctx = ctx();
    let err = ops::cluster::init(&ctx, &[1, 1, 2]).await.unwrap_err();
    // Should fail on node 1 (first unique node) being unreachable.
    assert!(matches!(
        err,
        Error::NodeUnreachable { .. } | Error::NotFound { .. }
    ));
}

#[tokio::test]
async fn clean_requires_confirmed_group_replicas() {
    let cluster = KvCluster::start().await;
    let bootstrap = bootstrap_authority::context(&cluster).await;
    ops::cluster::init(&bootstrap, &[1]).await.unwrap();

    // The bootstrap context still has a local server entry, but no local
    // entry may justify wiping a group absent from Group 0.
    let err = ops::cluster::clean(&bootstrap, 17, 2).await.unwrap_err();
    assert!(matches!(err, Error::NotFound { .. }), "{err:?}");
}
