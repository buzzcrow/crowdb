// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/bootstrap_node.rs"]
mod bootstrap_node;

use bootstrap_node::{context, TestNode};
use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops::cluster;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn failed_bootstrap_retry_never_deletes_an_existing_system_group() {
    let first = TestNode::start(Some(1), false).await;
    let second = TestNode::start(None, true).await;
    let ctx = context(&first, &second);
    assert!(cluster::init(&ctx, &[1, 2]).await.is_err());
    assert!(
        !first.deleted.load(Ordering::SeqCst),
        "retry deleted an existing Group 0"
    );
    assert!(ctx.config().stores.is_empty());
}

#[tokio::test]
async fn interrupted_bootstrap_preserves_new_group_for_identity_checked_resume() {
    let first = TestNode::start(None, false).await;
    let second = TestNode::start(None, true).await;
    let ctx = context(&first, &second);
    assert!(cluster::init(&ctx, &[1, 2]).await.is_err());
    assert!(!first.deleted.load(Ordering::SeqCst));
}

#[tokio::test]
async fn retry_rejects_an_existing_system_group_with_a_different_replica_identity() {
    let first = TestNode::start(Some(99), false).await;
    let second = TestNode::start(None, true).await;
    let error = cluster::init(&context(&first, &second), &[1, 2])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Conflict { .. }), "{error}");
    assert!(!first.deleted.load(Ordering::SeqCst));
}

#[tokio::test]
async fn incomplete_bootstrap_wiring_never_publishes_membership() {
    for (omit_endpoint, reject_wiring) in [(true, false), (false, true)] {
        let first = TestNode::start(None, false).await;
        let second = TestNode::with_wiring(None, false, omit_endpoint, reject_wiring).await;
        let ctx = context(&first, &second);
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(2), cluster::init(&ctx, &[1, 2])).await;
        assert!(result
            .expect("wiring failure must stop initialization before waiting for authority")
            .is_err());
        assert!(ctx.config().stores.is_empty());
        assert!(ctx.config().groups.is_empty());
        assert!(!first.deleted.load(Ordering::SeqCst));
    }
}
