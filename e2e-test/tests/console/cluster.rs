// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for [`ops::cluster`] validation and authority boundaries.

use super::fixture::TestKvCluster as KvCluster;
use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops;

use super::bootstrap_authority;

#[tokio::test]
#[ignore = "requires the owned release KV containers"]
async fn clean_requires_confirmed_group_replicas() {
    let cluster = KvCluster::start().await;
    let bootstrap = bootstrap_authority::context(&cluster).await;
    ops::cluster::init(&bootstrap, &[1]).await.unwrap();

    // The bootstrap context still has a local server entry, but no local
    // entry may justify wiping a group absent from Group 0.
    let err = ops::cluster::clean(&bootstrap, 17, 2).await.unwrap_err();
    assert!(matches!(err, Error::NotFound { .. }), "{err:?}");
}
