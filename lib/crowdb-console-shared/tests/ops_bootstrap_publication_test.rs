// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops::cluster;
use crowdb_protocol::common::{HwStatus, RackValue};
use crowdb_protocol::TextKey;
use crowdb_test_harness::cluster::KvCluster;
#[path = "common/bootstrap_authority.rs"]
mod bootstrap_authority;
use bootstrap_authority::context;

#[tokio::test]
async fn bootstrap_rejects_conflicting_hardware_without_overwriting_authority() {
    let cluster = KvCluster::start().await;
    let ctx = context(&cluster).await;
    let existing = RackValue {
        status: HwStatus::Up as i32,
        node_ids: vec![99],
    };
    ctx.sysmd().add_rack(1, &existing).await.unwrap();
    let result = cluster::init(&ctx, &[1]).await;
    assert!(matches!(result, Err(Error::Conflict { .. })), "{result:?}");
    assert_eq!(ctx.sysmd().get_rack(1).await.unwrap(), Some(existing));
    assert!(ctx.sysmd().get_store(0).await.unwrap().is_none());
    assert!(ctx.config().stores.is_empty());
}

#[tokio::test]
async fn bootstrap_preflights_logical_conflicts_before_publishing_missing_hardware() {
    let cluster = KvCluster::start().await;
    let ctx = context(&cluster).await;
    ctx.sysmd().add_store(0, &[99]).await.unwrap();
    let result = cluster::init(&ctx, &[1]).await;
    assert!(matches!(result, Err(Error::Conflict { .. })), "{result:?}");
    assert_eq!(
        ctx.sysmd().get_store(0).await.unwrap().unwrap().node_ids,
        vec![99]
    );
    assert!(ctx.sysmd().get_rack(1).await.unwrap().is_none());
}

#[tokio::test]
async fn bootstrap_resumes_missing_records_and_preserves_committed_revisions() {
    let cluster = KvCluster::start().await;
    let ctx = context(&cluster).await;
    ctx.sysmd()
        .add_rack(
            1,
            &RackValue {
                status: HwStatus::Up as i32,
                node_ids: Vec::new(),
            },
        )
        .await
        .unwrap();
    let key = crowdb_protocol::key::RackKey { rack_id: 1 }.to_path();
    let before = ctx
        .kv()
        .get(
            0,
            0,
            key.as_bytes(),
            crowdb_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .unwrap();
    cluster::init(&ctx, &[1]).await.unwrap();
    let after = ctx
        .kv()
        .get(
            0,
            0,
            key.as_bytes(),
            crowdb_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .unwrap();
    let (
        crowdb_kv_client::GetOutcome::Found { revision: before, .. },
        crowdb_kv_client::GetOutcome::Found { revision: after, .. },
    ) = (before, after)
    else {
        panic!("rack must remain present")
    };
    assert_eq!(before, after, "matching committed content must not be rewritten");
    assert_eq!(ctx.sysmd().get_store(0).await.unwrap().unwrap().node_ids, vec![1]);
    assert_eq!(ctx.sysmd().list_replicas_in_group(0, 0).await.unwrap().len(), 1);
}
