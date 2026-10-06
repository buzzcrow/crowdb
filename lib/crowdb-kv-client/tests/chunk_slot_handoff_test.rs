// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/handoff_cluster.rs"]
mod handoff_cluster;

use crowdb_kv_client::{ChunkSlotMapClient, Error};
use crowdb_protocol::chunk_slot::{
    ChunkServiceHandoff, ChunkServiceHandoffPhase, ChunkSlot, ChunkSlotFenceReceipt,
};
use handoff_cluster::TestHandoffCluster;
use std::sync::Arc;

#[tokio::test]
async fn a_replacement_controller_resumes_partial_fences_without_replacing_the_cohort() {
    let test = TestHandoffCluster::start().await;
    let client = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let initial = TestHandoffCluster::plan(2);
    let before = client.read_service().await.unwrap();
    let observed = client.prepare_handoff(&initial).await.unwrap();
    let replay = client.prepare_handoff(&initial).await.unwrap();
    assert_eq!(observed.revision(), replay.revision());
    let mut progress = initial.clone();
    progress.begin_fencing().unwrap();
    let observed = client.advance_handoff(&observed, &progress).await.unwrap();
    let slot = ChunkSlot::try_from(0).unwrap();
    let mut forged = progress.clone();
    forged
        .record_fence(ChunkSlotFenceReceipt { slot, revision: 10 })
        .unwrap();
    assert!(client.advance_handoff(&observed, &forged).await.is_err());
    let receipt = client.fence_handoff_slot(&observed, slot).await.unwrap();
    // A lost data-group reply is reconciled without rewriting the fence.
    assert_eq!(client.fence_handoff_slot(&observed, slot).await.unwrap(), receipt);
    progress.record_fence(receipt).unwrap();
    client.advance_handoff(&observed, &progress).await.unwrap();
    // Discard the local result, as with a lost reply or a controller crash.
    let replacement = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let resumed = replacement.read_handoff().await.unwrap().unwrap();
    assert_eq!(resumed.plan(), &progress);
    assert_eq!(
        replacement.prepare_handoff(&initial).await.unwrap().revision(),
        resumed.revision()
    );
    assert!(replacement
        .prepare_handoff(&TestHandoffCluster::plan(3))
        .await
        .is_err());
    assert!(replacement.advance_handoff(&resumed, &initial).await.is_err());
    let receipt = replacement
        .fence_handoff_slot(&resumed, ChunkSlot::try_from(1).unwrap())
        .await
        .unwrap();
    progress.record_fence(receipt).unwrap();
    let resumed = replacement.advance_handoff(&resumed, &progress).await.unwrap();
    progress.record_publication(2).unwrap();
    // Publish cannot be persisted separately from the complete map transaction.
    assert!(replacement.advance_handoff(&resumed, &progress).await.is_err());
    assert_eq!(
        replacement.read_service().await.unwrap().bindings(),
        before.bindings()
    );
    assert_eq!(
        replacement
            .read_handoff()
            .await
            .unwrap()
            .unwrap()
            .plan()
            .record()
            .phase,
        ChunkServiceHandoffPhase::Fence
    );
}

#[tokio::test]
async fn competing_cohorts_use_the_service_head_revision_and_reject_stale_maps() {
    let test = TestHandoffCluster::start().await;
    let first = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let second = ChunkSlotMapClient::new(Arc::clone(&test.kv));
    let a = TestHandoffCluster::plan(2);
    let b = TestHandoffCluster::plan(3);
    let (a_result, b_result) = tokio::join!(first.prepare_handoff(&a), second.prepare_handoff(&b));
    assert_ne!(a_result.is_ok(), b_result.is_ok());
    let winner = first.read_handoff().await.unwrap().unwrap();
    assert!(winner.plan() == &a || winner.plan() == &b);
    let mut stale = winner.plan().record().clone();
    stale.base_service_generation = 2;
    let stale = ChunkServiceHandoff::try_from(stale).unwrap();
    assert!(first.prepare_handoff(&stale).await.is_err());
    let mut progressed = winner.plan().clone();
    progressed.begin_fencing().unwrap();
    let fenced = first.advance_handoff(&winner, &progressed).await.unwrap();
    // A stale writer cannot remove another controller's persisted receipt.
    let receipt = first
        .fence_handoff_slot(&fenced, ChunkSlot::try_from(0).unwrap())
        .await
        .unwrap();
    progressed.record_fence(receipt).unwrap();
    first.advance_handoff(&fenced, &progressed).await.unwrap();
    let mut conflicting = fenced.plan().clone();
    let other = first
        .fence_handoff_slot(&fenced, ChunkSlot::try_from(1).unwrap())
        .await
        .unwrap();
    conflicting.record_fence(other).unwrap();
    assert!(matches!(
        second.advance_handoff(&fenced, &conflicting).await,
        Err(Error::CasFailed { .. })
    ));
}
