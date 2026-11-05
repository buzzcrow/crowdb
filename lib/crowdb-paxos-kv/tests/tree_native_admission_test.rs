// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_paxos_kv::kv::{ApplyAdmissionError, Batch, CrowdbTreeConfig, CrowdbTreeEngine, KVEngine};
use crowdb_tree_ffi::{TreeRuntime, TreeRuntimeOptions};

fn engine() -> CrowdbTreeEngine {
    CrowdbTreeEngine::open(&CrowdbTreeConfig {
        runtime: Some(Arc::new(
            TreeRuntime::new(TreeRuntimeOptions {
                workers: 1,
                ..Default::default()
            })
            .unwrap(),
        )),
        ..Default::default()
    })
    .unwrap()
}

fn put(value: &[u8]) -> Bytes {
    let mut payload = vec![1, 0, 0];
    payload.extend_from_slice(&3u32.to_le_bytes());
    payload.extend_from_slice(b"key");
    payload.extend_from_slice(&u32::try_from(value.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(value);
    Bytes::from(payload)
}

#[tokio::test(flavor = "current_thread")]
async fn native_permit_owns_the_payload_and_applies_on_the_shared_runtime() {
    let engine = engine();
    let payload = put(b"owned value");
    let permit = engine.admit_proposal(payload.clone()).await.unwrap().unwrap();
    drop(payload);
    permit.apply(1).await.unwrap();
    assert_eq!(engine.get(b"key").await, Some((1, b"owned value".to_vec())));
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_native_admission_releases_the_pinned_generation() {
    let engine = engine();
    let permit = engine.admit_proposal(put(b"cancelled")).await.unwrap().unwrap();
    drop(permit);
    engine.handle().clear().unwrap();
    assert!(engine.get(b"key").await.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn clear_returns_busy_without_panicking_or_revoking_an_armed_proposal() {
    let engine = engine();
    let permit = engine.admit_proposal(put(b"accepted")).await.unwrap().unwrap();
    assert!(engine.clear().is_err());
    permit.apply(1).await.unwrap();
    assert_eq!(engine.get(b"key").await, Some((1, b"accepted".to_vec())));
    engine.clear().unwrap();
    assert!(engine.get(b"key").await.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_native_proposal_is_rejected_before_it_has_a_permit() {
    let engine = engine();
    assert!(matches!(
        engine.admit_proposal(Bytes::from_static(&[1, 0])).await,
        Err(ApplyAdmissionError::Rejected(_))
    ));
    assert_eq!(engine.stats().contiguous_slot, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn native_empty_proposal_still_completes_its_real_slot() {
    let engine = engine();
    let permit = engine.admit_proposal(Bytes::new()).await.unwrap().unwrap();
    permit.apply(1).await.unwrap();
    assert_eq!(engine.stats().contiguous_slot, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn complete_gap_bound_rejects_an_oversized_proposal_before_consensus() {
    let engine = engine();
    let permit = engine.admit_proposal(put(&vec![0; 8 * 1024 * 1024])).await;
    assert!(matches!(permit, Err(ApplyAdmissionError::Rejected(_))));
}

#[tokio::test(flavor = "current_thread")]
async fn default_engine_returns_a_real_permit_and_applies_its_slot() {
    let engine = CrowdbTreeEngine::open(&CrowdbTreeConfig::default()).unwrap();
    assert!(engine.requires_contiguous_apply());
    let permit = engine.admit_proposal(put(b"default")).await.unwrap().unwrap();
    assert_eq!(engine.stats().contiguous_slot, 0);
    assert_eq!(engine.handle().durable_coverage(), 0);
    assert!(engine.get(b"key").await.is_none());
    permit.apply(1).await.unwrap();
    assert_eq!(engine.stats().contiguous_slot, 1);
    assert_eq!(engine.handle().durable_coverage(), 0);
    assert_eq!(engine.get(b"key").await, Some((1, b"default".to_vec())));
}

#[tokio::test(flavor = "current_thread")]
async fn occupied_native_writer_credit_is_busy_and_cancellation_returns_it() {
    let mut options = CrowdbTreeConfig {
        runtime: Some(Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap())),
        ..Default::default()
    };
    options.runtime_binding.limits.writer_bytes = 1024 * 1024;
    let engine = CrowdbTreeEngine::open(&options).unwrap();
    let mut retained = Vec::new();
    let mut busy = false;
    for _ in 0..16 {
        match engine.admit_proposal(put(b"reserved")).await {
            Ok(Some(permit)) => retained.push(permit),
            Err(ApplyAdmissionError::Busy) => {
                busy = true;
                break;
            }
            other => panic!(
                "unexpected native admission: {}",
                match other {
                    Err(ApplyAdmissionError::Rejected(error)) => error,
                    _ => "missing permit".to_owned(),
                }
            ),
        }
    }
    assert!(busy);
    assert!(!retained.is_empty());
    drop(retained);
    engine.handle().clear().unwrap();
    let permit = engine.admit_proposal(put(b"returned")).await.unwrap().unwrap();
    permit.apply(1).await.unwrap();
    assert_eq!(engine.get(b"key").await, Some((1, b"returned".to_vec())));
}

#[tokio::test(flavor = "current_thread")]
async fn chosen_follower_apply_waits_for_empty_gap_without_blocking_the_host() {
    use std::future::{poll_fn, Future};
    use std::task::Poll;
    let engine = engine();
    let batch = Batch::try_decode(&put(b"follower")).unwrap();
    let mut higher = Box::pin(engine.apply(2, &batch));
    poll_fn(|context| {
        assert!(higher.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    engine.apply_noop(1).await.unwrap();
    higher.await.unwrap();
    assert_eq!(engine.get(b"key").await, Some((2, b"follower".to_vec())));
    assert_eq!(engine.stats().contiguous_slot, 2);
}
