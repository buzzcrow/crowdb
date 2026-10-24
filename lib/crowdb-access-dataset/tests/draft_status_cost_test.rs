// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::DraftProgress, *};
use serde_json::json;
use std::sync::atomic::Ordering;

fn key(prefix: &[u8], identity: &DatasetIdentity, id: OperationId) -> Vec<u8> {
    [prefix, identity.key().unwrap().as_slice(), id.as_bytes()].concat()
}
async fn legacy_state(
    store: &dyn DatasetStore,
    key: &[u8],
    composition: bool,
    attempt: &CommitAttempt,
) -> Vec<u8> {
    let stored = store.get(key).await.unwrap().unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
    let record = if composition {
        &mut value["draft"]["record"]
    } else {
        &mut value["record"]
    };
    record["progress"] = serde_json::to_value(DraftProgress::Committing {
        target: attempt.target,
        attempt: attempt.id,
    })
    .unwrap();
    let digest = [0_u8; 32];
    value["edit"] = json!({"id": OperationId::random(), "digest": digest});
    let bytes = serde_json::to_vec(&value).unwrap();
    assert!(matches!(
        store
            .compare_exchange(key, Some(&stored.bytes), &bytes)
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    bytes
}
async fn legacy_receipt(store: &dyn DatasetStore, identity: &DatasetIdentity, attempt: &CommitAttempt) {
    let receipt = SuccessfulCommit {
        version: 1,
        attempt: attempt.clone(),
        committed_head: CommitGeneration::from_head(b"legacy successful head"),
    };
    for (prefix, bytes) in [
        (b"DS\0CA\0".as_slice(), attempt.encode().unwrap()),
        (b"DS\0CR\0", receipt.encode().unwrap()),
    ] {
        store
            .compare_exchange(&key(prefix, identity, attempt.id), None, &bytes)
            .await
            .unwrap();
    }
}
fn costs(store: &split_store::TestDraftStore) -> (usize, usize, usize) {
    (
        store.bounded_reads.load(Ordering::Relaxed),
        store.unbounded_reads.load(Ordering::Relaxed),
        store.mutations.load(Ordering::Relaxed),
    )
}

#[tokio::test]
async fn legacy_composition_status_never_helps_edits_or_reconstructs_publication() {
    for has_receipt in [false, true] {
        let (store, authority, identity, parent) = split_store::fixture().await;
        let operation = OperationId::random();
        common::begin_evaluated(&authority, &identity, operation, &common::input(parent)).await;
        let prepared = common::prepare(&authority, &identity, operation, 1).await;
        let attempt = common::attempt(&authority, &identity, prepared.preparation.unwrap().candidate).await;
        let key = key(b"DS\0CD\0", &identity, operation);
        let stored = legacy_state(store.as_ref(), &key, true, &attempt).await;
        if has_receipt {
            legacy_receipt(store.as_ref(), &identity, &attempt).await;
        }
        store.inspect_only.store(true, Ordering::Relaxed);
        store.reject_receipts.store(true, Ordering::Relaxed);
        let before = costs(&store);
        let restarted = DatasetAuthority::new(store.clone());
        let status = restarted.composition_status(&identity, operation).await.unwrap();
        assert_eq!(
            status.record.progress,
            DraftProgress::Prepared {
                target: attempt.target
            }
        );
        assert_eq!(costs(&store), (before.0 + 1, before.1, before.2));
        store.inspect_only.store(false, Ordering::Relaxed);
        assert_eq!(store.get(&key).await.unwrap().unwrap().bytes, stored);
    }
}

#[tokio::test]
async fn legacy_split_status_observes_one_draft_without_receipt_head_or_recovery_writes() {
    for has_receipt in [false, true] {
        let (store, authority, identity, snapshot) = split_store::fixture().await;
        let prepared = split_store::prepared(&authority, &identity, snapshot, None).await;
        let attempt = split_store::attempt(&authority, &identity, &prepared).await;
        let key = key(b"DS\0SD\0", &identity, prepared.operation);
        let stored = legacy_state(store.as_ref(), &key, false, &attempt).await;
        if has_receipt {
            legacy_receipt(store.as_ref(), &identity, &attempt).await;
        }
        store.inspect_only.store(true, Ordering::Relaxed);
        store.reject_split_proof_reads.store(true, Ordering::Relaxed);
        store.reject_receipts.store(true, Ordering::Relaxed);
        let before = costs(&store);
        let restarted = DatasetAuthority::new(store.clone());
        let status = restarted
            .split_draft_status(&identity, prepared.operation)
            .await
            .unwrap();
        assert_eq!(
            status.progress,
            DraftProgress::Prepared {
                target: attempt.target
            }
        );
        assert_eq!(costs(&store), (before.0 + 1, before.1, before.2));
        store.inspect_only.store(false, Ordering::Relaxed);
        assert_eq!(store.get(&key).await.unwrap().unwrap().bytes, stored);
    }
}
