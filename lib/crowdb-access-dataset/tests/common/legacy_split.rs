// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(dead_code)]

use crowdb_access_dataset::{composition::DraftProgress, split::*, *};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub fn revision(operation: OperationId, input_revision: u64) -> SplitRevisionId {
    let mut hash = Sha256::new();
    hash.update(b"crowdb-split-candidate-v1\0");
    hash.update(operation.as_bytes());
    hash.update(input_revision.to_be_bytes());
    SplitRevisionId::from_bytes(&hash.finalize()[..16]).unwrap()
}

pub fn state_key(prefix: &[u8], identity: &DatasetIdentity, operation: OperationId) -> Vec<u8> {
    [prefix, &identity.key().unwrap(), operation.as_bytes()].concat()
}

pub async fn state(store: &dyn DatasetStore, identity: &DatasetIdentity, record: &SplitDraftRecord) {
    record.validate().unwrap();
    let sources: BTreeSet<_> = record
        .splits
        .iter()
        .filter_map(|split| match &split.origin {
            MembershipOrigin::Inherited(source) => Some(source.snapshot),
            MembershipOrigin::Evaluated => None,
        })
        .collect();
    replace(
        store,
        &state_key(b"DS\0SD\0", identity, record.operation),
        &serde_json::to_vec(&json!({
            "record": record, "work_version": 1, "initial": false,
            "edit": null, "evaluation": null, "source_leases": sources,
        }))
        .unwrap(),
    )
    .await;
}

pub async fn prepared(
    store: &dyn DatasetStore,
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    expected_revision: Option<SplitRevisionId>,
) -> SplitRevisionRecord {
    let operation = OperationId::random();
    let input = SplitPreparation {
        revision: revision(operation, 1),
        policy: SplitPolicy::default(),
        splits: crate::split_store::partition_splits(),
    };
    let record = authority
        .prepare_split_revision(identity, snapshot, &input, &ReadCancellation::new())
        .await
        .unwrap();
    install(store, identity, operation, 1, expected_revision, &record).await
}

/// Installs historical draft/window bytes around independently prepared final resource data.
pub async fn install(
    store: &dyn DatasetStore,
    identity: &DatasetIdentity,
    operation: OperationId,
    input_revision: u64,
    expected_revision: Option<SplitRevisionId>,
    record: &SplitRevisionRecord,
) -> SplitRevisionRecord {
    assert_eq!(record.revision, revision(operation, input_revision));
    let mut record = record.clone();
    record.operation = operation;
    replace(
        store,
        &key::revision(identity, record.snapshot, record.revision).unwrap(),
        &record.encode().unwrap(),
    )
    .await;
    replace(
        store,
        &key::revision_summary(identity, record.snapshot, record.revision).unwrap(),
        &serde_json::to_vec(&json!({
            "version": 1, "snapshot": record.snapshot, "revision": record.revision,
            "operation": operation, "split_count": record.splits.len(),
        }))
        .unwrap(),
    )
    .await;
    windows(store, identity, operation, input_revision, &record).await;
    state(
        store,
        identity,
        &SplitDraftRecord {
            version: 1,
            operation,
            snapshot: record.snapshot,
            expected_revision,
            revision: input_revision,
            policy: record.policy,
            splits: record
                .splits
                .iter()
                .map(|split| DraftSplit {
                    name: split.name.clone(),
                    definition: split.definition.clone(),
                    origin: split.origin.clone(),
                })
                .collect(),
            progress: DraftProgress::Prepared {
                target: CommitTarget::SplitRevision {
                    snapshot: record.snapshot,
                    revision: record.revision,
                },
            },
        },
    )
    .await;
    record
}

async fn windows(
    store: &dyn DatasetStore,
    identity: &DatasetIdentity,
    operation: OperationId,
    input_revision: u64,
    record: &SplitRevisionRecord,
) {
    let mut windows = BTreeMap::<u32, Vec<serde_json::Value>>::new();
    for split in &record.splits {
        for page in &split.pages {
            windows.entry(page.page).or_default().push(json!({
                "key": key::membership_page(identity, record.snapshot, record.revision, &split.name, page.page).unwrap(),
                "digest": page.sha256,
            }));
        }
    }
    for (page, pages) in windows {
        let mut key = state_key(b"DS\0SW\0", identity, operation);
        key.extend_from_slice(&input_revision.to_be_bytes());
        key.extend_from_slice(&u64::from(page).to_be_bytes());
        replace(
            store,
            &key,
            &serde_json::to_vec(&json!({ "version": 1, "pages": pages })).unwrap(),
        )
        .await;
    }
}

pub async fn remove_root(store: &dyn DatasetStore, identity: &DatasetIdentity, record: &SplitRevisionRecord) {
    store
        .delete(&key::revision(identity, record.snapshot, record.revision).unwrap())
        .await
        .unwrap();
    store
        .delete(&key::revision_summary(identity, record.snapshot, record.revision).unwrap())
        .await
        .unwrap();
}

async fn replace(store: &dyn DatasetStore, key: &[u8], bytes: &[u8]) {
    let old = store.get(key).await.unwrap();
    assert!(matches!(
        store
            .compare_exchange(key, old.as_ref().map(|value| value.bytes.as_slice()), bytes)
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
}
