// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{fixture::TestCleanup, legacy, legacy_split, publication};
use crowdb_access_dataset::{composition::DraftProgress, split::*, *};
use std::sync::{atomic::Ordering, Arc};

pub fn block(test: &TestCleanup, prefix: &[u8], operations: &[OperationId]) {
    test.store.blocked.store(Arc::new(
        operations
            .iter()
            .map(|operation| legacy::key(prefix, &test.identity, *operation))
            .collect(),
    ));
}
pub fn observe(test: &TestCleanup, key: Vec<u8>) {
    test.store.observed.store(Some(Arc::new(key)));
    test.store.frozen.store(true, Ordering::Relaxed);
}
pub async fn remaining(test: &TestCleanup, prefix: &[u8], operations: &[OperationId]) -> usize {
    let mut remaining = 0;
    for operation in operations {
        remaining += usize::from(
            test.store
                .get(&legacy::key(prefix, &test.identity, *operation))
                .await
                .unwrap()
                .is_some(),
        );
    }
    remaining
}
pub async fn compositions(test: &TestCleanup) -> Vec<OperationId> {
    let mut operations = Vec::new();
    for marker in 1..=39 {
        let operation = OperationId::from_bytes(&[marker; 16]).unwrap();
        test.draft(operation).await;
        operations.push(operation);
    }
    operations
}
pub async fn edits(test: &TestCleanup, operation: OperationId) -> Vec<u8> {
    let key = legacy::key(b"DS\0CD\0", &test.identity, operation);
    let old = test.store.get(&key).await.unwrap().unwrap();
    let mut state: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
    let ids: Vec<_> = (1..=41)
        .map(|marker| OperationId::from_bytes(&[marker; 16]).unwrap())
        .collect();
    state["registered_edits"] = serde_json::json!(ids);
    legacy::replace(test.store.as_ref(), &key, &old.bytes, &state).await;
    let mut observed = legacy::key(b"DS\0CE\0", &test.identity, operation);
    observed.extend(ids[0].as_bytes());
    observed
}
pub async fn splits(test: &TestCleanup) -> Vec<OperationId> {
    let mut operations = Vec::new();
    for marker in 1..=39 {
        let operation = OperationId::from_bytes(&[marker; 16]).unwrap();
        legacy_split::state(
            test.store.as_ref(),
            &test.identity,
            &SplitDraftRecord {
                version: 1,
                operation,
                snapshot: test.parent,
                expected_revision: None,
                revision: 1,
                policy: SplitPolicy::default(),
                splits: Vec::new(),
                progress: DraftProgress::Abandoned,
            },
        )
        .await;
        test.store
            .compare_exchange(
                &legacy::key(b"DS\0SCG\0", &test.identity, operation),
                None,
                b"obsolete progress",
            )
            .await
            .unwrap();
        operations.push(operation);
    }
    operations
}
pub async fn members(test: &TestCleanup, operation: OperationId) -> Vec<u8> {
    let revision = legacy_split::revision(operation, 1);
    let mut pages = Vec::new();
    for index in 0..45 {
        let key = key::membership_page(
            &test.identity,
            test.parent,
            revision,
            &SplitName::new(format!("member-{index:02}")).unwrap(),
            0,
        )
        .unwrap();
        test.store
            .compare_exchange(&key, None, b"obsolete member payload")
            .await
            .unwrap();
        pages.push(serde_json::json!({"key":key,"digest":vec![0u8;32]}));
    }
    let observed: Vec<u8> = serde_json::from_value(pages[0]["key"].clone()).unwrap();
    let mut work = legacy::key(b"DS\0SW\0", &test.identity, operation);
    work.extend(1u64.to_be_bytes());
    work.extend(0u64.to_be_bytes());
    test.store
        .compare_exchange(
            &work,
            None,
            &serde_json::to_vec(&serde_json::json!({"version":1,"pages":pages})).unwrap(),
        )
        .await
        .unwrap();
    observed
}
pub async fn snapshots(test: &TestCleanup, authority: &DatasetAuthority) -> Vec<SnapshotId> {
    let mut snapshots = Vec::new();
    for marker in 1..=33 {
        let mut manifest: ManifestRecord =
            bincode::deserialize(&publication::publication_manifest(marker)).unwrap();
        if marker == 33 {
            let sample = manifest.samples[0].clone();
            manifest.samples = (0..51)
                .map(|index| SampleRecord {
                    sample_id: vec![index],
                    fields: sample.fields.clone(),
                })
                .collect();
        }
        let snapshot = authority
            .prepare_snapshot_candidate(
                &test.identity,
                None,
                bincode::serialize(&manifest).unwrap(),
                OperationId::from_bytes(&[marker; 16]).unwrap(),
            )
            .await
            .unwrap();
        authority
            .mark_snapshot_reclaim(&test.identity, snapshot)
            .await
            .unwrap();
        snapshots.push(snapshot);
    }
    snapshots
}
