// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
#[path = "common/inspection_writes.rs"]
mod inspection_writes;

use crowdb_access_dataset::{
    DatasetAuthority, DatasetIdentity, DatasetStore, FieldDefinition, FieldLocator, FieldRecord,
    ManifestRecord, NamespacePath, OperationId, SampleRecord, SchemaRecord, SnapshotId,
};
use inspection_writes::TestIndexWrites;
use md5::{Digest, Md5};
use std::sync::{atomic::Ordering, Arc};

fn manifest(samples: u8, bytes: usize) -> ManifestRecord {
    let value = vec![255; bytes];
    ManifestRecord {
        version: 1,
        snapshot: SnapshotId::random(),
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "body".into(),
                required: true,
            }],
        },
        samples: (0..samples)
            .map(|id| SampleRecord {
                sample_id: vec![id],
                fields: vec![FieldRecord {
                    name: "body".into(),
                    value: FieldLocator::Inline {
                        md5: Md5::digest(&value).into(),
                        value: value.clone(),
                    },
                }],
            })
            .collect(),
    }
}

fn ready_key(identity: &DatasetIdentity, snapshot: SnapshotId) -> Vec<u8> {
    let mut key = b"DS\0I\0".to_vec();
    key.extend(identity.key().unwrap());
    key.extend(snapshot.as_bytes());
    key.push(b'H');
    key
}

#[tokio::test]
async fn index_writes_overlap_with_both_count_and_encoded_byte_limits() {
    for bytes in [1, 64 * 1024] {
        let identity = DatasetIdentity::new(NamespacePath::root(), "write-budget").unwrap();
        let store = Arc::new(TestIndexWrites::new(identity.key().unwrap().len()));
        let authority = DatasetAuthority::new(store.clone());
        authority.create_dataset(identity.clone()).await.unwrap();
        let samples = 12;
        let snapshot = authority
            .prepare_snapshot_with_operation(
                &identity,
                None,
                bincode::serialize(&manifest(samples, bytes)).unwrap(),
                OperationId::random(),
            )
            .await
            .unwrap();
        assert_eq!(store.rows.load(Ordering::Relaxed), 1 + 3 * usize::from(samples));
        let pending = store.peak_pending.load(Ordering::Relaxed);
        assert!((2..=16).contains(&pending), "peak requests: {pending}");
        assert!(store.peak_bytes.load(Ordering::Relaxed) <= 2 * 1024 * 1024);
        if bytes > 1 {
            assert!(
                store.peak_values.load(Ordering::Relaxed) <= 7,
                "encoded values must consume the byte budget"
            );
        }
        assert!(store
            .get(&ready_key(&identity, snapshot))
            .await
            .unwrap()
            .is_some());
        assert!(authority.latest(&identity).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn failed_parallel_row_keeps_index_hidden_and_retries_immutable_rows() {
    let identity = DatasetIdentity::new(NamespacePath::root(), "write-failure").unwrap();
    let store = Arc::new(TestIndexWrites::new(identity.key().unwrap().len()));
    let authority = DatasetAuthority::new(store.clone());
    authority.create_dataset(identity.clone()).await.unwrap();
    let operation = OperationId::random();
    let bytes = bincode::serialize(&manifest(12, 1)).unwrap();
    store.fail_value.store(true, Ordering::Relaxed);
    assert!(authority
        .prepare_snapshot_with_operation(&identity, None, bytes.clone(), operation)
        .await
        .is_err());
    let snapshot = authority
        .publication_status(&identity, operation)
        .await
        .unwrap()
        .publication
        .snapshot;
    assert!(store
        .get(&ready_key(&identity, snapshot))
        .await
        .unwrap()
        .is_none());
    assert!(authority.latest(&identity).await.unwrap().is_none());
    let restarted = DatasetAuthority::new(store.clone());
    assert_eq!(
        restarted
            .prepare_snapshot_with_operation(&identity, None, bytes, operation)
            .await
            .unwrap(),
        snapshot
    );
    assert!(store
        .get(&ready_key(&identity, snapshot))
        .await
        .unwrap()
        .is_some());
    assert!(restarted.latest(&identity).await.unwrap().is_none());
}
