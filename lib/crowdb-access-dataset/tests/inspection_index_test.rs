// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
use crowdb_access_dataset::{
    inspection::{FieldMetadata, FieldStorageKind, InspectionState, SnapshotSummary},
    DatasetAuthority, DatasetIdentity, DatasetStore, FieldDefinition, FieldLocator, FieldRecord,
    ManifestRecord, MetadataScan, MetadataScanBudget, NamespacePath, OperationId, ReadCancellation,
    SampleRecord, SchemaRecord, SnapshotId,
};
use md5::{Digest, Md5};
use std::{sync::Arc, time::Duration};

fn manifest(id: &[u8]) -> ManifestRecord {
    let value = vec![0x51; 64 * 1024];
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
        samples: vec![SampleRecord {
            sample_id: id.into(),
            fields: vec![FieldRecord {
                name: "body".into(),
                value: FieldLocator::Inline {
                    md5: Md5::digest(&value).into(),
                    value,
                },
            }],
        }],
    }
}
fn scope(identity: &DatasetIdentity, snapshot: SnapshotId, kind: u8) -> Vec<u8> {
    let mut key = b"DS\0I\0".to_vec();
    key.extend_from_slice(&identity.key().unwrap());
    key.extend_from_slice(snapshot.as_bytes());
    key.push(kind);
    key
}
async fn rows(
    store: &dyn DatasetStore,
    prefix: Vec<u8>,
) -> Vec<(Vec<u8>, crowdb_access_dataset::StoredValue)> {
    store
        .scan_page(
            &MetadataScan {
                prefix,
                after: None,
                budget: MetadataScanBudget {
                    items: 100,
                    bytes: 1024 * 1024,
                    timeout: Duration::from_secs(1),
                },
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap()
        .entries
}
#[tokio::test]
async fn prepared_projection_is_complete_small_and_separate_from_payload() {
    let store = Arc::new(dataset_store::TestDatasetStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "projection").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let manifest = manifest(b"a\0b");
    let operation = OperationId::random();
    let encoded = bincode::serialize(&manifest).unwrap();
    let snapshot = authority
        .prepare_snapshot_with_operation(&identity, None, encoded.clone(), operation)
        .await
        .unwrap();
    assert!(authority.latest(&identity).await.unwrap().is_none());
    let summaries = rows(store.as_ref(), scope(&identity, snapshot, b'H')).await;
    let summary: SnapshotSummary = serde_json::from_slice(&summaries[0].1.bytes).unwrap();
    assert_eq!(summary.state, InspectionState::Ready);
    assert_eq!(summary.sample_count, 1);
    assert_eq!(summary.field_count, 1);
    let metadata = rows(store.as_ref(), scope(&identity, snapshot, b'F')).await;
    assert_eq!(metadata.len(), 1);
    assert!(metadata[0].1.bytes.len() < 512);
    let field: FieldMetadata = serde_json::from_slice(&metadata[0].1.bytes).unwrap();
    assert_eq!(field.storage, FieldStorageKind::Inline);
    assert_eq!(field.length, 64 * 1024);
    let values = rows(store.as_ref(), scope(&identity, snapshot, b'V')).await;
    let value: FieldLocator = serde_json::from_slice(&values[0].1.bytes).unwrap();
    assert_eq!(value, manifest.samples[0].fields[0].value);
    let restarted = DatasetAuthority::new(store.clone());
    assert_eq!(
        restarted
            .prepare_snapshot_with_operation(&identity, None, encoded.clone(), operation)
            .await
            .unwrap(),
        snapshot
    );
    assert_eq!(
        rows(store.as_ref(), scope(&identity, snapshot, b'F')).await,
        metadata
    );
    assert_eq!(
        restarted
            .publish_snapshot_with_operation(&identity, None, encoded, operation)
            .await
            .unwrap(),
        snapshot
    );
}
#[tokio::test]
async fn unsupported_legacy_identity_does_not_break_publication_or_look_empty() {
    let store = Arc::new(dataset_store::TestDatasetStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "large-id").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let manifest = manifest(&vec![1; 16 * 1024 + 1]);
    let snapshot = authority
        .publish_snapshot(&identity, None, bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    let summaries = rows(store.as_ref(), scope(&identity, snapshot, b'H')).await;
    let summary: SnapshotSummary = serde_json::from_slice(&summaries[0].1.bytes).unwrap();
    assert!(matches!(summary.state, InspectionState::Unsupported { .. }));
    assert_eq!(summary.sample_count, 1);
    assert_eq!(authority.latest(&identity).await.unwrap(), Some(snapshot));
}

struct TestInterruptedStore {
    inner: dataset_store::TestDatasetStore,
    fail_ready: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl DatasetStore for TestInterruptedStore {
    async fn get(
        &self,
        key: &[u8],
    ) -> Result<Option<crowdb_access_dataset::StoredValue>, crowdb_access_dataset::StoreError> {
        self.inner.get(key).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<crowdb_access_dataset::CasOutcome, crowdb_access_dataset::StoreError> {
        if key.starts_with(b"DS\0I\0")
            && key.last() == Some(&b'H')
            && self.fail_ready.swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            return Err(crowdb_access_dataset::StoreError::Rejected);
        }
        self.inner.compare_exchange(key, expected, value).await
    }
}
#[tokio::test]
async fn interrupted_index_preparation_cannot_publish_and_retries_same_rows() {
    let store = Arc::new(TestInterruptedStore {
        inner: dataset_store::TestDatasetStore::default(),
        fail_ready: true.into(),
    });
    let identity = DatasetIdentity::new(NamespacePath::root(), "interrupted").unwrap();
    let authority = DatasetAuthority::new(store.clone());
    authority.create_dataset(identity.clone()).await.unwrap();
    let bytes = bincode::serialize(&manifest(b"sample")).unwrap();
    let operation = OperationId::random();
    assert!(authority
        .publish_snapshot_with_operation(&identity, None, bytes.clone(), operation)
        .await
        .is_err());
    assert!(authority.latest(&identity).await.unwrap().is_none());
    let snapshot = authority
        .publication_status(&identity, operation)
        .await
        .unwrap()
        .publication
        .snapshot;
    assert!(store
        .get(&scope(&identity, snapshot, b'H'))
        .await
        .unwrap()
        .is_none());
    let before = rows(&store.inner, scope(&identity, snapshot, b'F')).await;
    assert_eq!(before.len(), 1);
    let restarted = DatasetAuthority::new(store.clone());
    assert_eq!(
        restarted
            .publish_snapshot_with_operation(&identity, None, bytes, operation)
            .await
            .unwrap(),
        snapshot
    );
    assert_eq!(rows(&store.inner, scope(&identity, snapshot, b'F')).await, before);
    assert!(store
        .get(&scope(&identity, snapshot, b'H'))
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn sample_index_preserves_exact_binary_order_and_snapshot_scope() {
    let store = Arc::new(dataset_store::TestDatasetStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "binary-ids").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let mut manifest = manifest(b"template");
    let ids = vec![
        b"a".to_vec(),
        b"a\0".to_vec(),
        b"a\0b".to_vec(),
        b"a\xff".to_vec(),
        b"b".to_vec(),
    ];
    let sample = manifest.samples[0].clone();
    manifest.samples = ids
        .iter()
        .rev()
        .map(|id| SampleRecord {
            sample_id: id.clone(),
            ..sample.clone()
        })
        .collect();
    let parent = authority
        .publish_snapshot(&identity, None, bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    let child = authority
        .publish_snapshot(&identity, Some(parent), bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    for snapshot in [parent, child] {
        let stored = rows(store.as_ref(), scope(&identity, snapshot, b'M')).await;
        let actual: Vec<Vec<u8>> = stored
            .iter()
            .map(|(_, value)| serde_json::from_slice(&value.bytes).unwrap())
            .collect();
        assert_eq!(actual, ids);
        let summary = rows(store.as_ref(), scope(&identity, snapshot, b'H')).await;
        let summary: SnapshotSummary = serde_json::from_slice(&summary[0].1.bytes).unwrap();
        assert_eq!(summary.snapshot, snapshot);
        assert_eq!(
            summary.parent,
            if snapshot == parent { None } else { Some(parent) }
        );
    }
}
