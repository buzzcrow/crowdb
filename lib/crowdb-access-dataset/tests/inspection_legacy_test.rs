// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
use crowdb_access_dataset::{
    inspection::{InspectionError, PreparationLimits, PreparationState},
    *,
};
use std::{sync::Arc, time::Duration};

fn snapshot_key(identity: &DatasetIdentity, snapshot: SnapshotId) -> Vec<u8> {
    let mut key = b"DS\0S\0".to_vec();
    key.extend_from_slice(&identity.key().unwrap());
    key.extend_from_slice(snapshot.as_bytes());
    key
}
#[derive(serde::Serialize)]
struct TestVersioned<T> {
    version: u16,
    payload: T,
}
struct TestStore(dataset_store::TestDatasetStore);
#[async_trait::async_trait]
impl DatasetStore for TestStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        assert!(
            !key.starts_with(b"DS\0S\0"),
            "legacy publication must use bounded read"
        );
        self.0.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.0.get_bounded(key, budget, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        self.0.compare_exchange(key, expected, value).await
    }
}
async fn fixture(opaque: bool) -> (Arc<TestStore>, DatasetIdentity, SnapshotId, Vec<u8>) {
    let store = Arc::new(TestStore(dataset_store::TestDatasetStore::default()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "legacy").unwrap();
    DatasetAuthority::new(store.clone())
        .create_dataset(identity.clone())
        .await
        .unwrap();
    let snapshot = SnapshotId::random();
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "value".into(),
                required: false,
            }],
        },
        samples: (0..3)
            .map(|i| SampleRecord {
                sample_id: vec![i],
                fields: vec![FieldRecord {
                    name: "value".into(),
                    value: FieldLocator::Tombstone,
                }],
            })
            .collect(),
    };
    let publication = SnapshotPublication {
        snapshot,
        parent: None,
        operation: OperationId::random(),
        manifest: if opaque {
            b"opaque".to_vec()
        } else {
            bincode::serialize(&manifest).unwrap()
        },
        state: PublicationState::Published,
    };
    let bytes = bincode::serialize(&TestVersioned {
        version: 1,
        payload: SnapshotRecord { publication },
    })
    .unwrap();
    store
        .compare_exchange(&snapshot_key(&identity, snapshot), None, &bytes)
        .await
        .unwrap();
    (store, identity, snapshot, bytes)
}
#[tokio::test]
async fn legacy_preparation_is_bounded_resumable_and_preserves_old_bytes() {
    let (store, identity, snapshot, old_bytes) = fixture(false).await;
    let cancel = ReadCancellation::new();
    let limits = PreparationLimits {
        rows: 2,
        ..PreparationLimits::default()
    };
    let mut previous = 0;
    let mut completed = None;
    for _ in 0..8 {
        let authority = DatasetAuthority::new(store.clone());
        let progress = authority
            .prepare_inspection_page(&identity, snapshot, limits, &cancel)
            .await
            .unwrap();
        assert!(progress.rows_written <= previous + 2);
        assert!(progress.rows_written >= previous);
        previous = progress.rows_written;
        assert_eq!(
            authority
                .inspection_preparation_status(&identity, snapshot, &cancel)
                .await
                .unwrap(),
            progress
        );
        if progress.state == PreparationState::Ready {
            completed = Some(progress);
            break;
        }
    }
    let completed = completed.expect("bounded steps must finish");
    assert_eq!(completed.rows_written, 10);
    let restarted = DatasetAuthority::new(store.clone());
    assert_eq!(
        restarted
            .prepare_inspection_page(&identity, snapshot, limits, &cancel)
            .await
            .unwrap(),
        completed
    );
    assert_eq!(
        store
            .0
            .get(&snapshot_key(&identity, snapshot))
            .await
            .unwrap()
            .unwrap()
            .bytes,
        old_bytes
    );
}
#[tokio::test]
async fn legacy_limits_cancellation_and_opaque_data_are_explicit() {
    let (store, identity, snapshot, _) = fixture(false).await;
    let authority = DatasetAuthority::new(store.clone());
    let cancel = ReadCancellation::new();
    assert!(matches!(
        authority
            .prepare_inspection_page(
                &identity,
                snapshot,
                PreparationLimits {
                    legacy_bytes: 32,
                    ..PreparationLimits::default()
                },
                &cancel
            )
            .await,
        Err(InspectionError::Limit)
    ));
    cancel.cancel();
    assert!(matches!(
        authority
            .prepare_inspection_page(&identity, snapshot, PreparationLimits::default(), &cancel)
            .await,
        Err(InspectionError::Cancelled)
    ));
    assert!(matches!(
        authority
            .inspection_preparation_status(&identity, snapshot, &ReadCancellation::new())
            .await,
        Err(InspectionError::NotPrepared)
    ));
    let (store, identity, snapshot, _) = fixture(true).await;
    let authority = DatasetAuthority::new(store);
    let result = authority
        .prepare_inspection_page(
            &identity,
            snapshot,
            PreparationLimits::default(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert!(matches!(result.state, PreparationState::Unsupported { .. }));
    assert_eq!(result.rows_written, 0);
    assert_eq!(
        authority
            .inspection_preparation_status(&identity, snapshot, &ReadCancellation::new())
            .await
            .unwrap(),
        result
    );
}
#[tokio::test]
async fn legacy_work_never_persists_reader_activity_on_limit_failure() {
    let (store, identity, snapshot, _) = fixture(false).await;
    let authority = DatasetAuthority::new(store.clone());
    let _ = authority
        .prepare_inspection_page(
            &identity,
            snapshot,
            PreparationLimits {
                bytes: 1,
                ..PreparationLimits::default()
            },
            &ReadCancellation::new(),
        )
        .await;
    // Preparing inspection work must not register snapshot reader activity.
    let mut key = b"DS\0L\0".to_vec();
    key.extend_from_slice(&identity.key().unwrap());
    key.extend_from_slice(snapshot.as_bytes());
    let stored = store
        .get_bounded(
            &key,
            MetadataScanBudget {
                items: 1,
                bytes: 4096,
                timeout: Duration::from_secs(1),
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert!(stored.is_none());
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TestAccess {
    _reclaiming: bool,
    _legacy: u64,
    _deadline: u64,
    leases: Vec<(OperationId, u64)>,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct TestEnvelope {
    _version: u16,
    payload: TestAccess,
}

#[tokio::test]
async fn legacy_preparation_rejects_reclaim_fence_and_corrupt_resume_position() {
    let (store, identity, snapshot, _) = fixture(false).await;
    let authority = DatasetAuthority::new(store.clone());
    let cancel = ReadCancellation::new();
    authority
        .prepare_inspection_page(
            &identity,
            snapshot,
            PreparationLimits {
                rows: 1,
                ..PreparationLimits::default()
            },
            &cancel,
        )
        .await
        .unwrap();
    let mut key = b"DS\0I\0".to_vec();
    key.extend_from_slice(&identity.key().unwrap());
    key.extend_from_slice(snapshot.as_bytes());
    key.push(b'P');
    let old = store.get(&key).await.unwrap().unwrap();
    let mut progress: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
    progress["position"]["sample"] = serde_json::json!(999);
    store
        .compare_exchange(&key, Some(&old.bytes), &serde_json::to_vec(&progress).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        authority
            .prepare_inspection_page(&identity, snapshot, PreparationLimits::default(), &cancel)
            .await,
        Err(InspectionError::Authority(AuthorityError::Corrupt))
    ));
    let (store, identity, snapshot, _) = fixture(false).await;
    let mut key = b"DS\0L\0".to_vec();
    key.extend_from_slice(&identity.key().unwrap());
    key.extend_from_slice(snapshot.as_bytes());
    let fence = TestEnvelope {
        _version: 2,
        payload: TestAccess {
            _reclaiming: true,
            _legacy: 0,
            _deadline: 0,
            leases: vec![],
        },
    };
    store
        .compare_exchange(&key, None, &bincode::serialize(&fence).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        DatasetAuthority::new(store)
            .prepare_inspection_page(&identity, snapshot, PreparationLimits::default(), &cancel)
            .await,
        Err(InspectionError::Authority(AuthorityError::SnapshotProtected))
    ));
}
