// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
#[path = "common/inspection_reads.rs"]
mod inspection_reads;
use crowdb_access_dataset::{
    inspection::{FieldStorageKind, InspectionError, InspectionSample},
    *,
};
use inspection_reads::TestIndexReads;
use md5::{Digest, Md5};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

struct TestMatrix {
    store: Arc<TestIndexReads>,
    authority: DatasetAuthority,
    identity: DatasetIdentity,
    parent: SnapshotId,
    child: SnapshotId,
    ids: Vec<Vec<u8>>,
}
async fn fixture() -> TestMatrix {
    let store = Arc::new(TestIndexReads::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "matrix-cost").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let mut manifest = ManifestRecord {
        version: 1,
        snapshot: SnapshotId::random(),
        schema: SchemaRecord {
            version: 1,
            fields: ["body", "hidden", "absent"]
                .into_iter()
                .map(|name| FieldDefinition {
                    name: name.into(),
                    required: false,
                })
                .collect(),
        },
        samples: (0..32u8)
            .map(|id| SampleRecord {
                sample_id: vec![id, 0, 255],
                fields: vec![
                    FieldRecord {
                        name: "body".into(),
                        value: FieldLocator::Inline {
                            value: vec![255; 13],
                            md5: Md5::digest([255; 13]).into(),
                        },
                    },
                    FieldRecord {
                        name: "hidden".into(),
                        value: FieldLocator::Tombstone,
                    },
                ],
            })
            .collect(),
    };
    let parent = authority
        .publish_snapshot(&identity, None, bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    for sample in &mut manifest.samples {
        sample.fields.retain(|field| field.name != "body");
    }
    let child = authority
        .publish_snapshot(&identity, Some(parent), bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    let ids = manifest
        .samples
        .iter()
        .map(|sample| sample.sample_id.clone())
        .collect();
    store.begin();
    TestMatrix {
        store,
        authority,
        identity,
        parent,
        child,
        ids,
    }
}
fn budget() -> MetadataScanBudget {
    MetadataScanBudget {
        items: 100,
        bytes: 1024 * 1024,
        timeout: Duration::from_secs(2),
    }
}
fn fields() -> Vec<String> {
    ["body", "hidden", "absent"]
        .into_iter()
        .map(String::from)
        .collect()
}
fn verify(rows: &[InspectionSample], ids: &[Vec<u8>], parent: SnapshotId, child: SnapshotId) {
    assert_eq!(rows.len(), ids.len());
    for (row, id) in rows.iter().zip(ids) {
        assert_eq!(&row.sample_id, id);
        let body = row.fields[0].as_ref().unwrap();
        assert_eq!(body.source_snapshot, parent);
        assert_eq!(body.storage, FieldStorageKind::Inline);
        let hidden = row.fields[1].as_ref().unwrap();
        assert_eq!(hidden.source_snapshot, child);
        assert_eq!(hidden.storage, FieldStorageKind::Tombstone);
        assert!(row.fields[2].is_none());
    }
}
fn costs(store: &TestIndexReads) -> (usize, usize, usize) {
    (
        store.reads.lock().unwrap().values().sum(),
        store.schema_scans.load(Ordering::Relaxed),
        store.member_scans.load(Ordering::Relaxed),
    )
}
fn assert_admitted_once(fixture: &TestMatrix) {
    let reads = fixture.store.reads.lock().unwrap();
    for snapshot in [fixture.parent, fixture.child] {
        assert_eq!(
            reads[&TestIndexReads::key(&fixture.identity, snapshot, b'H', &[])],
            1
        );
    }
    let offset = b"DS\0I\0".len() + fixture.identity.key().unwrap().len() + 16;
    assert!(!reads
        .keys()
        .any(|key| key.starts_with(b"DS\0I\0") && key[offset] == b'V'));
}

#[tokio::test]
async fn matrix_overlaps_metadata_without_payload_reads_or_repeated_ancestor_admission() {
    let f = fixture().await;
    let page = f
        .authority
        .inspection_matrix_page(
            &f.identity,
            f.child,
            &fields(),
            None,
            budget(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    verify(&page.entries, &f.ids, f.parent, f.child);
    assert!(page.after.is_none());
    assert_eq!(costs(&f.store), (166, 1, 1));
    assert_admitted_once(&f);
    assert!((2..=16).contains(&f.store.peak_pending.load(Ordering::Relaxed)));
    assert!(f.store.peak_reserved.load(Ordering::Relaxed) <= 2 * 1024 * 1024);
    assert_eq!(f.store.pending.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn exact_matrix_preserves_requested_order_and_membership_validation() {
    let f = fixture().await;
    let ids: Vec<_> = f.ids.iter().rev().cloned().collect();
    let rows = f
        .authority
        .inspection_exact_matrix(
            &f.identity,
            f.child,
            &ids,
            &fields(),
            budget(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    verify(&rows, &ids, f.parent, f.child);
    assert_eq!(costs(&f.store), (198, 1, 0));
    assert_admitted_once(&f);
    assert!((2..=16).contains(&f.store.peak_pending.load(Ordering::Relaxed)));
    assert!(f.store.peak_reserved.load(Ordering::Relaxed) <= 2 * 1024 * 1024);
    assert_eq!(f.store.pending.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn missing_explicit_members_fail_before_metadata_and_corruption_drops_pending_reads() {
    let f = fixture().await;
    let mut ids = f.ids.clone();
    ids[17] = b"missing".to_vec();
    assert!(matches!(
        f.authority
            .inspection_exact_matrix(
                &f.identity,
                f.child,
                &ids,
                &fields(),
                budget(),
                &ReadCancellation::new()
            )
            .await,
        Err(InspectionError::Authority(AuthorityError::Invalid(
            DatasetError::SampleNotFound
        )))
    ));
    assert_eq!(f.store.pending.load(Ordering::Relaxed), 0);
    let offset = b"DS\0I\0".len() + f.identity.key().unwrap().len() + 16;
    assert!(!f
        .store
        .reads
        .lock()
        .unwrap()
        .keys()
        .any(|key| key.starts_with(b"DS\0I\0") && key[offset] == b'F'));
    let key = TestIndexReads::key(&f.identity, f.child, b'F', &[&f.ids[10], b"hidden"]);
    let old = f.store.inner.get(&key).await.unwrap().unwrap();
    let mut metadata: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
    metadata["name"] = serde_json::json!("wrong");
    f.store
        .inner
        .compare_exchange(&key, Some(&old.bytes), &serde_json::to_vec(&metadata).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        f.authority
            .inspection_matrix_page(
                &f.identity,
                f.child,
                &fields(),
                None,
                budget(),
                &ReadCancellation::new()
            )
            .await,
        Err(InspectionError::Corrupt)
    ));
    assert_eq!(f.store.pending.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn matrix_reserves_aggregate_budget_and_cancellation_releases_all_reads() {
    let f = fixture().await;
    assert!(matches!(
        f.authority
            .inspection_matrix_page(
                &f.identity,
                f.child,
                &fields(),
                None,
                MetadataScanBudget {
                    bytes: 4096,
                    ..budget()
                },
                &ReadCancellation::new()
            )
            .await,
        Err(InspectionError::Limit | InspectionError::Store(StoreError::Limit))
    ));
    let bytes = f.store.total_bytes.load(Ordering::Relaxed);
    assert!(
        bytes <= 4096,
        "materialized {bytes} bytes; costs={:?}",
        costs(&f.store)
    );
    assert_eq!(f.store.pending.load(Ordering::Relaxed), 0);
    f.store.begin();
    let cancel = ReadCancellation::new();
    f.store.stalled.store(true, Ordering::Relaxed);
    let stop = async {
        while f.store.pending.load(Ordering::Relaxed) < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        cancel.cancel();
    };
    let projection = fields();
    let read = f
        .authority
        .inspection_matrix_page(&f.identity, f.child, &projection, None, budget(), &cancel);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async { tokio::join!(read, stop) })
        .await
        .unwrap();
    assert!(result.is_err());
    assert_eq!(f.store.pending.load(Ordering::Relaxed), 0);
    assert!(f.store.peak_reserved.load(Ordering::Relaxed) <= 2 * 1024 * 1024);
}
