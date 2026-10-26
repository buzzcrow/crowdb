// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
#[path = "common/inspection_reads.rs"]
mod inspection_reads;
use crowdb_access_dataset::{inspection::InspectionError, *};
use inspection_reads::TestIndexReads;
use md5::{Digest, Md5};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

fn require_send<T: Send>(value: T) -> T {
    value
}

fn manifest(count: u8, bytes: usize) -> ManifestRecord {
    let value = vec![255; bytes];
    ManifestRecord {
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
        samples: (0..count)
            .map(|i| SampleRecord {
                sample_id: vec![i, 0, 255],
                fields: vec![
                    FieldRecord {
                        name: "body".into(),
                        value: FieldLocator::Inline {
                            md5: Md5::digest(&value).into(),
                            value: value.clone(),
                        },
                    },
                    FieldRecord {
                        name: "hidden".into(),
                        value: FieldLocator::Tombstone,
                    },
                ],
            })
            .collect(),
    }
}

async fn fixture(
    count: u8,
    bytes: usize,
) -> (
    Arc<TestIndexReads>,
    DatasetAuthority,
    DatasetIdentity,
    SnapshotId,
    ManifestRecord,
) {
    let store = Arc::new(TestIndexReads::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "parallel-read").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let manifest = manifest(count, bytes);
    let snapshot = authority
        .publish_snapshot(&identity, None, bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    store.begin();
    (store, authority, identity, snapshot, manifest)
}

#[tokio::test]
async fn delivery_overlaps_bounded_reads_preserving_order_inheritance_and_tombstones() {
    let (store, authority, identity, parent, mut manifest) = fixture(32, 13).await;
    store.reading.store(false, Ordering::Relaxed);
    for sample in &mut manifest.samples {
        sample.fields.retain(|f| f.name != "body");
    }
    let child = authority
        .publish_snapshot(&identity, Some(parent), bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    let ids: Vec<_> = manifest
        .samples
        .iter()
        .rev()
        .map(|s| s.sample_id.clone())
        .collect();
    store.begin();
    let rows = require_send(authority.resolve_inspection_batch_locators(
        &identity,
        child,
        &ids,
        &["body".into(), "hidden".into(), "absent".into()],
        &ReadCancellation::new(),
    ))
    .await
    .unwrap();
    for (row, id) in rows.iter().zip(&ids) {
        assert_eq!(&row.sample_id, id);
        assert!(matches!(&row.fields["body"], FieldLocator::Inline { value, .. } if value == &vec![255; 13]));
        assert_eq!(row.fields["hidden"], FieldLocator::Tombstone);
        assert!(!row.fields.contains_key("absent"));
    }
    let peak = store.peak_pending.load(Ordering::Relaxed);
    assert!((2..=16).contains(&peak), "peak reads: {peak}");
    assert!(store.peak_reserved.load(Ordering::Relaxed) <= 2 * 1024 * 1024);
    let reads = store.reads.lock().unwrap();
    for snapshot in [parent, child] {
        assert_eq!(reads[&TestIndexReads::key(&identity, snapshot, b'H', &[])], 1);
    }
    let kind_offset = b"DS\0I\0".len() + identity.key().unwrap().len() + 16;
    for (kind, expected) in [(b'M', 32), (b'F', 160), (b'V', 32)] {
        assert_eq!(
            reads
                .iter()
                .filter(|(key, _)| key.starts_with(b"DS\0I\0") && key[kind_offset] == kind)
                .map(|(_, count)| count)
                .sum::<usize>(),
            expected
        );
    }
    assert!(!reads
        .keys()
        .any(|key| key == &TestIndexReads::key(&identity, parent, b'V', &[&ids[0], b"hidden"])));
}

#[tokio::test]
async fn mixed_locators_keep_integrity_checks_and_storage_failure_drops_peers() {
    let (store, authority, identity, parent, mut manifest) = fixture(20, 13).await;
    store.reading.store(false, Ordering::Relaxed);
    for (index, sample) in manifest.samples.iter_mut().enumerate() {
        sample.fields[0].value = if index % 2 == 0 {
            FieldLocator::Chunk {
                location: vec![0, 255, u8::try_from(index).unwrap()],
                length: 4097,
                md5: [7; 16],
            }
        } else {
            FieldLocator::Inline {
                value: Vec::new(),
                md5: Md5::digest([]).into(),
            }
        };
    }
    let snapshot = authority
        .publish_snapshot(&identity, Some(parent), bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    let ids: Vec<_> = manifest.samples.iter().map(|s| s.sample_id.clone()).collect();
    store.begin();
    let fields = ["body".into()];
    let rows = authority
        .resolve_inspection_batch_locators(&identity, snapshot, &ids, &fields, &ReadCancellation::new())
        .await
        .unwrap();
    for (row, expected) in rows.iter().zip(&manifest.samples) {
        assert_eq!(row.fields["body"], expected.fields[0].value);
    }
    store.fail_value.store(true, Ordering::Relaxed);
    assert!(matches!(
        authority
            .resolve_inspection_batch_locators(&identity, snapshot, &ids, &fields, &ReadCancellation::new())
            .await,
        Err(InspectionError::Store(StoreError::Rejected))
    ));
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
    store.fail_value.store(false, Ordering::Relaxed);
    let key = TestIndexReads::key(&identity, snapshot, b'V', &[&ids[0], b"body"]);
    let old = store.inner.get(&key).await.unwrap().unwrap();
    let bad = FieldLocator::Chunk {
        location: vec![1],
        length: 4096,
        md5: [7; 16],
    };
    store
        .inner
        .compare_exchange(&key, Some(&old.bytes), &serde_json::to_vec(&bad).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        authority
            .resolve_inspection_batch_locators(&identity, snapshot, &ids, &fields, &ReadCancellation::new())
            .await,
        Err(InspectionError::Corrupt)
    ));
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn delivery_enforces_aggregate_bytes_before_materialization_and_releases_failed_reads() {
    let (store, authority, identity, snapshot, manifest) = fixture(72, 64 * 1024).await;
    let ids: Vec<_> = manifest.samples.iter().map(|s| s.sample_id.clone()).collect();
    assert!(matches!(
        authority
            .resolve_inspection_batch_locators(
                &identity,
                snapshot,
                &ids,
                &["body".into()],
                &ReadCancellation::new()
            )
            .await,
        Err(InspectionError::Limit | InspectionError::Store(StoreError::Limit))
    ));
    assert!(store.total_bytes.load(Ordering::Relaxed) <= 16 * 1024 * 1024);
    assert!(store.peak_reserved.load(Ordering::Relaxed) <= 2 * 1024 * 1024);
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
    // The exact fitting prefix must not fail just because unused reservations are outstanding.
    store.begin();
    assert_eq!(
        authority
            .resolve_inspection_batch_locators(
                &identity,
                snapshot,
                &ids[..63],
                &["body".into()],
                &ReadCancellation::new()
            )
            .await
            .unwrap()
            .len(),
        63
    );
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn cancellation_and_deadline_drop_all_pending_metadata_reads() {
    let (store, authority, identity, snapshot, manifest) = fixture(20, 1).await;
    let ids: Vec<_> = manifest.samples.iter().map(|s| s.sample_id.clone()).collect();
    let cancel = ReadCancellation::new();
    store.stalled.store(true, Ordering::Relaxed);
    let stop = async {
        while store.pending.load(Ordering::Relaxed) < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        cancel.cancel();
    };
    let fields = ["body".into()];
    let read = authority.resolve_inspection_batch_locators(&identity, snapshot, &ids, &fields, &cancel);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async { tokio::join!(read, stop) })
        .await
        .unwrap();
    assert!(result.is_err());
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
    let started = std::time::Instant::now();
    assert!(matches!(
        authority
            .resolve_inspection_batch_locators(
                &identity,
                snapshot,
                &ids,
                &["body".into()],
                &ReadCancellation::new()
            )
            .await,
        Err(InspectionError::Limit)
    ));
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn missing_members_and_corrupt_inline_payloads_fail_without_partial_delivery() {
    let (store, authority, identity, snapshot, manifest) = fixture(20, 13).await;
    let mut ids: Vec<_> = manifest.samples.iter().map(|s| s.sample_id.clone()).collect();
    ids[10] = b"missing".to_vec();
    assert!(matches!(
        authority
            .resolve_inspection_batch_locators(
                &identity,
                snapshot,
                &ids,
                &["body".into()],
                &ReadCancellation::new()
            )
            .await,
        Err(InspectionError::Authority(AuthorityError::Invalid(
            DatasetError::SampleNotFound
        )))
    ));
    assert!(!store
        .reads
        .lock()
        .unwrap()
        .keys()
        .any(|key| key == &TestIndexReads::key(&identity, snapshot, b'V', &[&ids[0], b"body"])));
    let id = &manifest.samples[0].sample_id;
    let key = TestIndexReads::key(&identity, snapshot, b'V', &[id, b"body"]);
    let old = store.inner.get(&key).await.unwrap().unwrap();
    let mut bad: FieldLocator = serde_json::from_slice(&old.bytes).unwrap();
    let FieldLocator::Inline { value, .. } = &mut bad else {
        panic!("inline fixture")
    };
    value[0] = 0;
    store
        .inner
        .compare_exchange(&key, Some(&old.bytes), &serde_json::to_vec(&bad).unwrap())
        .await
        .unwrap();
    assert!(authority
        .resolve_inspection_batch_locators(
            &identity,
            snapshot,
            std::slice::from_ref(id),
            &["body".into()],
            &ReadCancellation::new()
        )
        .await
        .is_err());
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
}
