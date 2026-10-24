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

struct TestSelection {
    store: Arc<TestIndexReads>,
    authority: DatasetAuthority,
    identity: DatasetIdentity,
    parent: SnapshotId,
    child: SnapshotId,
    ids: Vec<Vec<u8>>,
}
async fn fixture() -> TestSelection {
    let store = Arc::new(TestIndexReads::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "selection-cost").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let mut manifest = ManifestRecord {
        version: 1,
        snapshot: SnapshotId::random(),
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "value".into(),
                required: false,
            }],
        },
        samples: (0..32u8)
            .map(|id| {
                let value = match id % 5 {
                    0 | 4 => Some(inline(b"old")),
                    1 => Some(inline(b"")),
                    2 => Some(FieldLocator::Chunk {
                        location: b"never-read".to_vec(),
                        length: 3,
                        md5: Md5::digest(b"old").into(),
                    }),
                    _ => None,
                };
                SampleRecord {
                    sample_id: vec![0, 255, id],
                    fields: value
                        .into_iter()
                        .map(|value| FieldRecord {
                            name: "value".into(),
                            value,
                        })
                        .collect(),
                }
            })
            .collect(),
    };
    let parent = authority
        .publish_snapshot(&identity, None, bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    for (id, sample) in manifest.samples.iter_mut().enumerate() {
        sample.fields = if id % 5 == 4 {
            vec![FieldRecord {
                name: "value".into(),
                value: FieldLocator::Tombstone,
            }]
        } else {
            vec![]
        };
    }
    let child = authority
        .publish_snapshot(&identity, Some(parent), bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    let ids = manifest
        .samples
        .into_iter()
        .map(|sample| sample.sample_id)
        .collect();
    store.begin();
    TestSelection {
        store,
        authority,
        identity,
        parent,
        child,
        ids,
    }
}
fn inline(value: &[u8]) -> FieldLocator {
    FieldLocator::Inline {
        value: value.to_vec(),
        md5: Md5::digest(value).into(),
    }
}
fn rule() -> Selection {
    Selection::Not(Box::new(Selection::FieldMatch {
        field: "value".into(),
        value: b"old".to_vec(),
    }))
}
fn budget() -> MetadataScanBudget {
    MetadataScanBudget {
        items: 100,
        bytes: 1024 * 1024,
        timeout: Duration::from_secs(2),
    }
}
fn assert_bounds(store: &TestIndexReads) {
    assert!((2..=16).contains(&store.peak_pending.load(Ordering::Relaxed)));
    assert!(store.peak_reserved.load(Ordering::Relaxed) <= 2 * 1024 * 1024);
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn selection_overlaps_effective_fields_without_reading_chunk_or_tombstone_values() {
    let f = fixture().await;
    let page = f
        .authority
        .inspection_selection_page(
            &f.identity,
            f.child,
            &rule(),
            None,
            budget(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    let expected: Vec<_> = f
        .ids
        .iter()
        .enumerate()
        .filter(|(id, _)| id % 5 != 0)
        .map(|(_, id)| id.clone())
        .collect();
    assert_eq!(page.entries, expected);
    assert!(page.after.is_none());
    assert_bounds(&f.store);
    let reads = f.store.reads.lock().unwrap();
    assert_eq!(reads.values().sum::<usize>(), 78);
    for snapshot in [f.parent, f.child] {
        assert_eq!(reads[&TestIndexReads::key(&f.identity, snapshot, b'H', &[])], 1);
    }
    for (id, sample) in f.ids.iter().enumerate() {
        let value = TestIndexReads::key(&f.identity, f.parent, b'V', &[sample, b"value"]);
        assert_eq!(reads.get(&value).copied().unwrap_or(0), usize::from(id % 5 <= 1));
        let child_value = TestIndexReads::key(&f.identity, f.child, b'V', &[sample, b"value"]);
        assert!(!reads.contains_key(&child_value));
    }
    assert_eq!(f.store.schema_scans.load(Ordering::Relaxed), 1);
    assert_eq!(f.store.member_scans.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn explicit_selection_validates_members_in_parallel_before_predicate_values() {
    let f = fixture().await;
    let explicit = Selection::And(vec![Selection::Explicit(f.ids.clone()), rule()]);
    let page = f
        .authority
        .inspection_selection_page(
            &f.identity,
            f.child,
            &explicit,
            None,
            budget(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 25);
    assert_eq!(f.store.reads.lock().unwrap().values().sum::<usize>(), 110);
    assert_bounds(&f.store);
    f.store.begin();
    let invalid = Selection::And(vec![Selection::Explicit(vec![b"missing".to_vec()]), rule()]);
    assert!(matches!(
        f.authority
            .inspection_selection_page(
                &f.identity,
                f.child,
                &invalid,
                None,
                budget(),
                &ReadCancellation::new()
            )
            .await,
        Err(InspectionError::Authority(AuthorityError::Invalid(
            DatasetError::SampleNotFound
        )))
    ));
    let offset = b"DS\0I\0".len() + f.identity.key().unwrap().len() + 16;
    assert!(!f
        .store
        .reads
        .lock()
        .unwrap()
        .keys()
        .any(|key| key.starts_with(b"DS\0I\0") && matches!(key[offset], b'F' | b'V')));
}

#[tokio::test]
async fn selection_enforces_aggregate_bytes_and_cancellation_drops_pending_reads() {
    let f = fixture().await;
    assert!(matches!(
        f.authority
            .inspection_selection_page(
                &f.identity,
                f.child,
                &rule(),
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
    assert!(f.store.total_bytes.load(Ordering::Relaxed) <= 4096);
    assert_eq!(f.store.pending.load(Ordering::Relaxed), 0);
    f.store.begin();
    f.store.stalled.store(true, Ordering::Relaxed);
    let cancel = ReadCancellation::new();
    let stop = async {
        while f.store.pending.load(Ordering::Relaxed) < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        cancel.cancel();
    };
    let predicate = rule();
    let read =
        f.authority
            .inspection_selection_page(&f.identity, f.child, &predicate, None, budget(), &cancel);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async { tokio::join!(read, stop) })
        .await
        .unwrap();
    assert!(result.is_err());
    assert_bounds(&f.store);
}
