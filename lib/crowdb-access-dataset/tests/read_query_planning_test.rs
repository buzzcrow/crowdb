// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/access_store.rs"]
mod access_store;
#[path = "common/publication.rs"]
mod publication;
use crowdb_access_dataset::*;
use std::sync::{atomic::Ordering, Arc};

async fn fixture() -> (
    Arc<access_store::TestAccessStore>,
    Arc<DatasetAuthority>,
    DatasetIdentity,
) {
    let store = access_store::TestAccessStore::shared();
    let authority = Arc::new(DatasetAuthority::new(store.clone()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "planning-cost").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let mut manifest: ManifestRecord = bincode::deserialize(&publication::publication_manifest(1)).unwrap();
    let sample = manifest.samples[0].clone();
    manifest.samples = (0..81)
        .map(|i| SampleRecord {
            sample_id: format!("sample-{i:03}").into_bytes(),
            ..sample.clone()
        })
        .collect();
    authority
        .publish_snapshot(&identity, None, bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    (store, authority, identity)
}

fn spec(selection: Selection) -> ReadSpec {
    ReadSpec {
        snapshot: "latest".into(),
        selection,
        fields: vec!["marker".into()],
        batch_size: 7,
        partition: ReadPartition {
            worker: 0,
            workers: 1,
            rank: 0,
            world_size: 1,
        },
        shuffle: Some(ReadShuffle {
            seed: 42,
            group_size: 81,
        }),
        split: None,
    }
}

fn costs(store: &access_store::TestAccessStore) -> [u64; 5] {
    [
        &store.admission_reads,
        &store.bounded_reads,
        &store.schema_scans,
        &store.member_scans,
        &store.writes,
    ]
    .map(|counter| counter.load(Ordering::Relaxed))
}
fn delta(store: &access_store::TestAccessStore, before: [u64; 5]) -> [u64; 5] {
    let after = costs(store);
    std::array::from_fn(|i| after[i] - before[i])
}

#[tokio::test]
async fn multi_window_shuffle_prepares_once_and_new_requests_recheck() {
    let (store, authority, identity) = fixture().await;
    let reads = DatasetReads::new(authority.clone());
    let selection = Selection::Or(vec![
        Selection::Prefix(b"sample-".to_vec()),
        Selection::Explicit(vec![b"sample-000".to_vec(), b"sample-080".to_vec()]),
    ]);
    let bound = reads.bind(&identity, spec(selection.clone())).await.unwrap();
    let before = costs(&store);
    let mut after = None;
    let mut reference_ids = Vec::new();
    loop {
        let page = authority
            .inspection_selection_page(
                &identity,
                bound.snapshot,
                &selection,
                after,
                MetadataScanBudget {
                    items: 20,
                    bytes: 1024 * 1024,
                    timeout: std::time::Duration::from_secs(2),
                },
                &ReadCancellation::new(),
            )
            .await
            .unwrap();
        reference_ids.extend(page.entries);
        after = page.after;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(reference_ids.len(), 81);
    // Standalone page calls each perform their own admission and query preparation.
    assert_eq!(delta(&store, before), [5, 25, 5, 5, 0]);
    let before = costs(&store);
    let page = reads
        .next(&identity, &bound.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(page.ids.len(), 7);
    assert!(page.ids.iter().all(|id| reference_ids.contains(id)));
    // Three admission/projection reads + two explicit IDs, one schema and five member windows.
    assert_eq!(delta(&store, before), [1, 5, 1, 5, 0]);

    let restarted = DatasetReads::new(authority);
    let before = costs(&store);
    let replay = restarted
        .next(&identity, &bound.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(replay.ids, page.ids);
    assert_eq!(replay.next, page.next);
    assert_eq!(delta(&store, before), [1, 5, 1, 5, 0]);

    let before = costs(&store);
    let second = restarted
        .next(&identity, &page.next.unwrap(), &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(second.ids.len(), 7);
    assert!(second.ids.iter().all(|id| !page.ids.contains(id)));
    assert_eq!(delta(&store, before), [1, 5, 1, 5, 0]);
}

#[tokio::test]
async fn empty_filtered_windows_keep_advancing_without_replanning() {
    let (store, authority, identity) = fixture().await;
    let reads = DatasetReads::new(authority);
    let bound = reads
        .bind(
            &identity,
            spec(Selection::And(vec![
                Selection::Explicit(vec![b"sample-080".to_vec()]),
                Selection::FieldMatch {
                    field: "marker".into(),
                    value: vec![1],
                },
            ])),
        )
        .await
        .unwrap();
    let before = costs(&store);
    let page = reads
        .next(&identity, &bound.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(page.ids, vec![b"sample-080".to_vec()]);
    assert!(page.next.is_none());
    // One explicit ID plus metadata and verified inline value for each of 81 candidates.
    assert_eq!(delta(&store, before), [1, 4 + 2 * 81, 1, 5, 0]);
}

#[tokio::test]
async fn unknown_explicit_id_fails_before_any_member_window() {
    let (store, authority, identity) = fixture().await;
    let reads = DatasetReads::new(authority);
    let bound = reads
        .bind(&identity, spec(Selection::Explicit(vec![b"missing".to_vec()])))
        .await
        .unwrap();
    let before = costs(&store);
    assert!(matches!(
        reads
            .next(&identity, &bound.cursor, &ReadCancellation::new())
            .await,
        Err(AuthorityError::Invalid(DatasetError::SampleNotFound))
    ));
    assert_eq!(delta(&store, before), [1, 4, 1, 0, 0]);
}
