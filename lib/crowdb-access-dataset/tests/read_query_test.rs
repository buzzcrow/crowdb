// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod dataset_store;
#[path = "common/publication.rs"]
mod publication;
use crowdb_access_dataset::{
    DatasetAuthority, DatasetIdentity, DatasetReads, NamespacePath, ReadCancellation, ReadPartition,
    ReadSpec, Selection,
};
use dataset_store::TestStore;
use std::sync::Arc;
fn spec() -> ReadSpec {
    ReadSpec {
        snapshot: "latest".into(),
        selection: Selection::Prefix(b"sample-".to_vec()),
        fields: vec!["marker".into()],
        batch_size: 1,
        partition: ReadPartition {
            worker: 0,
            workers: 1,
            rank: 0,
            world_size: 1,
        },
        shuffle: None,
        split: None,
    }
}
#[tokio::test]
async fn continuation_replays_and_rejects_cross_dataset_scope() {
    let authority = Arc::new(DatasetAuthority::new(Arc::new(TestStore::default())));
    let identity = DatasetIdentity::new(NamespacePath::root(), "queries").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, publication::publication_manifest(1))
        .await
        .unwrap();
    let reads = DatasetReads::new(authority.clone());
    let binding = reads.bind(&identity, spec()).await.unwrap();
    authority
        .publish_snapshot(&identity, Some(snapshot), publication::publication_manifest(2))
        .await
        .unwrap();
    let page = reads
        .next(&identity, &binding.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(page.snapshot, snapshot);
    let restarted = DatasetReads::new(authority);
    let replay = restarted
        .next(&identity, &binding.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(page.ids, replay.ids);
    assert_eq!(page.next, replay.next);
    let other = DatasetIdentity::new(NamespacePath::root(), "other").unwrap();
    assert!(restarted
        .next(&other, &binding.cursor, &ReadCancellation::new())
        .await
        .is_err());
    assert!(restarted
        .next(&identity, "invalid", &ReadCancellation::new())
        .await
        .is_err());
}

#[tokio::test]
async fn continuation_stays_small_without_copying_selected_ids() {
    use crowdb_access_dataset::{
        DatasetStore, FieldDefinition, FieldLocator, FieldRecord, ManifestRecord, SampleRecord, SchemaRecord,
        SnapshotId,
    };
    use md5::{Digest, Md5};
    let store = Arc::new(TestStore::default());
    let authority = Arc::new(DatasetAuthority::new(store.clone()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "paged").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let manifest = ManifestRecord {
        version: 1,
        snapshot: SnapshotId::random(),
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "marker".into(),
                required: true,
            }],
        },
        samples: (0..3000)
            .map(|i| SampleRecord {
                sample_id: format!("sample-{i:04}-{}", "x".repeat(200)).into_bytes(),
                fields: vec![FieldRecord {
                    name: "marker".into(),
                    value: FieldLocator::Inline {
                        value: vec![1],
                        md5: Md5::digest([1]).into(),
                    },
                }],
            })
            .collect(),
    };
    authority
        .publish_snapshot(&identity, None, bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    let reads = DatasetReads::new(authority);
    let opened = reads.bind(&identity, spec()).await.unwrap();
    assert!(opened.cursor.len() < 4096);
    let batch = reads
        .next(&identity, &opened.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(batch.ids.len(), 1);
    let next = batch.next.unwrap();
    assert!(next.len() < 4096);
    assert_eq!(
        reads
            .next(&identity, &next, &ReadCancellation::new())
            .await
            .unwrap()
            .ids[0],
        manifest.samples[1].sample_id
    );
    assert!(store
        .scan_page(
            &crowdb_access_dataset::MetadataScan {
                prefix: b"DS\0RS\0".to_vec(),
                after: None,
                budget: crowdb_access_dataset::MetadataScanBudget {
                    items: 20,
                    bytes: 1024 * 1024,
                    timeout: std::time::Duration::from_secs(2)
                }
            },
            &ReadCancellation::new()
        )
        .await
        .unwrap()
        .entries
        .is_empty());
}
