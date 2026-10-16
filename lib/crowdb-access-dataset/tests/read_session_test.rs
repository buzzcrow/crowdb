// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod dataset_store;
#[path = "common/publication.rs"]
mod publication;
use crowdb_access_dataset::{
    DatasetAuthority, DatasetIdentity, DatasetSessions, NamespacePath, OperationId, Selection,
    SessionPartition, SessionSpec,
};
use dataset_store::TestStore;
use std::sync::Arc;
fn spec() -> SessionSpec {
    SessionSpec {
        id: OperationId::random(),
        snapshot: "latest".into(),
        selection: Selection::Prefix(b"sample-".to_vec()),
        fields: vec!["marker".into()],
        batch_size: 1,
        partition: SessionPartition {
            worker: 0,
            workers: 1,
            rank: 0,
            world_size: 1,
        },
        shuffle: None,
    }
}
#[tokio::test]
async fn fetch_ack_resume_and_close_are_scoped_and_snapshot_bound() {
    let authority = Arc::new(DatasetAuthority::new(Arc::new(TestStore::default())));
    let identity = DatasetIdentity::new(NamespacePath::root(), "sessions").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let first = authority
        .publish_snapshot(&identity, None, publication::publication_manifest(1))
        .await
        .unwrap();
    let sessions = DatasetSessions::new(authority.clone());
    let plan = spec();
    let other = spec();
    let opened = sessions.start(&identity, plan.clone(), 10).await.unwrap();
    sessions.start(&identity, other.clone(), 10).await.unwrap();
    authority
        .publish_snapshot(&identity, Some(first), publication::publication_manifest(2))
        .await
        .unwrap();
    let batch = sessions.next(&identity, plan.id, 11).await.unwrap();
    assert_eq!(batch.snapshot, first);
    assert_eq!(
        sessions.next(&identity, plan.id, 12).await.unwrap().next,
        batch.next
    );
    assert_eq!(
        sessions
            .resume(&identity, plan.id, 13)
            .await
            .unwrap()
            .acknowledged,
        opened.acknowledged
    );
    assert!(sessions
        .acknowledge(&identity, other.id, &opened.acknowledged, &batch.next, 14)
        .await
        .is_err());
    assert!(sessions
        .acknowledge(&identity, plan.id, &opened.acknowledged, "forged", 14)
        .await
        .is_err());
    let progress = sessions
        .acknowledge(&identity, plan.id, &opened.acknowledged, &batch.next, 14)
        .await
        .unwrap();
    assert_eq!(
        sessions
            .acknowledge(&identity, plan.id, &opened.acknowledged, &batch.next, 15)
            .await
            .unwrap()
            .acknowledged,
        progress.acknowledged
    );
    assert!(sessions.next(&identity, plan.id, 100).await.is_err());
    let restarted = DatasetSessions::new(authority);
    assert_eq!(
        restarted
            .resume(&identity, plan.id, 100)
            .await
            .unwrap()
            .acknowledged,
        progress.acknowledged
    );
    restarted.close(&identity, plan.id).await.unwrap();
    assert!(restarted.resume(&identity, plan.id, 101).await.is_err());
    restarted.resume(&identity, other.id, 101).await.unwrap();
    assert_eq!(
        restarted.next(&identity, other.id, 102).await.unwrap().snapshot,
        first
    );
}

#[tokio::test]
async fn progress_record_stays_small_when_selection_spans_id_pages() {
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
    let sessions = DatasetSessions::new(authority);
    let plan = spec();
    let opened = sessions.start(&identity, plan.clone(), 10).await.unwrap();
    let mut key = b"DS\0RS\0".to_vec();
    key.extend(identity.key().unwrap());
    key.extend(plan.id.as_bytes());
    assert!(store.get(&key).await.unwrap().unwrap().bytes.len() < 1024);
    let batch = sessions.next(&identity, plan.id, 11).await.unwrap();
    assert_eq!(batch.ids.len(), 1);
    sessions
        .acknowledge(&identity, plan.id, &opened.acknowledged, &batch.next, 12)
        .await
        .unwrap();
    assert!(store.get(&key).await.unwrap().unwrap().bytes.len() < 1024);
    assert_eq!(
        sessions.next(&identity, plan.id, 13).await.unwrap().ids[0],
        manifest.samples[1].sample_id
    );
}
