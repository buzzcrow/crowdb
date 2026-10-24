// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/dataset_digest_payload.rs"]
mod payload;
#[path = "common/storage.rs"]
mod storage;

use crowdb_access_dataset::{
    ChunkKvDatasetStore, DatasetAuthority, DatasetIdentity, DatasetStore, DatasetUploads, FieldDefinition,
    FieldLocator, FieldRecord, ManifestRecord, NamespacePath, OperationId, SampleRecord, SchemaRecord,
    SnapshotId,
};
use crowdb_sdk_native::{
    dataset::{BatchRequest, DatasetClient},
    Error, OperationContext,
};
use md5::{Digest, Md5};
use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

fn context(id: u64) -> OperationContext {
    OperationContext::new(
        NonZeroU64::new(id).unwrap(),
        Instant::now() + Duration::from_secs(5),
    )
}

#[tokio::test]
async fn completed_part_digest_only_certifies_the_identical_field_range() {
    for replace_part in [false, true] {
        let metadata = metadata::client().await;
        let runtime = storage::runtime_with_metadata(metadata.clone());
        let store = Arc::new(ChunkKvDatasetStore::new(metadata));
        let authority = Arc::new(DatasetAuthority::new(store.clone()));
        let payloads = Arc::new(payload::TestDigestPayload::default());
        let uploads = Arc::new(DatasetUploads::new(store.clone(), payloads.clone()));
        let identity = DatasetIdentity::new(NamespacePath::root(), "digest").unwrap();
        authority.create_dataset(identity.clone()).await.unwrap();
        let location = DatasetUploads::location(&identity, OperationId::random()).unwrap();
        let expected: [u8; 16] = Md5::digest(b"original").into();
        uploads.create(&location, 8, 8, expected, u64::MAX).await.unwrap();
        uploads
            .put_part(&location, 0, bytes::Bytes::from_static(b"original"), 1)
            .await
            .unwrap();
        uploads.complete(&location, 2).await.unwrap();
        let snapshot = publish(&authority, &identity, location.clone(), expected).await;
        if replace_part {
            payloads.bytes.store(Arc::new(b"replaced".to_vec()));
            let mut key = location;
            key.extend_from_slice(b"\0P\0\0\0\0");
            let old = store.get(&key).await.unwrap().unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
            value["part"]["md5"] = serde_json::json!(<[u8; 16]>::from(Md5::digest(b"replaced")));
            store
                .compare_exchange(&key, Some(&old.bytes), &serde_json::to_vec(&value).unwrap())
                .await
                .unwrap();
        }
        let client = DatasetClient::from_services_for_tests(runtime.clone(), identity, authority, uploads);
        let mut batch = client
            .read_batch(
                context(1),
                BatchRequest {
                    snapshot,
                    sample_ids: vec![b"one".to_vec()],
                    fields: vec!["value".into()],
                },
            )
            .await
            .unwrap();
        let held = batch.body.next().await.unwrap().unwrap().buffer;
        if replace_part {
            assert_eq!(held.as_slice(), b"replaced");
            assert!(matches!(batch.body.next().await, Err(Error::Integrity)));
            assert!(matches!(batch.body.next().await, Err(Error::Integrity)));
        } else {
            assert_eq!(held.as_slice(), b"original");
            assert!(batch.body.next().await.unwrap().is_none());
        }
        runtime.shutdown().await.unwrap();
        assert_eq!(held.as_slice().len(), 8);
    }
}

async fn publish(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    location: Vec<u8>,
    md5: [u8; 16],
) -> SnapshotId {
    let manifest = ManifestRecord {
        version: 1,
        snapshot: SnapshotId::random(),
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "value".into(),
                required: true,
            }],
        },
        samples: vec![SampleRecord {
            sample_id: b"one".to_vec(),
            fields: vec![FieldRecord {
                name: "value".into(),
                value: FieldLocator::Chunk {
                    location,
                    length: 8,
                    md5,
                },
            }],
        }],
    };
    authority
        .publish_snapshot(identity, None, bincode::serialize(&manifest).unwrap())
        .await
        .unwrap()
}
