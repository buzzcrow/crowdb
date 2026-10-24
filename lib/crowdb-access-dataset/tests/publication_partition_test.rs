// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
use crowdb_access_dataset::*;
use md5::{Digest, Md5};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
struct TestVersionedPartition {
    version: u16,
    payload: ManifestPartition,
}

#[tokio::test]
async fn publication_serializes_borrowed_partitions_with_the_established_wire_layout() {
    let store = Arc::new(dataset_store::TestDatasetStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "partition-layout").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let mut manifest = ManifestRecord {
        version: 1,
        snapshot: SnapshotId::random(),
        schema: SchemaRecord {
            version: 7,
            fields: vec![FieldDefinition {
                name: "body".into(),
                required: false,
            }],
        },
        samples: (0..2050u32)
            .rev()
            .map(|index| {
                let bytes = vec![u8::try_from(index % 256).unwrap(); index as usize % 67];
                SampleRecord {
                    sample_id: index.to_be_bytes().to_vec(),
                    fields: vec![FieldRecord {
                        name: "body".into(),
                        value: FieldLocator::Inline {
                            md5: Md5::digest(&bytes).into(),
                            value: bytes,
                        },
                    }],
                }
            })
            .collect(),
    };
    let operation = OperationId::random();
    let input = bincode::serialize(&manifest).unwrap();
    let snapshot = authority
        .prepare_snapshot_with_operation(&identity, None, input.clone(), operation)
        .await
        .unwrap();
    manifest.snapshot = snapshot;
    for expected in manifest.partition(1024).unwrap() {
        let key = manifest_partition(&identity, snapshot, expected.partition).unwrap();
        let stored = store.get(&key).await.unwrap().unwrap();
        let decoded: TestVersionedPartition = bincode::deserialize(&stored.bytes).unwrap();
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.payload, expected);
        let expected_bytes = bincode::serialize(&(1u16, &expected)).unwrap();
        assert_eq!(stored.bytes, expected_bytes);
    }
    assert_eq!(
        authority.get_manifest(&identity, snapshot).await.unwrap(),
        manifest
    );
    assert_eq!(
        authority
            .prepare_snapshot_with_operation(&identity, None, input, operation)
            .await
            .unwrap(),
        snapshot
    );
}
