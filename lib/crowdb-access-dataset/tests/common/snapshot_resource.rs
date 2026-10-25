// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::*;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct TestTimedSnapshot {
    pub publication: SnapshotPublication,
    pub gc_after: u64,
}

pub fn snapshot_key(identity: &DatasetIdentity, snapshot: SnapshotId) -> Vec<u8> {
    [
        b"DS\0S\0".as_slice(),
        &identity.key().unwrap(),
        snapshot.as_bytes(),
    ]
    .concat()
}

pub async fn deadline(store: &dyn DatasetStore, identity: &DatasetIdentity, snapshot: SnapshotId) -> u64 {
    let value = store
        .get(&snapshot_key(identity, snapshot))
        .await
        .unwrap()
        .unwrap();
    let (version, record): (u16, TestTimedSnapshot) = bincode::deserialize(&value.bytes).unwrap();
    assert_eq!(version, 2);
    assert_eq!(record.publication.snapshot, snapshot);
    record.gc_after
}

pub async fn downgrade(store: &dyn DatasetStore, identity: &DatasetIdentity, snapshot: SnapshotId) {
    let key = snapshot_key(identity, snapshot);
    let value = store.get(&key).await.unwrap().unwrap();
    let (version, record): (u16, TestTimedSnapshot) = bincode::deserialize(&value.bytes).unwrap();
    assert_eq!(version, 2);
    let bytes = bincode::serialize(&(
        1_u16,
        SnapshotRecord {
            publication: record.publication,
        },
    ))
    .unwrap();
    assert!(matches!(
        store
            .compare_exchange(&key, Some(&value.bytes), &bytes)
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
}

pub async fn mutate(
    store: &dyn DatasetStore,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    update: impl FnOnce(&mut SnapshotPublication),
) {
    let key = snapshot_key(identity, snapshot);
    let value = store.get(&key).await.unwrap().unwrap();
    let bytes = if value.bytes[..2] == 2_u16.to_le_bytes() {
        let (version, mut resource): (u16, TestTimedSnapshot) = bincode::deserialize(&value.bytes).unwrap();
        update(&mut resource.publication);
        bincode::serialize(&(version, resource)).unwrap()
    } else {
        let (version, mut resource): (u16, SnapshotRecord) = bincode::deserialize(&value.bytes).unwrap();
        assert_eq!(version, 1);
        update(&mut resource.publication);
        bincode::serialize(&(version, resource)).unwrap()
    };
    assert!(matches!(
        store
            .compare_exchange(&key, Some(&value.bytes), &bytes)
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
}

pub async fn put_large(store: &dyn DatasetStore, identity: &DatasetIdentity, marker: u8, gc_after: u64) {
    let snapshot = SnapshotId::from_bytes(&[marker; 16]).unwrap();
    let payload = vec![marker; 64 * 1024];
    let field = FieldRecord {
        name: "marker".into(),
        value: FieldLocator::Inline {
            md5: Md5::digest(&payload).into(),
            value: payload,
        },
    };
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "marker".into(),
                required: true,
            }],
        },
        samples: (0_u32..80)
            .map(|index| SampleRecord {
                sample_id: index.to_be_bytes().to_vec(),
                fields: vec![field.clone()],
            })
            .collect(),
    };
    manifest.validate().unwrap();
    let resource = TestTimedSnapshot {
        publication: SnapshotPublication {
            snapshot,
            parent: None,
            operation: OperationId::from_bytes(snapshot.as_bytes()).unwrap(),
            manifest: bincode::serialize(&manifest).unwrap(),
            state: PublicationState::Prepared,
        },
        gc_after,
    };
    let bytes = bincode::serialize(&(2_u16, resource)).unwrap();
    assert!(bytes.len() > 5 * 1024 * 1024);
    assert!(matches!(
        store
            .compare_exchange(&snapshot_key(identity, snapshot), None, &bytes)
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
}
