// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crate::inspection::{fixture, publish};
use crowdb_access_dataset::*;
use md5::{Digest, Md5};
use std::sync::atomic::Ordering;
fn inline(bytes: &[u8]) -> FieldLocator {
    FieldLocator::Inline {
        value: bytes.to_vec(),
        md5: Md5::digest(bytes).into(),
    }
}
pub(super) async fn effective_fixture() -> (DatasetAuthority, DatasetIdentity, SnapshotId) {
    let (store, authority, identity) = fixture().await;
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
        samples: (b'a'..=b'e')
            .map(|id| SampleRecord {
                sample_id: vec![id],
                fields: vec![FieldRecord {
                    name: "value".into(),
                    value: inline(b"old"),
                }],
            })
            .collect(),
    };
    manifest.samples[1].fields[0].value = inline(b"");
    manifest.samples[2].fields[0].value = FieldLocator::Chunk {
        location: b"never-read".to_vec(),
        length: 3,
        md5: Md5::digest(b"old").into(),
    };
    manifest.samples[3].fields.clear();
    let parent = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            bincode::serialize(&manifest).unwrap(),
            OperationId::random(),
        )
        .await
        .unwrap();
    publish(&authority, &identity, parent).await;
    for sample in &mut manifest.samples {
        sample.fields.clear();
    }
    manifest.samples[4].fields.push(FieldRecord {
        name: "value".into(),
        value: FieldLocator::Tombstone,
    });
    let child = authority
        .prepare_snapshot_candidate(
            &identity,
            Some(parent),
            bincode::serialize(&manifest).unwrap(),
            OperationId::random(),
        )
        .await
        .unwrap();
    publish(&authority, &identity, child).await;
    store.inspecting.store(true, Ordering::Relaxed);
    (authority, identity, child)
}
