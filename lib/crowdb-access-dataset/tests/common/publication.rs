// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{
    FieldDefinition, FieldLocator, FieldRecord, ManifestRecord, SampleRecord, SchemaRecord, SnapshotId,
};
use md5::{Digest, Md5};

fn md5_digest(payload: &[u8]) -> [u8; 16] {
    Md5::digest(payload).into()
}

pub(super) fn publication_manifest(marker: u8) -> Vec<u8> {
    let value = vec![marker];
    let manifest = ManifestRecord {
        version: 1,
        snapshot: SnapshotId::from_bytes(&[marker; 16]).unwrap(),
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "marker".into(),
                required: true,
            }],
        },
        samples: vec![SampleRecord {
            sample_id: format!("sample-{marker}").into_bytes(),
            fields: vec![FieldRecord {
                name: "marker".into(),
                value: FieldLocator::Inline {
                    md5: md5_digest(&value),
                    value,
                },
            }],
        }],
    };
    bincode::serialize(&manifest).unwrap()
}
