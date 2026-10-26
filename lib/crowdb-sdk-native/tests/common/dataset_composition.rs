// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{composition::*, split::SplitPolicy, *};
use crowdb_sdk_native::dataset::DatasetClient;

pub async fn publish(client: &DatasetClient, location: Vec<u8>, digest: [u8; 16]) -> SnapshotId {
    let snapshot = SnapshotId::random();
    let input = CompositionRequest {
        parent: None,
        source: CompositionSource::All,
        schema: Some(SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "value".into(),
                required: true,
            }],
        }),
        samples: vec![SampleChange::Upsert(SampleRecord {
            sample_id: b"one".to_vec(),
            fields: vec![FieldRecord {
                name: "value".into(),
                value: FieldLocator::Chunk {
                    location,
                    length: 8,
                    md5: digest,
                },
            }],
        })],
        splits: vec![],
        policy: SplitPolicy::default(),
    };
    let prepared = client
        .prepare_snapshot(super::context(20), snapshot, &input)
        .await
        .unwrap();
    assert_eq!(prepared.snapshot, snapshot);
    assert_eq!(prepared.sample_count, 1);
    assert_eq!(
        client
            .prepare_snapshot(super::context(21), snapshot, &input)
            .await
            .unwrap(),
        prepared
    );
    client
        .update_snapshot_head(
            super::context(22),
            &SnapshotHeadUpdate {
                snapshot,
                expected_head: None,
            },
        )
        .await
        .unwrap();
    snapshot
}
