// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{composition::*, *};
use crowdb_sdk_native::dataset::DatasetClient;

pub async fn publish(client: &DatasetClient, location: Vec<u8>, digest: [u8; 16]) -> SnapshotId {
    let operation = OperationId::random();
    client
        .begin_composition(
            super::context(20),
            operation,
            &CompositionInput {
                parent: None,
                source: CompositionSource::All,
                schema: Some(SchemaRecord {
                    version: 1,
                    fields: vec![FieldDefinition {
                        name: "value".into(),
                        required: true,
                    }],
                }),
                inherit_parent_splits: false,
                parent_split_revision: None,
                splits: vec![],
                policy: None,
            },
        )
        .await
        .unwrap();
    let edited = client
        .edit_composition(
            super::context(21),
            operation,
            &CompositionEdit {
                id: OperationId::random(),
                expected_revision: 1,
                schema: None,
                splits: None,
                policy: None,
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
            },
        )
        .await
        .unwrap();
    assert_eq!(edited.uploads.staged_uploads, 1);
    let snapshot = prepare(client, operation).await;
    client
        .publish_composition(
            super::context(24),
            operation,
            &CommitAttempt {
                version: 1,
                id: OperationId::random(),
                target: CommitTarget::Snapshot { snapshot },
                expected_head: None,
            },
        )
        .await
        .unwrap();
    let mut cleaned = false;
    for _ in 0..32 {
        if client
            .cleanup_composition(super::context(25), operation)
            .await
            .unwrap()
            .complete
        {
            cleaned = true;
            break;
        }
    }
    assert!(cleaned);
    snapshot
}

async fn prepare(client: &DatasetClient, operation: OperationId) -> SnapshotId {
    let mut evaluated = false;
    for page in 0..32 {
        if client
            .evaluate_composition(super::context(22), operation, 2, page)
            .await
            .unwrap()
            .complete
        {
            evaluated = true;
            break;
        }
    }
    assert!(evaluated);
    let mut snapshot = None;
    for _ in 0..32 {
        let status = client
            .prepare_composition(super::context(23), operation, 2)
            .await
            .unwrap();
        if status.uploads.publication_ready {
            snapshot = status.uploads.candidate;
            break;
        }
    }
    snapshot.expect("bounded preparation and ownership complete")
}
