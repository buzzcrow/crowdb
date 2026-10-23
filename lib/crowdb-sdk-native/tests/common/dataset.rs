// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{
    ChunkKvDatasetStore, DatasetAuthority, DatasetIdentity, FieldDefinition, FieldLocator, FieldRecord,
    ManifestRecord, NamespacePath, SampleRecord, SchemaRecord, SnapshotId,
};
use crowdb_sdk_native::{dataset::DatasetClient, ClientRuntime, OperationContext, RuntimeLimits};
use md5::{Digest, Md5};
use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

pub struct TestFixture {
    pub runtime: ClientRuntime,
    pub client: DatasetClient,
    pub snapshot: SnapshotId,
}

pub fn context(id: u64) -> OperationContext {
    OperationContext::new(
        NonZeroU64::new(id).unwrap(),
        Instant::now() + Duration::from_secs(10),
    )
}

impl TestFixture {
    pub async fn new(held: usize) -> Self {
        let metadata = super::metadata::client().await;
        let authority = DatasetAuthority::new(Arc::new(ChunkKvDatasetStore::new(metadata.clone())));
        let runtime = super::storage::runtime_with_limits(
            metadata,
            RuntimeLimits {
                held_bytes: held,
                ..RuntimeLimits::default()
            },
        );
        let identity = DatasetIdentity::new(NamespacePath::root(), "native").unwrap();
        let client = DatasetClient::new(&runtime, identity.clone()).unwrap();
        client.create(context(1)).await.unwrap();
        let snapshot = authority
            .publish_snapshot(&identity, None, manifest())
            .await
            .unwrap();
        Self {
            runtime,
            client,
            snapshot,
        }
    }
}

fn manifest() -> Vec<u8> {
    let manifest = ManifestRecord {
        version: 1,
        snapshot: SnapshotId::random(),
        schema: SchemaRecord {
            version: 1,
            fields: vec![
                FieldDefinition {
                    name: "value".into(),
                    required: true,
                },
                FieldDefinition {
                    name: "optional".into(),
                    required: false,
                },
            ],
        },
        samples: (0..3)
            .map(|index| {
                let value = vec![index; 16];
                SampleRecord {
                    sample_id: vec![index + 1],
                    fields: vec![FieldRecord {
                        name: "value".into(),
                        value: FieldLocator::Inline {
                            md5: Md5::digest(&value).into(),
                            value,
                        },
                    }],
                }
            })
            .collect(),
    };
    bincode::serialize(&manifest).unwrap()
}
