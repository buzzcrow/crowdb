// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{inspection::FieldMetadata, *};
use crowdb_sdk_native::{
    dataset::{BatchRequest, DatasetClient, ReadBatch},
    ClientRuntime, OperationContext,
};
use md5::{Digest, Md5};
use std::{
    num::NonZeroU64,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use super::{metadata, storage};

pub struct TestInlineFixture {
    client: DatasetClient,
    pub runtime: ClientRuntime,
    store: Arc<ChunkKvDatasetStore>,
    stats: Arc<metadata::TestMutationGate>,
    snapshot: SnapshotId,
    pub data: Vec<u8>,
}

pub fn context(id: u64) -> OperationContext {
    OperationContext::new(
        NonZeroU64::new(id).unwrap(),
        Instant::now() + Duration::from_secs(10),
    )
}

impl TestInlineFixture {
    pub async fn new(length: usize) -> Self {
        let stats = Arc::new(metadata::TestMutationGate::default());
        let metadata = metadata::client_with_gate(stats.clone()).await;
        let store = Arc::new(ChunkKvDatasetStore::new(metadata.clone()));
        let authority = DatasetAuthority::new(store.clone());
        let runtime = storage::runtime_with_metadata(metadata);
        let identity = DatasetIdentity::new(NamespacePath::root(), "inline-digest").unwrap();
        authority.create_dataset(identity.clone()).await.unwrap();
        let data = vec![93; length];
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
                    value: FieldLocator::Inline {
                        value: data.clone(),
                        md5: Md5::digest(&data).into(),
                    },
                }],
            }],
        };
        let snapshot = authority
            .prepare_snapshot_candidate(
                &identity,
                None,
                bincode::serialize(&manifest).unwrap(),
                OperationId::random(),
            )
            .await
            .unwrap();
        Self {
            client: DatasetClient::new(&runtime, identity).unwrap(),
            runtime,
            store,
            stats,
            snapshot,
            data,
        }
    }

    pub async fn read(&self, query: bool, request: OperationContext) -> crowdb_sdk_native::Result<ReadBatch> {
        if query {
            let binding = self
                .client
                .bind_read(
                    context(1),
                    ReadSpec {
                        snapshot: self.snapshot.to_string(),
                        selection: Selection::Explicit(vec![b"one".to_vec()]),
                        fields: vec!["value".into()],
                        batch_size: 1,
                        partition: ReadPartition {
                            worker: 0,
                            workers: 1,
                            rank: 0,
                            world_size: 1,
                        },
                        shuffle: None,
                        split: None,
                    },
                )
                .await?;
            self.client.next_batch(request, &binding.cursor).await
        } else {
            self.client
                .read_batch(
                    request,
                    BatchRequest {
                        snapshot: self.snapshot,
                        sample_ids: vec![b"one".to_vec()],
                        fields: vec!["value".into()],
                    },
                )
                .await
        }
    }

    pub fn counts(&self) -> (usize, usize, usize) {
        (
            self.stats.reads.load(Ordering::Relaxed),
            self.stats.bounded_reads.load(Ordering::Relaxed),
            self.stats.writes.load(Ordering::Relaxed),
        )
    }

    fn key(&self, kind: u8) -> Vec<u8> {
        let mut key = b"DS\0I\0".to_vec();
        key.extend(self.client.identity().key().unwrap());
        key.extend(self.snapshot.as_bytes());
        key.push(kind);
        key.extend_from_slice(b"one\0\0value\0\0");
        key
    }

    pub async fn corrupt(&self, fault: u8) {
        let key = self.key(if fault == 0 { b'V' } else { b'F' });
        let old = self.store.get(&key).await.unwrap().unwrap();
        let bytes = if fault == 0 {
            let mut locator: FieldLocator = serde_json::from_slice(&old.bytes).unwrap();
            if let FieldLocator::Inline { value, md5 } = &mut locator {
                if value.is_empty() {
                    md5[0] ^= 1;
                    let field_key = self.key(b'F');
                    let previous = self.store.get(&field_key).await.unwrap().unwrap();
                    let mut field: FieldMetadata = serde_json::from_slice(&previous.bytes).unwrap();
                    field.md5 = Some(*md5);
                    self.store
                        .compare_exchange(
                            &field_key,
                            Some(&previous.bytes),
                            &serde_json::to_vec(&field).unwrap(),
                        )
                        .await
                        .unwrap();
                } else {
                    value[0] ^= 1;
                }
            }
            serde_json::to_vec(&locator).unwrap()
        } else {
            let mut field: FieldMetadata = serde_json::from_slice(&old.bytes).unwrap();
            match fault {
                1 => field.length += 1,
                2 => field.md5.as_mut().unwrap()[0] ^= 1,
                _ => field.storage = inspection::FieldStorageKind::Chunk,
            }
            serde_json::to_vec(&field).unwrap()
        };
        assert!(matches!(
            self.store
                .compare_exchange(&key, Some(&old.bytes), &bytes)
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
    }
}
