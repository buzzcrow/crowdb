// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_access_dataset::{
    ChunkKvDatasetStore, DatasetAuthority, DatasetIdentity, DatasetUploads, FieldDefinition, FieldLocator,
    FieldRecord, ManifestRecord, NamespacePath, OperationId, PayloadDurability, PayloadStorage, SampleRecord,
    SchemaRecord, SnapshotId, UploadError,
};
use crowdb_chunk_client::{ChunkIoClient, RoutedDiskWriter, SmallWriteIntent};
use crowdb_chunk_kv_client::{ChunkKvRpcTransport, ChunkKvRpcTransportConfig};
use crowdb_diskio_client::DiskioClient;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::{
    chunkdb::rpc::{ChunkType, Location},
    common::ChunkId,
};
use crowdb_sdk_native::{
    dataset::{BatchRequest, DatasetClient},
    runtime::RuntimeClients,
    ClientRuntime, OperationContext, RuntimeLimits,
};
use md5::{Digest, Md5};

use super::{metadata, storage::TestReadStorage};

// Only seeds metadata for the immutable frame already served by TestRpcReadServer.
// The Dataset facade below reads through the production ChunkPayloadStorage.
struct TestExistingFrame(usize);

#[async_trait]
impl PayloadStorage for TestExistingFrame {
    async fn write(
        &self,
        _: &[u8],
        data: Bytes,
        intent: Arc<dyn SmallWriteIntent>,
    ) -> Result<Vec<Location>, UploadError> {
        assert_eq!(data.as_ref(), b"original");
        let location = Location {
            chunk_id: Some(ChunkId { high: 1, low: 1 }),
            offset: 0,
            length: self.0 as u64,
            logical_offset: 0,
            logical_length: 8,
        };
        intent
            .before_write(&location)
            .await
            .map_err(|error| UploadError::Storage(error.to_string()))?;
        Ok(vec![location])
    }
    async fn read(&self, _: &[Location]) -> Result<Bytes, UploadError> {
        Ok(Bytes::from_static(b"original"))
    }
    async fn durability(&self, _: &[Location]) -> Result<PayloadDurability, UploadError> {
        Ok(PayloadDurability::Durable)
    }
    async fn discard(&self, _: &[Location]) -> Result<bool, UploadError> {
        panic!("read fixture discarded a frame")
    }
}

pub struct TestDatasetTransport {
    pub runtime: ClientRuntime,
    pub client: DatasetClient,
    pub gate: Arc<metadata::TestMutationGate>,
    pub snapshot: SnapshotId,
}

pub fn context() -> OperationContext {
    OperationContext::new(
        NonZeroU64::new(101).unwrap(),
        Instant::now() + Duration::from_secs(10),
    )
}

impl TestDatasetTransport {
    pub async fn new(backend: Arc<TestReadStorage>, disk: Arc<DiskioClient>) -> Self {
        let gate = Arc::new(metadata::TestMutationGate::default());
        let metadata = metadata::client_with_gate(gate.clone()).await;
        let store = Arc::new(ChunkKvDatasetStore::new(metadata.clone()));
        let authority = DatasetAuthority::new(store.clone());
        let runtime = ClientRuntime::adopt_clients(
            RuntimeClients {
                control: Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new()))),
                metadata,
                metadata_transport: Arc::new(
                    ChunkKvRpcTransport::with_config(8, 1, 1, &ChunkKvRpcTransportConfig::default()).unwrap(),
                ),
                chunks: Arc::new(ChunkIoClient::from_parts(
                    backend.clone(),
                    Arc::new(RoutedDiskWriter::from_client(disk)),
                    ChunkType::Dataset,
                )),
            },
            RuntimeLimits {
                requests: 1,
                held_bytes: 2 * 65_536,
                ..RuntimeLimits::default()
            },
        )
        .unwrap();
        let identity = DatasetIdentity::new(NamespacePath::root(), "transport-failure").unwrap();
        let client = DatasetClient::new(&runtime, identity.clone()).unwrap();
        client.create(context()).await.unwrap();
        let uploads = DatasetUploads::new(store, Arc::new(TestExistingFrame(backend.data[0].len())));
        let location = DatasetUploads::location(&identity, OperationId::random()).unwrap();
        let digest = Md5::digest(b"original").into();
        uploads.create(&location, 8, 8, digest, u64::MAX).await.unwrap();
        uploads
            .put_part(&location, 0, Bytes::from_static(b"original"), 1)
            .await
            .unwrap();
        uploads.complete(&location, 2).await.unwrap();
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
            samples: (1..=2)
                .map(|id| SampleRecord {
                    sample_id: vec![id],
                    fields: vec![FieldRecord {
                        name: "value".into(),
                        value: FieldLocator::Chunk {
                            location: location.clone(),
                            length: 8,
                            md5: digest,
                        },
                    }],
                })
                .collect(),
        };
        let snapshot = authority
            .publish_snapshot(&identity, None, bincode::serialize(&manifest).unwrap())
            .await
            .unwrap();
        Self {
            runtime,
            client,
            gate,
            snapshot,
        }
    }

    pub fn request(&self) -> BatchRequest {
        BatchRequest {
            snapshot: self.snapshot,
            sample_ids: vec![vec![1], vec![2]],
            fields: vec!["value".into()],
        }
    }
}

pub fn assert_failure(error: &crowdb_sdk_native::Error, cancelled: bool) {
    use crowdb_sdk_native::{dataset::FailureKind, Error};
    if cancelled {
        assert_eq!(*error, Error::Cancelled);
    } else {
        assert!(
            matches!(
                error,
                Error::Dataset {
                    kind: FailureKind::Storage,
                    ..
                }
            ),
            "{error:?}"
        );
    }
}

pub fn terminal_rpc_counts(
    disk: &DiskioClient,
    before: crowdb_diskio_client::DiskioStatus,
    accepted_is_terminal: bool,
) -> crowdb_diskio_client::DiskioStatus {
    let terminal = disk.status();
    if accepted_is_terminal {
        let mut expected = before;
        expected.read_operations += 1;
        assert_rpc_counts(terminal, expected);
    } else {
        // StripReader retries ordinary TCP target failures before the facade
        // receives its terminal error. Only accepted RDMA interruption forbids it.
        assert_eq!(terminal.read_operations, before.read_operations + 3);
        assert_eq!(terminal.retries, before.retries);
        eprintln!(
            "TCP terminal deltas: reads={} DiskIO retries={} connects={} reconnects={}",
            terminal.read_operations - before.read_operations,
            terminal.retries - before.retries,
            terminal.connect_attempts - before.connect_attempts,
            terminal.reconnect_attempts - before.reconnect_attempts,
        );
    }
    terminal
}

pub fn assert_rpc_counts(
    actual: crowdb_diskio_client::DiskioStatus,
    expected: crowdb_diskio_client::DiskioStatus,
) {
    assert_eq!(actual.read_operations, expected.read_operations);
    assert_eq!(actual.retries, expected.retries);
    assert_eq!(actual.connect_attempts, expected.connect_attempts);
    assert_eq!(actual.reconnect_attempts, expected.reconnect_attempts);
}
