// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_access_s3::integrity::SinglePartIntegrity;
use crowdb_access_s3::metadata::{BucketId, ChunkKvMetadataStore, MetadataKey, ObjectRecord, TenantId};
use crowdb_chunk_client::{ChunkAllocator, ChunkIoClient, DiskWriter, Result};
use crowdb_chunk_kv_client::{ChunkKvRpcTransport, ChunkKvRpcTransportConfig};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::chunkdb::rpc::*;
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::frame::{encode_frame, FrameMagic};
use crowdb_rpc_ffi::OwnedPayload;
use crowdb_sdk_native::runtime::RuntimeClients;
use crowdb_sdk_native::s3::S3Client;
use crowdb_sdk_native::{ClientRuntime, OperationContext, RuntimeLimits};

pub struct TestReadStorage {
    pub data: Vec<OwnedPayload>,
    pub reads: AtomicUsize,
}

fn chunk_id(index: usize) -> ChunkId {
    ChunkId {
        high: 1,
        low: index as u64 + 1,
    }
}

#[async_trait]
impl ChunkAllocator for TestReadStorage {
    async fn allocate_chunk(&self, _: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        panic!("read allocated")
    }
    async fn append_chunk(&self, _: AppendChunkRequest) -> Result<AppendChunkResponse> {
        panic!("read appended")
    }
    async fn seal_chunk(&self, _: SealChunkRequest) -> Result<SealChunkResponse> {
        panic!("read sealed")
    }
    async fn delete_chunk(&self, _: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        panic!("read deleted")
    }
    async fn update_chunk_strip(&self, _: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        panic!("read updated")
    }
    async fn query_chunk(&self, request: QueryChunkRequest) -> Result<QueryChunkResponse> {
        let id = request.chunk_id.unwrap();
        Ok(QueryChunkResponse {
            chunk: Some(Chunk {
                id: Some(id),
                state: ChunkState::Sealed as i32,
                capacity: 1,
                sealed_length: 1,
                strips: vec![ChunkStrip {
                    unit_kb: 1,
                    capacity: 1,
                    sealed_length: 1,
                    sealed_ts_ms: 1,
                    strip: Some(Strip::MirrorStrip(MirrorStrip {
                        segments: vec![Segment {
                            disk_id: Some(DiskId {
                                high: id.high,
                                low: id.low,
                            }),
                            unit_count: 1,
                            ..Segment::default()
                        }],
                    })),
                    ..ChunkStrip::default()
                }],
                ..Chunk::default()
            }),
            layout_validity_ms: 60_000,
        })
    }
}

#[async_trait]
impl DiskWriter for TestReadStorage {
    async fn write(&self, _: &Segment, _: u64, _: Bytes) -> Result<()> {
        panic!("read wrote")
    }
    async fn write_at_byte_offset(&self, _: &Segment, _: u64, _: u64, _: Bytes) -> Result<()> {
        panic!("read wrote")
    }
    async fn read_payload(
        &self,
        segment: &Segment,
        _: u64,
        offset: u64,
        length: u32,
    ) -> Result<OwnedPayload> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let index = usize::try_from(segment.disk_id.unwrap().low - 1).unwrap();
        let offset = usize::try_from(offset).unwrap();
        Ok(self.data[index].slice(offset..offset + length as usize))
    }
}

pub fn context(id: u64) -> OperationContext {
    OperationContext::new(
        NonZeroU64::new(id).unwrap(),
        Instant::now() + Duration::from_secs(1),
    )
}

pub struct TestFixture {
    pub runtime: ClientRuntime,
    pub client: S3Client,
    pub backend: Arc<TestReadStorage>,
    pub metadata: ChunkKvMetadataStore,
    pub tenant: TenantId,
    pub bucket: BucketId,
}

impl TestFixture {
    pub async fn new(held_bytes: usize) -> Self {
        let backend = Arc::new(TestReadStorage {
            data: [b"original".as_slice(), b"replacement".as_slice()]
                .into_iter()
                .enumerate()
                .map(|(index, payload)| {
                    let mut frame = Vec::with_capacity(4096);
                    frame.extend_from_slice(
                        &encode_frame(FrameMagic::RepoSmallV1, chunk_id(index), payload, 1).unwrap(),
                    );
                    OwnedPayload::from(frame)
                })
                .collect(),
            reads: AtomicUsize::new(0),
        });
        let metadata = super::metadata::client().await;
        let runtime = ClientRuntime::from_clients(
            RuntimeClients {
                control: Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new()))),
                metadata: metadata.clone(),
                metadata_transport: Arc::new(
                    ChunkKvRpcTransport::with_config(8, 1, 1, &ChunkKvRpcTransportConfig::default()).unwrap(),
                ),
                chunks: Arc::new(ChunkIoClient::from_parts(
                    backend.clone(),
                    backend.clone(),
                    ChunkType::S3,
                )),
            },
            RuntimeLimits {
                held_bytes,
                ..RuntimeLimits::default()
            },
        )
        .unwrap();
        let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
        let client = S3Client::new(runtime.clone(), tenant.clone()).unwrap();
        let bucket = client.create_bucket(context(1), b"bucket").await.unwrap();
        Self {
            runtime,
            client,
            backend,
            metadata: ChunkKvMetadataStore::new(metadata),
            tenant,
            bucket,
        }
    }

    pub fn record(&self, index: usize) -> ObjectRecord {
        let payload = [b"original".as_slice(), b"replacement".as_slice()][index];
        let mut integrity = SinglePartIntegrity::default();
        integrity.update(&Bytes::from_static(payload));
        let (etag, checksum) = integrity.finish();
        ObjectRecord {
            bucket_id: self.bucket,
            key: b"key".to_vec(),
            logical_length: payload.len() as u64,
            checksum,
            etag,
            created_at_ms: 1000,
            modified_at_ms: 2000,
            content_type: "application/octet-stream".into(),
            attributes: Vec::new(),
            data_reference: bincode::serialize(&vec![Location {
                chunk_id: Some(chunk_id(index)),
                length: self.backend.data[index].len() as u64,
                logical_length: payload.len() as u64,
                ..Location::default()
            }])
            .unwrap(),
            data_length: payload.len() as u64,
        }
    }

    pub async fn publish(&self, record: &ObjectRecord) -> u64 {
        self.metadata
            .put(
                MetadataKey::object(&self.tenant, self.bucket, &record.key).unwrap(),
                record.encode().unwrap(),
            )
            .await
            .unwrap()
    }
}
