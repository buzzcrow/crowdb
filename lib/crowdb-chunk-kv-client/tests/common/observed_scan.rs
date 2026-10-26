// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogSource, ChunkKvTransport, ClientConfig, ClientError, Result,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, ChunkKvResponse, Id128, KeyRange, OperationResult, OwnerDescriptor,
    PartitionArtifact, PointRequest, RpcValue, ScanContinuation, ScanRequest,
};
use crowdb_protocol::chunk_stream::StreamName;

struct TestCatalog;

#[async_trait]
impl ChunkKvRangeCatalogSource for TestCatalog {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        let mut page = ChunkKvRangeCatalogPage {
            generation: 1,
            page_index: 0,
            entries: vec![ChunkKvRangeCatalogEntry {
                partition_id: Id128 { high: 1, low: 1 },
                range: KeyRange {
                    start: Vec::new(),
                    end: None,
                },
                owner: OwnerDescriptor {
                    instance_id: 1,
                    rpc_endpoint: "owner".into(),
                },
                owner_epoch: 1,
                state: ChunkKvRangeCatalogPartitionState::Serving,
                artifact: PartitionArtifact {
                    tree_id: 1,
                    stream_name: StreamName { high: 1, low: 1 },
                    tail_overlay: None,
                },
                transition_id: None,
            }],
            checksum: [0; 32],
        };
        page.seal().unwrap();
        let mut head = ChunkKvRangeCatalogHead {
            generation: 1,
            previous_generation: None,
            pages: vec![ChunkKvRangeCatalogPageRef {
                page_generation: 1,
                page_index: 0,
                first_key: Vec::new(),
                page_checksum: page.checksum,
            }],
            checksum: [0; 32],
        };
        head.seal().unwrap();
        Ok((head, vec![page]))
    }
}

pub struct TestTransport {
    pub items: Vec<RpcValue>,
    pub continuation_key: Option<Vec<u8>>,
    pub failure: Option<ClientError>,
    pub calls: AtomicUsize,
    pub bounded_calls: AtomicUsize,
}

impl TestTransport {
    pub fn row(key: &[u8], value_bytes: usize) -> Self {
        Self {
            items: vec![RpcValue {
                key: key.to_vec(),
                value: vec![7; value_bytes],
                revision: 1,
            }],
            continuation_key: None,
            failure: None,
            calls: AtomicUsize::new(0),
            bounded_calls: AtomicUsize::new(0),
        }
    }

    fn response(&self, request: &ScanRequest) -> Result<ChunkKvResponse> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        Ok(ChunkKvResponse {
            map_revision: 1,
            journal_position: None,
            result: Ok(OperationResult::Scan {
                items: self.items.clone(),
                continuation: self.continuation_key.as_ref().map(|key| ScanContinuation {
                    direction: request.direction,
                    last_key: key.clone(),
                    partition_id: request.routing.partition_id,
                    owner_epoch: request.routing.owner_epoch,
                    map_revision: request.routing.map_revision,
                }),
            }),
        })
    }
}

#[async_trait]
impl ChunkKvTransport for TestTransport {
    async fn point(&self, _endpoint: &str, _request: &PointRequest) -> Result<ChunkKvResponse> {
        panic!("unexpected point RPC");
    }

    async fn scan(&self, _endpoint: &str, request: &ScanRequest) -> Result<ChunkKvResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.response(request)
    }

    async fn scan_bounded(
        &self,
        _endpoint: &str,
        request: &ScanRequest,
        max_bytes: usize,
    ) -> Result<ChunkKvResponse> {
        self.bounded_calls.fetch_add(1, Ordering::SeqCst);
        if self
            .items
            .first()
            .is_some_and(|row| row.key.len() + row.value.len() > max_bytes)
        {
            return Err(ClientError::TooLarge);
        }
        self.response(request)
    }
}

pub fn client(transport: Arc<TestTransport>, max_response_bytes: usize) -> ChunkKvClient {
    ChunkKvClient::new(
        ClientConfig {
            max_response_bytes,
            max_attempts: 1,
            ..ClientConfig::default()
        },
        Arc::new(TestCatalog),
        transport,
    )
    .unwrap()
}
