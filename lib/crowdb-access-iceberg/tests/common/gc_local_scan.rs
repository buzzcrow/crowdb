// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogSource, ChunkKvTransport, ClientConfig, Result,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, ChunkKvResponse, Id128, KeyRange, OperationResult, OwnerDescriptor,
    PartitionArtifact, PointRequest, RpcValue, ScanContinuation, ScanRequest,
};
use crowdb_protocol::chunk_stream::StreamName;
use std::sync::Arc;

struct TestCatalog;
#[async_trait]
impl ChunkKvRangeCatalogSource for TestCatalog {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        let mut page = ChunkKvRangeCatalogPage {
            generation: 7,
            page_index: 0,
            entries: vec![ChunkKvRangeCatalogEntry {
                partition_id: Id128 { high: 7, low: 1 },
                range: KeyRange {
                    start: Vec::new(),
                    end: None,
                },
                owner: OwnerDescriptor {
                    instance_id: 1,
                    rpc_endpoint: "owner".into(),
                },
                owner_epoch: 7,
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
            generation: 7,
            previous_generation: None,
            pages: vec![ChunkKvRangeCatalogPageRef {
                page_generation: 7,
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
    rows: Vec<Vec<u8>>,
    pub requests: ArcSwap<Vec<ScanRequest>>,
}
impl TestTransport {
    pub fn new(rows: Vec<Vec<u8>>) -> Self {
        Self {
            rows,
            requests: ArcSwap::from_pointee(Vec::new()),
        }
    }
}
#[async_trait]
impl ChunkKvTransport for TestTransport {
    async fn point(&self, _: &str, _: &PointRequest) -> Result<ChunkKvResponse> {
        panic!("unexpected point");
    }
    async fn scan(&self, _: &str, request: &ScanRequest) -> Result<ChunkKvResponse> {
        self.requests.rcu(|requests| {
            let mut next = (**requests).clone();
            next.push(request.clone());
            next
        });
        let mut rows: Vec<_> = self
            .rows
            .iter()
            .filter(|key| {
                request.start.as_ref().map_or(true, |start| *key >= start)
                    && request.end.as_ref().map_or(true, |end| *key < end)
                    && request
                        .continuation
                        .as_ref()
                        .map_or(true, |token| *key > &token.last_key)
            })
            .cloned()
            .collect();
        let more = rows.len() > request.limit as usize;
        rows.truncate(request.limit as usize);
        let continuation = more.then(|| ScanContinuation {
            direction: request.direction,
            last_key: rows.last().unwrap().clone(),
            partition_id: request.routing.partition_id,
            owner_epoch: request.routing.owner_epoch,
            map_revision: request.routing.map_revision,
        });
        Ok(ChunkKvResponse {
            map_revision: 7,
            journal_position: None,
            result: Ok(OperationResult::Scan {
                items: rows
                    .into_iter()
                    .map(|key| RpcValue {
                        key,
                        value: vec![7],
                        revision: 1,
                    })
                    .collect(),
                continuation,
            }),
        })
    }
}
pub fn client(transport: Arc<TestTransport>) -> Arc<ChunkKvClient> {
    Arc::new(ChunkKvClient::new(ClientConfig::default(), Arc::new(TestCatalog), transport).unwrap())
}
