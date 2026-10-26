// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogSource, ChunkKvTransport, ClientConfig, Result,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, ChunkKvResponse, Id128, KeyRange, OwnerDescriptor, PartitionArtifact,
    PointOperation, PointRequest, RequestRouting, RpcCompareCondition,
};
use crowdb_protocol::chunk_stream::StreamName;

#[derive(Default)]
pub struct TestCatalog {
    pub loads: AtomicUsize,
}

#[async_trait]
impl ChunkKvRangeCatalogSource for TestCatalog {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        let generation = self.loads.fetch_add(1, Ordering::Relaxed) as u64 + 1;
        let mut page = ChunkKvRangeCatalogPage {
            generation,
            page_index: 0,
            entries: vec![ChunkKvRangeCatalogEntry {
                partition_id: Id128 {
                    high: 1,
                    low: generation,
                },
                range: KeyRange {
                    start: Vec::new(),
                    end: None,
                },
                owner: OwnerDescriptor {
                    instance_id: generation,
                    rpc_endpoint: format!("owner-{generation}"),
                },
                owner_epoch: generation + 10,
                state: ChunkKvRangeCatalogPartitionState::Serving,
                artifact: PartitionArtifact {
                    tree_id: generation,
                    stream_name: StreamName { high: 4, low: 5 },
                    tail_overlay: None,
                },
                transition_id: None,
            }],
            checksum: [0; 32],
        };
        page.seal().unwrap();
        let mut head = ChunkKvRangeCatalogHead {
            generation,
            previous_generation: generation.checked_sub(1).filter(|value| *value != 0),
            pages: vec![ChunkKvRangeCatalogPageRef {
                page_generation: generation,
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
    expected: PointOperation,
    payloads: Vec<(usize, usize)>,
    replies: Mutex<VecDeque<Result<ChunkKvResponse>>>,
    pub calls: Mutex<Vec<(String, RequestRouting)>>,
    pub dropped_calls: Arc<AtomicUsize>,
}

impl TestTransport {
    pub fn new(operation: &PointOperation, replies: Vec<Result<ChunkKvResponse>>) -> Self {
        Self {
            expected: operation.clone(),
            payloads: payloads(operation),
            replies: Mutex::new(replies.into()),
            calls: Mutex::new(Vec::new()),
            dropped_calls: Arc::default(),
        }
    }
}

struct TestCallGuard(Arc<AtomicUsize>);
impl Drop for TestCallGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl ChunkKvTransport for TestTransport {
    async fn point(&self, endpoint: &str, request: &PointRequest) -> Result<ChunkKvResponse> {
        assert_eq!(request.operation, self.expected, "operation bytes changed");
        assert_eq!(
            payloads(&request.operation),
            self.payloads,
            "point payload was copied"
        );
        request.routing.validate().unwrap();
        self.calls
            .lock()
            .unwrap()
            .push((endpoint.into(), request.routing.clone()));
        let _guard = TestCallGuard(Arc::clone(&self.dropped_calls));
        let reply = self.replies.lock().unwrap().pop_front();
        if let Some(reply) = reply {
            reply
        } else {
            std::future::pending().await
        }
    }
}

fn payloads(operation: &PointOperation) -> Vec<(usize, usize)> {
    let mut buffers = vec![operation.key()];
    match operation {
        PointOperation::Put { value, .. } | PointOperation::PutIfAbsent { value, .. } => {
            buffers.push(value);
        }
        PointOperation::CompareExchange { condition, value, .. } => {
            if let RpcCompareCondition::Value(expected) = condition {
                buffers.push(expected);
            }
            buffers.push(value);
        }
        PointOperation::ConditionalDelete {
            condition: RpcCompareCondition::Value(expected),
            ..
        } => {
            buffers.push(expected);
        }
        _ => {}
    }
    buffers
        .into_iter()
        .filter(|bytes| !bytes.is_empty())
        .map(|bytes| (bytes.as_ptr() as usize, bytes.len()))
        .collect()
}

pub fn client(
    transport: Arc<TestTransport>,
    config: Option<ClientConfig>,
) -> (ChunkKvClient, Arc<TestCatalog>) {
    let catalog = Arc::new(TestCatalog::default());
    let config = config.unwrap_or_else(|| ClientConfig {
        operation_timeout: Duration::from_secs(2),
        retry_backoff: Duration::from_millis(1),
        max_attempts: 8,
        ..ClientConfig::default()
    });
    (
        ChunkKvClient::new(config, catalog.clone(), transport).unwrap(),
        catalog,
    )
}
