// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogSource, ChunkKvTransport, ClientConfig, Result,
};
use crowdb_protocol::chunk_kv::*;
use crowdb_protocol::chunk_stream::StreamName;
use tokio::sync::{mpsc, oneshot};

struct TestCatalog(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>);

#[async_trait]
impl ChunkKvRangeCatalogSource for TestCatalog {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        Ok((self.0.clone(), self.1.clone()))
    }
}

enum TestCommand {
    Point(PointOperation, oneshot::Sender<ChunkKvResponse>),
    Scan(ScanRequest, oneshot::Sender<ChunkKvResponse>),
}

pub struct TestMetadataTransport {
    sender: mpsc::Sender<TestCommand>,
    lose_mutation_reply: Arc<AtomicBool>,
    blackout: Option<Arc<TestMutationBlackout>>,
    route: Option<Arc<TestRouteChange>>,
}

#[derive(Default)]
pub struct TestRouteChange {
    pub generation: std::sync::atomic::AtomicU64,
    pub rejected: std::sync::atomic::AtomicU64,
    pub accepted: std::sync::atomic::AtomicU64,
}

#[derive(Default)]
pub struct TestMutationBlackout {
    pub key: arc_swap::ArcSwapOption<Vec<u8>>,
    pub blocked: AtomicBool,
}

#[async_trait]
impl ChunkKvTransport for TestMetadataTransport {
    async fn point(&self, endpoint: &str, request: &PointRequest) -> Result<ChunkKvResponse> {
        if let Some(route) = &self.route {
            let generation = route.generation.load(Ordering::Acquire);
            if endpoint != format!("test-owner-{generation}") || request.routing.owner_epoch != generation {
                route.rejected.fetch_add(1, Ordering::Relaxed);
                return Ok(ChunkKvResponse {
                    map_revision: generation,
                    journal_position: None,
                    result: Err(RpcFailure {
                        code: ChunkKvRpcErrorCode::RefreshRequired,
                        message: "owner moved before operation".into(),
                        retry_after_ms: None,
                        latest_map_revision: Some(generation),
                        owner_hint: None,
                    }),
                });
            }
            route.accepted.store(generation, Ordering::Release);
        }
        if self
            .blackout
            .as_ref()
            .is_some_and(|fault| fault.blocked.load(Ordering::Acquire))
        {
            return Err(crowdb_chunk_kv_client::ClientError::Transport(
                "test metadata blackout".into(),
            ));
        }
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(TestCommand::Point(request.operation.clone(), sender))
            .await
            .unwrap();
        let response = receiver.await.unwrap();
        if !matches!(request.operation, PointOperation::Get { .. }) {
            if let Some(fault) = self.blackout.as_ref().filter(|fault| {
                fault
                    .key
                    .load()
                    .as_ref()
                    .is_some_and(|key| key.as_slice() == request.operation.key())
            }) {
                fault.blocked.store(true, Ordering::Release);
                return Err(crowdb_chunk_kv_client::ClientError::Transport(
                    "test lost applied publication and confirmation reads".into(),
                ));
            }
        }
        if !matches!(request.operation, PointOperation::Get { .. })
            && self.lose_mutation_reply.load(Ordering::Acquire)
        {
            return Err(crowdb_chunk_kv_client::ClientError::Transport(
                "test lost an applied mutation reply".into(),
            ));
        }
        Ok(response)
    }

    async fn seek(&self, _: &str, _: &SeekRequest) -> Result<ChunkKvResponse> {
        panic!("unexpected seek")
    }

    async fn scan(&self, _: &str, request: &ScanRequest) -> Result<ChunkKvResponse> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(TestCommand::Scan(request.clone(), sender))
            .await
            .unwrap();
        Ok(receiver.await.unwrap())
    }

    async fn scan_bounded(
        &self,
        endpoint: &str,
        request: &ScanRequest,
        max_bytes: usize,
    ) -> Result<ChunkKvResponse> {
        let response = self.scan(endpoint, request).await?;
        if let Ok(OperationResult::Scan { items, .. }) = &response.result {
            if items
                .iter()
                .map(|value| value.key.len() + value.value.len())
                .sum::<usize>()
                > max_bytes
            {
                return Err(crowdb_chunk_kv_client::ClientError::TooLarge);
            }
        }
        Ok(response)
    }
}

fn matches(observed: Option<&RpcValue>, condition: &RpcCompareCondition) -> bool {
    observed.is_some_and(|observed| match condition {
        RpcCompareCondition::Value(value) => observed.value == *value,
        RpcCompareCondition::Revision(revision) => observed.revision == *revision,
    })
}

fn point(
    operation: PointOperation,
    values: &mut BTreeMap<Vec<u8>, RpcValue>,
    revision: &mut u64,
) -> OperationResult {
    let key = operation.key().to_vec();
    let observed = values.get(&key).cloned();
    let (applied, value) = match operation {
        PointOperation::Get { .. } => return OperationResult::Value(observed),
        PointOperation::Put { value, .. } => (true, Some(value)),
        PointOperation::PutIfAbsent { value, .. } => (observed.is_none(), Some(value)),
        PointOperation::Delete { .. } => (observed.is_some(), None),
        PointOperation::CompareExchange { value, condition, .. } => {
            (matches(observed.as_ref(), &condition), Some(value))
        }
        PointOperation::ConditionalDelete { condition, .. } => (matches(observed.as_ref(), &condition), None),
    };
    if applied {
        *revision += 1;
        if let Some(value) = value {
            values.insert(
                key.clone(),
                RpcValue {
                    key,
                    value,
                    revision: *revision,
                },
            );
        } else {
            values.remove(&key);
        }
    }
    OperationResult::Mutation {
        applied,
        revision: applied.then_some(*revision),
        observed,
    }
}

fn scan(request: &ScanRequest, values: &BTreeMap<Vec<u8>, RpcValue>) -> OperationResult {
    let mut items: Vec<_> = values
        .values()
        .filter(|value| {
            request.start.as_ref().map_or(true, |start| value.key >= *start)
                && request.end.as_ref().map_or(true, |end| value.key < *end)
                && request
                    .continuation
                    .as_ref()
                    .map_or(true, |token| value.key > token.last_key)
        })
        .cloned()
        .collect();
    if request.direction == ScanDirection::Reverse {
        items.reverse();
    }
    let truncated = items.len() > request.limit as usize;
    items.truncate(request.limit as usize);
    let continuation = if truncated {
        items.last().map(|last| ScanContinuation {
            direction: request.direction,
            last_key: last.key.clone(),
            partition_id: request.routing.partition_id,
            owner_epoch: request.routing.owner_epoch,
            map_revision: request.routing.map_revision,
        })
    } else {
        None
    };
    OperationResult::Scan { items, continuation }
}

pub async fn client() -> Arc<ChunkKvClient> {
    let (head, pages) = catalog(1, 1);
    client_with_source(Arc::new(TestCatalog(head, pages))).await
}

pub fn catalog(
    generation: u64,
    owner_instance: u64,
) -> (ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>) {
    let mut page = ChunkKvRangeCatalogPage {
        generation,
        page_index: 0,
        entries: vec![ChunkKvRangeCatalogEntry {
            partition_id: Id128 { high: 1, low: 1 },
            range: KeyRange {
                start: Vec::new(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: owner_instance,
                rpc_endpoint: format!("test-owner-{owner_instance}"),
            },
            owner_epoch: generation,
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
        generation,
        previous_generation: None,
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: generation,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    (head, vec![page])
}

pub async fn client_with_source(source: Arc<dyn ChunkKvRangeCatalogSource>) -> Arc<ChunkKvClient> {
    client_with_options(
        source,
        Arc::new(AtomicBool::new(false)),
        ClientConfig::default(),
        None,
        None,
    )
    .await
}

#[allow(dead_code)] // Used by namespace uncertainty tests only.
pub async fn client_with_loss(loss: Arc<AtomicBool>) -> Arc<ChunkKvClient> {
    let (head, pages) = catalog(1, 1);
    client_with_options(
        Arc::new(TestCatalog(head, pages)),
        loss,
        ClientConfig {
            max_attempts: 1,
            ..ClientConfig::default()
        },
        None,
        None,
    )
    .await
}

#[allow(dead_code)] // Used by multipart publication fault acceptance.
pub async fn client_with_blackout(blackout: Arc<TestMutationBlackout>) -> Arc<ChunkKvClient> {
    let (head, pages) = catalog(1, 1);
    client_with_options(
        Arc::new(TestCatalog(head, pages)),
        Arc::new(AtomicBool::new(false)),
        ClientConfig {
            max_attempts: 1,
            ..ClientConfig::default()
        },
        Some(blackout),
        None,
    )
    .await
}

#[allow(dead_code)] // The catalog-move fixture is only used by topology acceptance.
pub async fn client_with_route(
    source: Arc<dyn ChunkKvRangeCatalogSource>,
    route: Arc<TestRouteChange>,
) -> Arc<ChunkKvClient> {
    client_with_options(
        source,
        Arc::new(AtomicBool::new(false)),
        ClientConfig::default(),
        None,
        Some(route),
    )
    .await
}

async fn client_with_options(
    source: Arc<dyn ChunkKvRangeCatalogSource>,
    loss: Arc<AtomicBool>,
    config: ClientConfig,
    blackout: Option<Arc<TestMutationBlackout>>,
    route: Option<Arc<TestRouteChange>>,
) -> Arc<ChunkKvClient> {
    let (sender, mut receiver) = mpsc::channel(64);
    tokio::spawn(async move {
        let mut values = BTreeMap::new();
        let mut revision = 0;
        while let Some(command) = receiver.recv().await {
            let (result, response) = match command {
                TestCommand::Point(operation, response) => {
                    (point(operation, &mut values, &mut revision), response)
                }
                TestCommand::Scan(request, response) => (scan(&request, &values), response),
            };
            let _ = response.send(ChunkKvResponse {
                map_revision: 1,
                journal_position: None,
                result: Ok(result),
            });
        }
    });
    let client = Arc::new(
        ChunkKvClient::new(
            config,
            source,
            Arc::new(TestMetadataTransport {
                sender,
                lose_mutation_reply: loss,
                blackout,
                route,
            }),
        )
        .unwrap(),
    );
    client.refresh_catalog().await.unwrap();
    client
}
