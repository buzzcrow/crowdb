// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogSource, ChunkKvTransport, ClientConfig, ClientError, Result,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, ChunkKvResponse, ChunkKvRpcErrorCode, Id128, KeyRange,
    OperationResult, OwnerDescriptor, PartitionArtifact, PointRequest, RpcFailure, RpcValue,
    ScanContinuation, ScanRequest,
};
use crowdb_protocol::chunk_stream::StreamName;

pub(super) type TestVersion = (ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>);
pub(super) type TestBoundary<'a> = (&'a [u8], Option<&'a [u8]>, u64);

pub(super) fn catalog(generation: u64, bounds: &[TestBoundary<'_>]) -> TestVersion {
    let entries = bounds
        .iter()
        .enumerate()
        .map(|(index, (start, end, owner))| {
            let partition_id = Id128 {
                high: generation,
                low: u64::try_from(index).unwrap() + 1,
            };
            ChunkKvRangeCatalogEntry {
                partition_id,
                range: KeyRange {
                    start: start.to_vec(),
                    end: end.map(<[u8]>::to_vec),
                },
                owner: OwnerDescriptor {
                    instance_id: *owner,
                    rpc_endpoint: format!("owner-{owner}"),
                },
                owner_epoch: generation,
                state: ChunkKvRangeCatalogPartitionState::Serving,
                artifact: PartitionArtifact {
                    tree_id: generation,
                    stream_name: StreamName {
                        high: generation,
                        low: partition_id.low,
                    },
                    tail_overlay: None,
                },
                transition_id: None,
            }
        })
        .collect();
    let mut page = ChunkKvRangeCatalogPage {
        generation,
        page_index: 0,
        entries,
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
    (head, vec![page])
}

pub(super) struct TestCatalog {
    versions: Mutex<VecDeque<TestVersion>>,
    loads: AtomicUsize,
}

impl TestCatalog {
    pub(super) fn new(versions: Vec<TestVersion>) -> Self {
        Self {
            versions: Mutex::new(versions.into()),
            loads: AtomicUsize::new(0),
        }
    }

    pub(super) fn loads(&self) -> usize {
        self.loads.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl ChunkKvRangeCatalogSource for TestCatalog {
    async fn load(&self) -> Result<TestVersion> {
        self.loads.fetch_add(1, Ordering::Relaxed);
        let mut versions = self.versions.lock().unwrap();
        Ok(if versions.len() > 1 {
            versions.pop_front().unwrap()
        } else {
            versions[0].clone()
        })
    }
}

#[derive(Clone, Copy)]
pub(super) enum TestFault {
    Route(ChunkKvRpcErrorCode),
    Generation(u64),
    Continuation,
    OverBudgetRows,
}

#[derive(Clone, Debug)]
pub(super) struct TestCall {
    pub(super) endpoint: String,
    pub(super) request: ScanRequest,
    pub(super) bytes: usize,
}

pub(super) struct TestTransport {
    rows: Vec<RpcValue>,
    calls: Mutex<Vec<TestCall>>,
    faults: Mutex<BTreeMap<usize, TestFault>>,
    legacy_calls: AtomicUsize,
}

impl TestTransport {
    pub(super) fn new(keys: &[&[u8]]) -> Self {
        Self {
            rows: keys
                .iter()
                .map(|key| RpcValue {
                    key: key.to_vec(),
                    value: vec![7; 7],
                    revision: 1,
                })
                .collect(),
            calls: Mutex::new(Vec::new()),
            faults: Mutex::new(BTreeMap::new()),
            legacy_calls: AtomicUsize::new(0),
        }
    }

    pub(super) fn with_value_size(mut self, index: usize, bytes: usize) -> Self {
        self.rows[index].value = vec![7; bytes];
        self
    }

    pub(super) fn fault(&self, call: usize, fault: TestFault) {
        assert!(self.faults.lock().unwrap().insert(call, fault).is_none());
    }

    pub(super) fn faults_consumed(&self) -> bool {
        self.faults.lock().unwrap().is_empty()
    }

    pub(super) fn calls(&self) -> Vec<TestCall> {
        self.calls.lock().unwrap().clone()
    }

    pub(super) fn legacy_calls(&self) -> usize {
        self.legacy_calls.load(Ordering::Relaxed)
    }

    fn page(&self, request: &ScanRequest, budget: usize, fault: Option<TestFault>) -> ChunkKvResponse {
        if let Some(TestFault::Route(code)) = fault {
            return failure(request, code);
        }
        let mut rows = Vec::new();
        let mut bytes = 0;
        let mut truncated = matches!(fault, Some(TestFault::Continuation));
        for row in &self.rows {
            if request.start.as_ref().is_some_and(|start| row.key < *start)
                || request.end.as_ref().is_some_and(|end| row.key >= *end)
                || request
                    .continuation
                    .as_ref()
                    .is_some_and(|token| row.key <= token.last_key)
            {
                continue;
            }
            let row_bytes = row.key.len() + row.value.len();
            if !matches!(fault, Some(TestFault::OverBudgetRows))
                && (rows.len() == request.limit as usize || bytes + row_bytes > budget)
            {
                if rows.is_empty() {
                    return failure(request, ChunkKvRpcErrorCode::ScanLimitExceeded);
                }
                truncated = true;
                break;
            }
            bytes += row_bytes;
            rows.push(row.clone());
        }
        let continuation = truncated
            .then(|| rows.last())
            .flatten()
            .map(|row| ScanContinuation {
                direction: request.direction,
                last_key: row.key.clone(),
                partition_id: request.routing.partition_id,
                owner_epoch: request.routing.owner_epoch,
                map_revision: request.routing.map_revision,
            });
        ChunkKvResponse {
            map_revision: match fault {
                Some(TestFault::Generation(value)) => value,
                _ => request.routing.map_revision,
            },
            journal_position: None,
            result: Ok(OperationResult::Scan {
                items: rows,
                continuation,
            }),
        }
    }
}

#[async_trait]
impl ChunkKvTransport for TestTransport {
    async fn point(&self, _: &str, _: &PointRequest) -> Result<ChunkKvResponse> {
        Err(ClientError::Transport(
            "point RPC is outside this scan test".into(),
        ))
    }

    async fn scan(&self, _: &str, _: &ScanRequest) -> Result<ChunkKvResponse> {
        self.legacy_calls.fetch_add(1, Ordering::Relaxed);
        Err(ClientError::Transport(
            "strict scan used the legacy transport".into(),
        ))
    }

    async fn scan_bounded(
        &self,
        endpoint: &str,
        request: &ScanRequest,
        bytes: usize,
    ) -> Result<ChunkKvResponse> {
        let index = {
            let mut calls = self.calls.lock().unwrap();
            calls.push(TestCall {
                endpoint: endpoint.into(),
                request: request.clone(),
                bytes,
            });
            calls.len()
        };
        let fault = self.faults.lock().unwrap().remove(&index);
        Ok(self.page(request, bytes, fault))
    }
}

fn failure(request: &ScanRequest, code: ChunkKvRpcErrorCode) -> ChunkKvResponse {
    ChunkKvResponse {
        map_revision: request.routing.map_revision,
        journal_position: None,
        result: Err(RpcFailure {
            code,
            message: "injected partition outcome".into(),
            retry_after_ms: None,
            latest_map_revision: Some(request.routing.map_revision + 1),
            owner_hint: None,
        }),
    }
}

pub(super) fn client(source: Arc<TestCatalog>, transport: Arc<TestTransport>) -> ChunkKvClient {
    ChunkKvClient::new(
        ClientConfig {
            max_attempts: 3,
            retry_backoff: Duration::from_millis(1),
            ..ClientConfig::default()
        },
        source,
        transport,
    )
    .unwrap()
}
