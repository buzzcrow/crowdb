// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use crowdb_access_s3::metadata::{ChunkKvMetadataStore, MultipartRepository, TenantId};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogSource, ChunkKvTransport, ClientConfig, ClientError, Result,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, ChunkKvResponse, Id128, KeyRange, OperationResult, OwnerDescriptor,
    PartitionArtifact, PointOperation, PointRequest, RpcCompareCondition, RpcValue, ScanContinuation,
    ScanRequest,
};
use crowdb_protocol::chunk_stream::StreamName;
use tokio::sync::{mpsc, oneshot};

#[path = "multipart_scan_fault.rs"]
mod scan_fault;
use scan_fault::{TestScanAction, TestScans};
#[path = "multipart_point_control.rs"]
mod point_control;
use point_control::TestPoints;
pub(super) use point_control::{TestPointBarrier, TestPointEntry};

struct TestCatalog(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>);

#[async_trait]
impl ChunkKvRangeCatalogSource for TestCatalog {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        Ok((self.0.clone(), self.1.clone()))
    }
}

enum TestActorRequest {
    Point(PointOperation, oneshot::Sender<Result<ChunkKvResponse>>),
    Scan(ScanRequest, oneshot::Sender<Result<ChunkKvResponse>>),
    Inject(TestConflict),
    LoseAfter(usize),
    FailBefore(usize),
    ClearConflict,
    ScanControl(TestScanAction),
}

struct TestConflict {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    skip: usize,
}

pub(super) struct TestProbe {
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    lose_reply: Arc<AtomicBool>,
    sender: mpsc::UnboundedSender<TestActorRequest>,
    scans: Arc<AtomicUsize>,
    largest_scan: Arc<AtomicUsize>,
    points: Arc<TestPoints>,
}

impl TestProbe {
    pub(super) fn counts(&self) -> (usize, usize) {
        (
            self.reads.load(Ordering::Relaxed),
            self.writes.load(Ordering::Relaxed),
        )
    }

    pub(super) fn conflict(&self, key: Vec<u8>, value: Vec<u8>, skip: usize) {
        self.sender
            .send(TestActorRequest::Inject(TestConflict {
                key,
                value: Some(value),
                skip,
            }))
            .unwrap();
    }

    pub(super) fn conflict_absent(&self, key: Vec<u8>, skip: usize) {
        self.sender
            .send(TestActorRequest::Inject(TestConflict {
                key,
                value: None,
                skip,
            }))
            .unwrap();
    }

    pub(super) fn clear_conflict_check(&self) {
        self.sender.send(TestActorRequest::ClearConflict).unwrap();
    }

    pub(super) fn pause_mutation(&self, key: Vec<u8>) -> TestPointBarrier {
        self.points.pause(key, true, false, 0)
    }

    pub(super) fn pause_get(&self, key: Vec<u8>, skip: usize) -> TestPointBarrier {
        self.points.pause(key, false, false, skip)
    }

    pub(super) fn pause_after_get(&self, key: Vec<u8>, skip: usize) -> TestPointBarrier {
        self.points.pause(key, false, true, skip)
    }

    pub(super) fn fail_get(&self, key: Vec<u8>, skip: usize) {
        self.points.fail_get(key, skip);
    }

    pub(super) fn get_fault_consumed(&self) -> bool {
        self.points.get_fault_consumed()
    }

    pub(super) fn operation_log(&self) -> Vec<TestPointEntry> {
        self.points.log()
    }

    pub(super) fn lose_after(&self, mutations: usize) {
        self.sender.send(TestActorRequest::LoseAfter(mutations)).unwrap();
    }

    pub(super) fn fail_before(&self, mutations: usize) {
        self.sender.send(TestActorRequest::FailBefore(mutations)).unwrap();
    }

    pub(super) fn scan_counts(&self) -> (usize, usize) {
        (
            self.scans.load(Ordering::SeqCst),
            self.largest_scan.load(Ordering::SeqCst),
        )
    }

    pub(super) fn fail_scan(&self, prefix: Vec<u8>) {
        self.sender
            .send(TestActorRequest::ScanControl(TestScanAction::Fail(prefix)))
            .unwrap();
    }

    pub(super) fn pause_scan(&self, prefix: Vec<u8>) -> oneshot::Receiver<()> {
        let (started, receiver) = oneshot::channel();
        self.sender
            .send(TestActorRequest::ScanControl(TestScanAction::Pause(
                prefix, started,
            )))
            .unwrap();
        receiver
    }

    pub(super) fn resume_scans(&self) {
        self.sender
            .send(TestActorRequest::ScanControl(TestScanAction::Resume))
            .unwrap();
    }
}

struct TestActorTransport {
    sender: mpsc::UnboundedSender<TestActorRequest>,
    points: Arc<TestPoints>,
}

#[async_trait]
impl ChunkKvTransport for TestActorTransport {
    async fn point(&self, _: &str, request: &PointRequest) -> Result<ChunkKvResponse> {
        self.points.before(&request.operation).await?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(TestActorRequest::Point(request.operation.clone(), response))
            .expect("actor is running");
        let result = receiver.await.expect("actor replies");
        self.points.after(&request.operation).await?;
        result
    }

    async fn seek(&self, _: &str, _: &crowdb_protocol::chunk_kv::SeekRequest) -> Result<ChunkKvResponse> {
        unreachable!()
    }

    async fn scan(&self, _: &str, request: &ScanRequest) -> Result<ChunkKvResponse> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(TestActorRequest::Scan(request.clone(), response))
            .expect("actor is running");
        receiver.await.expect("actor replies")
    }
}

fn reply(operation: PointOperation, values: &mut HashMap<Vec<u8>, RpcValue>) -> ChunkKvResponse {
    let result = match operation {
        PointOperation::Put { key, value } => {
            let revision = values.get(&key).map_or(1, |old| old.revision + 1);
            values.insert(key.clone(), RpcValue { key, value, revision });
            OperationResult::Mutation {
                applied: true,
                revision: Some(revision),
                observed: None,
            }
        }
        PointOperation::Get { key } => OperationResult::Value(values.get(&key).cloned()),
        PointOperation::PutIfAbsent { key, value } => {
            let observed = values.get(&key).cloned();
            let applied = observed.is_none();
            if applied {
                values.insert(
                    key.clone(),
                    RpcValue {
                        key,
                        value,
                        revision: 1,
                    },
                );
            }
            OperationResult::Mutation {
                applied,
                revision: applied.then_some(1),
                observed,
            }
        }
        PointOperation::CompareExchange {
            key,
            condition: RpcCompareCondition::Value(expected),
            value,
        } => {
            let observed = values.get(&key).cloned();
            let applied = observed
                .as_ref()
                .is_some_and(|observed| observed.value == expected);
            let revision = observed.as_ref().map_or(1, |observed| observed.revision + 1);
            if applied {
                values.insert(key.clone(), RpcValue { key, value, revision });
            }
            OperationResult::Mutation {
                applied,
                revision: applied.then_some(revision),
                observed,
            }
        }
        other => panic!("unexpected operation: {other:?}"),
    };
    ChunkKvResponse {
        map_revision: 1,
        journal_position: None,
        result: Ok(result),
    }
}

fn scan_reply(request: &ScanRequest, values: &HashMap<Vec<u8>, RpcValue>) -> ChunkKvResponse {
    let mut ordered: Vec<RpcValue> = values
        .values()
        .filter(|entry| {
            request.start.as_ref().map_or(true, |start| entry.key >= *start)
                && request.end.as_ref().map_or(true, |end| entry.key < *end)
                && request
                    .continuation
                    .as_ref()
                    .map_or(true, |token| entry.key > token.last_key)
        })
        .cloned()
        .collect();
    ordered.sort_by(|left, right| left.key.cmp(&right.key));
    let limit = usize::try_from(request.limit).unwrap();
    let truncated = ordered.len() > limit;
    ordered.truncate(limit);
    let continuation = if truncated {
        Some(ScanContinuation {
            direction: request.direction,
            last_key: ordered.last().unwrap().key.clone(),
            partition_id: request.routing.partition_id,
            owner_epoch: request.routing.owner_epoch,
            map_revision: request.routing.map_revision,
        })
    } else {
        None
    };
    ChunkKvResponse {
        map_revision: 1,
        journal_position: None,
        result: Ok(OperationResult::Scan {
            items: ordered,
            continuation,
        }),
    }
}

pub(super) async fn repository() -> (MultipartRepository, Arc<AtomicBool>, Arc<ChunkKvMetadataStore>) {
    let (repository, probe, store) = repository_with_probe().await;
    (repository, probe.lose_reply.clone(), store)
}

fn spawn_actor(
    mut receiver: mpsc::UnboundedReceiver<TestActorRequest>,
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    lose_reply: Arc<AtomicBool>,
    scans: Arc<AtomicUsize>,
    largest_scan: Arc<AtomicUsize>,
) {
    tokio::spawn(async move {
        let mut values = HashMap::new();
        let mut conflict: Option<TestConflict> = None;
        let mut forbidden_get: Option<Vec<u8>> = None;
        let mut lose_after = None;
        let mut fail_before = None;
        let mut scan_control = TestScans::default();
        while let Some(request) = receiver.recv().await {
            match request {
                TestActorRequest::Point(operation, response) => {
                    let is_mutation = !matches!(operation, PointOperation::Get { .. });
                    if let PointOperation::Get { key } = &operation {
                        reads.fetch_add(1, Ordering::Relaxed);
                        assert_ne!(
                            Some(key),
                            forbidden_get.as_ref(),
                            "GET after definite CAS conflict"
                        );
                    } else {
                        writes.fetch_add(1, Ordering::Relaxed);
                    }
                    if is_mutation {
                        match fail_before.as_mut() {
                            Some(0) => {
                                fail_before = None;
                                let _ = response
                                    .send(Err(ClientError::Transport("request lost before apply".into())));
                                continue;
                            }
                            Some(remaining) => *remaining -= 1,
                            None => {}
                        }
                    }
                    let mutation_key = match &operation {
                        PointOperation::CompareExchange { key, .. }
                        | PointOperation::PutIfAbsent { key, .. } => Some(key),
                        _ => None,
                    };
                    if let Some(fault) = conflict.as_mut().filter(|fault| Some(&fault.key) == mutation_key) {
                        if fault.skip == 0 {
                            let fault = conflict.take().unwrap();
                            let revision = values
                                .get(&fault.key)
                                .map_or(1, |value: &RpcValue| value.revision + 1);
                            forbidden_get = Some(fault.key.clone());
                            if let Some(value) = fault.value {
                                values.insert(
                                    fault.key.clone(),
                                    RpcValue {
                                        key: fault.key,
                                        value,
                                        revision,
                                    },
                                );
                            } else {
                                values.remove(&fault.key);
                            }
                        } else {
                            fault.skip -= 1;
                        }
                    }
                    let result = reply(operation, &mut values);
                    let lost = is_mutation
                        && (lose_reply.swap(false, Ordering::SeqCst)
                            || match lose_after.as_mut() {
                                Some(0) => {
                                    lose_after = None;
                                    true
                                }
                                Some(remaining) => {
                                    *remaining -= 1;
                                    false
                                }
                                None => false,
                            });
                    if lost {
                        let _ = response.send(Err(ClientError::Transport("committed reply lost".into())));
                    } else {
                        let _ = response.send(Ok(result));
                    }
                }
                TestActorRequest::Scan(request, response) => {
                    scans.fetch_add(1, Ordering::SeqCst);
                    largest_scan.fetch_max(request.limit as usize, Ordering::SeqCst);
                    scan_control.handle(request, response, &values);
                }
                TestActorRequest::Inject(fault) => {
                    conflict = Some(fault);
                    forbidden_get = None;
                }
                TestActorRequest::ClearConflict => {
                    conflict = None;
                    forbidden_get = None;
                }
                TestActorRequest::LoseAfter(remaining) => lose_after = Some(remaining),
                TestActorRequest::FailBefore(remaining) => fail_before = Some(remaining),
                TestActorRequest::ScanControl(action) => scan_control.configure(action, &values),
            }
        }
    });
}

pub(super) async fn repository_with_probe() -> (MultipartRepository, Arc<TestProbe>, Arc<ChunkKvMetadataStore>)
{
    let (sender, receiver) = mpsc::unbounded_channel::<TestActorRequest>();
    let probe = Arc::new(TestProbe {
        reads: Arc::new(AtomicUsize::new(0)),
        writes: Arc::new(AtomicUsize::new(0)),
        lose_reply: Arc::new(AtomicBool::new(false)),
        sender: sender.clone(),
        scans: Arc::new(AtomicUsize::new(0)),
        largest_scan: Arc::new(AtomicUsize::new(0)),
        points: Arc::new(TestPoints::default()),
    });
    spawn_actor(
        receiver,
        probe.reads.clone(),
        probe.writes.clone(),
        probe.lose_reply.clone(),
        probe.scans.clone(),
        probe.largest_scan.clone(),
    );
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
    let client = Arc::new(
        ChunkKvClient::new(
            ClientConfig {
                operation_timeout: Duration::from_secs(1),
                retry_backoff: Duration::from_millis(1),
                max_attempts: 1,
                ..ClientConfig::default()
            },
            Arc::new(TestCatalog(head, vec![page])),
            Arc::new(TestActorTransport {
                sender,
                points: probe.points.clone(),
            }),
        )
        .unwrap(),
    );
    client.refresh_catalog().await.unwrap();
    let store = Arc::new(ChunkKvMetadataStore::new(client));
    (
        MultipartRepository::new(Arc::clone(&store), TenantId::new(b"tenant".to_vec()).unwrap()),
        probe,
        store,
    )
}
