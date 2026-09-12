// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use crowdb_chunk_kv_client::{
    CatalogSource, ChunkKvClient, ChunkKvTransport, ClientConfig, MultiScanRequest, Result,
};
use crowdb_protocol::chunk_kv::{
    CatalogEntry, CatalogHead, CatalogPage, CatalogPageRef, CatalogPartitionState, ChunkKvResponse,
    ChunkKvRpcErrorCode, Id128, KeyRange, OperationResult, OwnerDescriptor, PartitionArtifact, PointRequest,
    RpcFailure, RpcJournalPosition, RpcValue, ScanContinuation, ScanDirection, ScanRequest, SeekKind,
    SeekRequest,
};
use crowdb_protocol::chunk_stream::StreamName;

type Boundary<'a> = (&'a [u8], Option<&'a [u8]>, u64);

fn catalog(generation: u64, boundaries: &[Boundary<'_>]) -> (CatalogHead, Vec<CatalogPage>) {
    let entries = boundaries
        .iter()
        .map(|(start, end, owner)| CatalogEntry {
            partition_id: Id128 {
                high: generation,
                low: *owner,
            },
            range: KeyRange {
                start: start.to_vec(),
                end: end.map(<[u8]>::to_vec),
            },
            owner: OwnerDescriptor {
                instance_id: *owner,
                rpc_endpoint: format!("owner-{owner}"),
            },
            owner_epoch: generation,
            state: CatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: generation,
                tree_manifest: generation,
                stream_name: StreamName {
                    high: generation,
                    low: *owner,
                },
                applied_seq: 0,
            },
            transition_id: None,
        })
        .collect();
    let mut page = CatalogPage {
        generation,
        page_index: 0,
        entries,
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = CatalogHead {
        generation,
        previous_generation: generation.checked_sub(1).filter(|value| *value != 0),
        pages: vec![CatalogPageRef {
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

struct ScriptedCatalog {
    versions: Mutex<Vec<(CatalogHead, Vec<CatalogPage>)>>,
}

#[async_trait]
impl CatalogSource for ScriptedCatalog {
    async fn load(&self) -> Result<(CatalogHead, Vec<CatalogPage>)> {
        let mut versions = self.versions.lock().unwrap();
        if versions.len() > 1 {
            Ok(versions.remove(0))
        } else {
            Ok(versions[0].clone())
        }
    }
}

struct OrderedTransport {
    keys: HashMap<String, Vec<Vec<u8>>>,
    fail_owner_two_once: Mutex<bool>,
    seeks: Mutex<Vec<(String, SeekRequest)>>,
}

impl OrderedTransport {
    fn stable() -> Self {
        Self {
            keys: HashMap::from([
                ("owner-1".into(), vec![b"a".to_vec(), b"b".to_vec()]),
                ("owner-2".into(), vec![b"m".to_vec(), b"z".to_vec()]),
            ]),
            fail_owner_two_once: Mutex::new(false),
            seeks: Mutex::new(Vec::new()),
        }
    }

    fn changing() -> Self {
        Self {
            keys: HashMap::from([
                ("owner-1".into(), vec![b"a".to_vec(), b"b".to_vec()]),
                ("owner-2".into(), vec![b"m".to_vec(), b"z".to_vec()]),
                ("owner-3".into(), vec![b"a".to_vec(), b"b".to_vec()]),
                ("owner-4".into(), vec![b"m".to_vec()]),
                ("owner-5".into(), vec![b"z".to_vec()]),
            ]),
            fail_owner_two_once: Mutex::new(true),
            seeks: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl ChunkKvTransport for OrderedTransport {
    async fn point(&self, _endpoint: &str, _request: &PointRequest) -> Result<ChunkKvResponse> {
        unreachable!("ordered tests do not issue point RPCs")
    }

    async fn seek(&self, endpoint: &str, request: &SeekRequest) -> Result<ChunkKvResponse> {
        self.seeks
            .lock()
            .unwrap()
            .push((endpoint.to_string(), request.clone()));
        Ok(ChunkKvResponse {
            map_revision: request.routing.map_revision,
            journal_position: Some(RpcJournalPosition {
                stream_name: Id128 { high: 8, low: 9 },
                offset: 10,
            }),
            result: Ok(OperationResult::Value(Some(RpcValue {
                key: request.key.clone(),
                value: b"value".to_vec(),
                revision: 7,
            }))),
        })
    }

    async fn scan(&self, endpoint: &str, request: &ScanRequest) -> Result<ChunkKvResponse> {
        if endpoint == "owner-2" {
            let mut fail = self.fail_owner_two_once.lock().unwrap();
            if *fail {
                *fail = false;
                return Ok(ChunkKvResponse {
                    map_revision: request.routing.map_revision,
                    journal_position: None,
                    result: Err(RpcFailure {
                        code: ChunkKvRpcErrorCode::NotMyRange,
                        message: "split".into(),
                        retry_after_ms: None,
                        latest_map_revision: Some(2),
                        owner_hint: None,
                    }),
                });
            }
        }
        let mut keys = self.keys.get(endpoint).cloned().unwrap_or_default();
        keys.retain(|key| {
            request
                .start
                .as_deref()
                .map_or(true, |start| key.as_slice() >= start)
                && request.end.as_deref().map_or(true, |end| key.as_slice() < end)
                && request.continuation.as_ref().map_or(true, |token| {
                    if request.direction == ScanDirection::Forward {
                        key > &token.last_key
                    } else {
                        key < &token.last_key
                    }
                })
        });
        keys.sort_unstable();
        if request.direction == ScanDirection::Reverse {
            keys.reverse();
        }
        let has_more = keys.len() > request.limit as usize;
        keys.truncate(request.limit as usize);
        let items: Vec<_> = keys
            .into_iter()
            .map(|key| RpcValue {
                value: [b"value:".as_slice(), &key].concat(),
                key,
                revision: 1,
            })
            .collect();
        let continuation = has_more.then(|| ScanContinuation {
            direction: request.direction,
            last_key: items.last().unwrap().key.clone(),
            partition_id: request.routing.partition_id,
            owner_epoch: request.routing.owner_epoch,
            map_revision: request.routing.map_revision,
        });
        Ok(ChunkKvResponse {
            map_revision: request.routing.map_revision,
            journal_position: None,
            result: Ok(OperationResult::Scan { items, continuation }),
        })
    }
}

fn scan(direction: ScanDirection, max_items: usize) -> MultiScanRequest {
    MultiScanRequest {
        start: None,
        end: None,
        direction,
        max_items,
        max_bytes: 1_024,
        continuation: None,
    }
}

#[tokio::test]
async fn seek_routes_directly_and_preserves_typed_fields() {
    let transport = Arc::new(OrderedTransport::stable());
    let client = ChunkKvClient::new(
        ClientConfig::default(),
        Arc::new(ScriptedCatalog {
            versions: Mutex::new(vec![catalog(
                1,
                &[
                    (b"".as_slice(), Some(b"m".as_slice()), 1),
                    (b"m".as_slice(), None, 2),
                ],
            )]),
        }),
        transport.clone(),
    )
    .unwrap();
    let response = client.floor(b"z".to_vec()).await.unwrap();
    assert_eq!(response.journal_position.unwrap().offset, 10);
    assert_eq!(
        response.result.unwrap(),
        OperationResult::Value(Some(RpcValue {
            key: b"z".to_vec(),
            value: b"value".to_vec(),
            revision: 7,
        }))
    );
    let seeks = transport.seeks.lock().unwrap();
    assert_eq!(seeks[0].0, "owner-2");
    assert_eq!(seeks[0].1.kind, SeekKind::Floor);
}

#[tokio::test]
async fn scan_pages_forward_and_reverse_without_repeating_boundaries() {
    let client = ChunkKvClient::new(
        ClientConfig::default(),
        Arc::new(ScriptedCatalog {
            versions: Mutex::new(vec![catalog(
                1,
                &[
                    (b"".as_slice(), Some(b"m".as_slice()), 1),
                    (b"m".as_slice(), None, 2),
                ],
            )]),
        }),
        Arc::new(OrderedTransport::stable()),
    )
    .unwrap();

    let first = client.scan(scan(ScanDirection::Forward, 3)).await.unwrap();
    assert_eq!(
        first.items.iter().map(|item| &item.key).collect::<Vec<_>>(),
        [&b"a".to_vec(), &b"b".to_vec(), &b"m".to_vec()]
    );
    let mut next = scan(ScanDirection::Forward, 3);
    next.continuation = first.continuation;
    let second = client.scan(next).await.unwrap();
    assert_eq!(
        second
            .items
            .iter()
            .map(|item| item.key.as_slice())
            .collect::<Vec<_>>(),
        [b"z".as_slice()]
    );
    assert!(second.continuation.is_none());

    let first = client.scan(scan(ScanDirection::Reverse, 3)).await.unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|item| item.key.as_slice())
            .collect::<Vec<_>>(),
        [b"z".as_slice(), b"m".as_slice(), b"b".as_slice()]
    );
    let mut next = scan(ScanDirection::Reverse, 3);
    next.continuation = first.continuation;
    let second = client.scan(next).await.unwrap();
    assert_eq!(
        second
            .items
            .iter()
            .map(|item| item.key.as_slice())
            .collect::<Vec<_>>(),
        [b"a".as_slice()]
    );
}

#[tokio::test]
async fn topology_replan_resumes_strictly_after_last_emitted_key() {
    let first = catalog(
        1,
        &[
            (b"".as_slice(), Some(b"m".as_slice()), 1),
            (b"m".as_slice(), None, 2),
        ],
    );
    let second = catalog(
        2,
        &[
            (b"".as_slice(), Some(b"c".as_slice()), 3),
            (b"c".as_slice(), Some(b"t".as_slice()), 4),
            (b"t".as_slice(), None, 5),
        ],
    );
    let client = ChunkKvClient::new(
        ClientConfig::default(),
        Arc::new(ScriptedCatalog {
            versions: Mutex::new(vec![first, second]),
        }),
        Arc::new(OrderedTransport::changing()),
    )
    .unwrap();
    let page = client.scan(scan(ScanDirection::Forward, 10)).await.unwrap();
    assert_eq!(
        page.items
            .iter()
            .map(|item| item.key.as_slice())
            .collect::<Vec<_>>(),
        [b"a".as_slice(), b"b".as_slice(), b"m".as_slice(), b"z".as_slice()]
    );
    assert!(page.terminal_failure.is_none());
}
