// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use crowdb_chunk_kv_client::{
    BatchItem, CatalogSource, ChunkKvClient, ChunkKvTransport, ClientConfig, ComposedItemError, Result,
};
use crowdb_protocol::chunk_kv::{
    BatchMutationRequest, BatchMutationResponse, BatchMutationResult, CatalogEntry, CatalogHead, CatalogPage,
    CatalogPageRef, CatalogPartitionState, ChunkKvResponse, ChunkKvRpcErrorCode, Id128, KeyRange,
    MultiGetRequest, MultiGetResponse, OperationResult, OwnerDescriptor, PartitionArtifact, PointOperation,
    PointRequest, RpcFailure, RpcValue,
};
use crowdb_protocol::chunk_stream::StreamName;

fn catalog() -> (CatalogHead, Vec<CatalogPage>) {
    let make_entry = |id, start: &[u8], end: Option<&[u8]>| CatalogEntry {
        partition_id: Id128 { high: 1, low: id },
        range: KeyRange {
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
        },
        owner: OwnerDescriptor {
            instance_id: id,
            rpc_endpoint: format!("owner-{id}"),
        },
        owner_epoch: id,
        state: CatalogPartitionState::Serving,
        artifact: PartitionArtifact {
            tree_manifest: id,
            stream_name: StreamName { high: 2, low: id },
            applied_seq: 0,
        },
        transition_id: None,
    };
    let mut page = CatalogPage {
        generation: 1,
        page_index: 0,
        entries: vec![make_entry(1, b"", Some(b"m")), make_entry(2, b"m", None)],
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = CatalogHead {
        generation: 1,
        previous_generation: None,
        pages: vec![CatalogPageRef {
            page_generation: 1,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    (head, vec![page])
}

struct StaticCatalog;

#[async_trait]
impl CatalogSource for StaticCatalog {
    async fn load(&self) -> Result<(CatalogHead, Vec<CatalogPage>)> {
        Ok(catalog())
    }
}

#[derive(Default)]
struct ComposeTransport {
    batches: Mutex<Vec<(String, Vec<Vec<u8>>)>>,
}

struct RetryTransport {
    batches: Mutex<Vec<(String, Vec<crowdb_protocol::chunk_kv::ClientRequestId>)>>,
    fail_owner_two: Mutex<bool>,
}

fn unavailable() -> RpcFailure {
    RpcFailure {
        code: ChunkKvRpcErrorCode::RequestExpired,
        message: "result unavailable".into(),
        retry_after_ms: None,
        latest_map_revision: None,
        owner_hint: None,
    }
}

#[async_trait]
impl ChunkKvTransport for ComposeTransport {
    async fn point(&self, _endpoint: &str, _request: &PointRequest) -> Result<ChunkKvResponse> {
        unreachable!("composition tests use group RPCs")
    }

    async fn multi_get(&self, endpoint: &str, request: &MultiGetRequest) -> Result<MultiGetResponse> {
        if endpoint == "owner-2" {
            return Ok(MultiGetResponse {
                map_revision: 1,
                result: Err(unavailable()),
            });
        }
        Ok(MultiGetResponse {
            map_revision: 1,
            result: Ok(request
                .keys
                .iter()
                .map(|key| {
                    Some(RpcValue {
                        key: key.clone(),
                        value: [b"value:".as_slice(), key].concat(),
                        revision: 3,
                    })
                })
                .collect()),
        })
    }

    async fn batch_mutate(
        &self,
        endpoint: &str,
        request: &BatchMutationRequest,
    ) -> Result<BatchMutationResponse> {
        self.batches.lock().unwrap().push((
            endpoint.into(),
            request
                .operations
                .iter()
                .map(|item| item.operation.key().to_vec())
                .collect(),
        ));
        if endpoint == "owner-2" {
            return Ok(BatchMutationResponse {
                map_revision: 1,
                result: Err(unavailable()),
            });
        }
        Ok(BatchMutationResponse {
            map_revision: 1,
            result: Ok(request
                .operations
                .iter()
                .map(|item| BatchMutationResult {
                    request_id: item.request_id,
                    journal_position: None,
                    result: Ok(OperationResult::Mutation {
                        applied: true,
                        revision: Some(item.request_id.client_sequence),
                        observed: None,
                    }),
                })
                .collect()),
        })
    }
}

#[async_trait]
impl ChunkKvTransport for RetryTransport {
    async fn point(&self, _endpoint: &str, _request: &PointRequest) -> Result<ChunkKvResponse> {
        unreachable!("composition tests use group RPCs")
    }

    async fn batch_mutate(
        &self,
        endpoint: &str,
        request: &BatchMutationRequest,
    ) -> Result<BatchMutationResponse> {
        self.batches.lock().unwrap().push((
            endpoint.into(),
            request.operations.iter().map(|item| item.request_id).collect(),
        ));
        if endpoint == "owner-2" {
            let mut fail = self.fail_owner_two.lock().unwrap();
            if *fail {
                *fail = false;
                return Err(crowdb_chunk_kv_client::ClientError::Transport(
                    "response lost".into(),
                ));
            }
        }
        Ok(BatchMutationResponse {
            map_revision: 1,
            result: Ok(request
                .operations
                .iter()
                .map(|item| BatchMutationResult {
                    request_id: item.request_id,
                    journal_position: None,
                    result: Ok(OperationResult::Mutation {
                        applied: true,
                        revision: Some(1),
                        observed: None,
                    }),
                })
                .collect()),
        })
    }
}

#[tokio::test]
async fn multi_get_preserves_duplicates_order_and_partial_failure() {
    let client = ChunkKvClient::new(
        ClientConfig::default(),
        Arc::new(StaticCatalog),
        Arc::new(ComposeTransport::default()),
    )
    .unwrap();
    let results = client
        .multi_get(vec![b"a".to_vec(), b"z".to_vec(), b"a".to_vec()])
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_ref().unwrap().as_ref().unwrap().key, b"a");
    assert!(matches!(results[1], Err(ComposedItemError::Server(_))));
    assert_eq!(results[2].as_ref().unwrap().as_ref().unwrap().key, b"a");
}

#[tokio::test]
async fn batch_preserves_partition_order_and_input_results() {
    let transport = Arc::new(ComposeTransport::default());
    let client = ChunkKvClient::new(
        ClientConfig::default(),
        Arc::new(StaticCatalog),
        transport.clone(),
    )
    .unwrap();
    let item = |key: &[u8]| BatchItem {
        operation: PointOperation::Put {
            key: key.to_vec(),
            value: b"value".to_vec(),
        },
        request_id: None,
    };
    let results = client
        .batch_mutate(vec![item(b"a"), item(b"z"), item(b"b")])
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok());
    assert!(matches!(results[1], Err(ComposedItemError::Server(_))));
    assert!(results[2].is_ok());
    let batches = transport.batches.lock().unwrap();
    assert!(batches
        .iter()
        .any(|(owner, keys)| owner == "owner-1" && keys == &[b"a".to_vec(), b"b".to_vec()]));
}

#[tokio::test]
async fn empty_compositions_succeed_without_catalog_access() {
    let client = ChunkKvClient::new(
        ClientConfig::default(),
        Arc::new(StaticCatalog),
        Arc::new(ComposeTransport::default()),
    )
    .unwrap();
    assert!(client.multi_get(Vec::new()).await.unwrap().is_empty());
    assert!(client.batch_mutate(Vec::new()).await.unwrap().is_empty());
}

#[tokio::test]
async fn batch_retries_only_unresolved_operations_with_stable_identities() {
    let transport = Arc::new(RetryTransport {
        batches: Mutex::new(Vec::new()),
        fail_owner_two: Mutex::new(true),
    });
    let client = ChunkKvClient::new(
        ClientConfig::default(),
        Arc::new(StaticCatalog),
        transport.clone(),
    )
    .unwrap();
    let item = |key: &[u8]| BatchItem {
        operation: PointOperation::Put {
            key: key.to_vec(),
            value: b"value".to_vec(),
        },
        request_id: None,
    };
    let results = client
        .batch_mutate(vec![item(b"a"), item(b"z"), item(b"b")])
        .await
        .unwrap();
    assert!(results.iter().all(std::result::Result::is_ok));

    let batches = transport.batches.lock().unwrap();
    let owner_one: Vec<_> = batches.iter().filter(|(owner, _)| owner == "owner-1").collect();
    let owner_two: Vec<_> = batches.iter().filter(|(owner, _)| owner == "owner-2").collect();
    assert_eq!(owner_one.len(), 1);
    assert_eq!(owner_two.len(), 2);
    assert_eq!(owner_two[0].1, owner_two[1].1);
}
