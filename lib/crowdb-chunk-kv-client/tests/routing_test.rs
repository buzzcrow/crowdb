// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use crowdb_chunk_kv_client::{
    CatalogSource, ChunkKvClient, ChunkKvTransport, ClientConfig, ClientError, Result,
};
use crowdb_protocol::chunk_kv::{
    CatalogEntry, CatalogHead, CatalogPage, CatalogPageRef, CatalogPartitionState, ChunkKvResponse,
    ChunkKvRpcErrorCode, Id128, KeyRange, OperationResult, OwnerDescriptor, PartitionArtifact, PointRequest,
    RpcFailure,
};
use crowdb_protocol::chunk_stream::StreamName;

fn catalog(generation: u64, owner: u64, epoch: u64) -> (CatalogHead, Vec<CatalogPage>) {
    let mut page = CatalogPage {
        generation,
        page_index: 0,
        entries: vec![CatalogEntry {
            partition_id: Id128 { high: 1, low: 2 },
            range: KeyRange {
                start: Vec::new(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: owner,
                rpc_endpoint: format!("owner-{owner}"),
            },
            owner_epoch: epoch,
            state: CatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: 2,
                tree_manifest: 3,
                stream_name: StreamName { high: 4, low: 5 },
                applied_seq: 6,
            },
            transition_id: None,
        }],
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = CatalogHead {
        generation,
        previous_generation: generation.checked_sub(1).filter(|previous| *previous != 0),
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
    generations: Mutex<Vec<(CatalogHead, Vec<CatalogPage>)>>,
}

#[async_trait]
impl CatalogSource for ScriptedCatalog {
    async fn load(&self) -> Result<(CatalogHead, Vec<CatalogPage>)> {
        let mut generations = self.generations.lock().unwrap();
        if generations.is_empty() {
            return Err(ClientError::CatalogUnavailable("group 0 unavailable".into()));
        }
        Ok(generations.remove(0))
    }
}

#[derive(Default)]
struct ScriptedTransport {
    requests: Mutex<Vec<(String, PointRequest)>>,
}

#[async_trait]
impl ChunkKvTransport for ScriptedTransport {
    async fn point(&self, endpoint: &str, request: &PointRequest) -> Result<ChunkKvResponse> {
        self.requests
            .lock()
            .unwrap()
            .push((endpoint.to_string(), request.clone()));
        if endpoint == "owner-1" {
            Ok(ChunkKvResponse {
                map_revision: 1,
                journal_position: None,
                result: Err(RpcFailure {
                    code: ChunkKvRpcErrorCode::NotMyRange,
                    message: "moved".into(),
                    retry_after_ms: None,
                    latest_map_revision: Some(2),
                    owner_hint: None,
                }),
            })
        } else {
            Ok(ChunkKvResponse {
                map_revision: 2,
                journal_position: None,
                result: Ok(OperationResult::Mutation {
                    applied: true,
                    revision: Some(7),
                    observed: None,
                }),
            })
        }
    }
}

fn config() -> ClientConfig {
    ClientConfig {
        operation_timeout: Duration::from_secs(1),
        retry_backoff: Duration::from_millis(1),
        ..ClientConfig::default()
    }
}

#[tokio::test]
async fn redirect_refreshes_route_without_changing_logical_identity() {
    let source = Arc::new(ScriptedCatalog {
        generations: Mutex::new(vec![catalog(1, 1, 3), catalog(2, 2, 4)]),
    });
    let transport = Arc::new(ScriptedTransport::default());
    let client = ChunkKvClient::new(config(), source, transport.clone()).unwrap();
    client.refresh_catalog().await.unwrap();
    let response = client
        .put(b"object".to_vec(), b"metadata".to_vec())
        .await
        .unwrap();
    assert!(response.result.is_ok());

    let requests = transport.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].0, "owner-1");
    assert_eq!(requests[1].0, "owner-2");
    assert_eq!(requests[0].1.routing.request_id, requests[1].1.routing.request_id);
    assert_eq!(requests[0].1.operation, requests[1].1.operation);
    assert_eq!(requests[0].1.routing.owner_epoch, 3);
    assert_eq!(requests[1].1.routing.owner_epoch, 4);
}

#[tokio::test]
async fn cold_client_needs_group_zero_but_warm_client_routes_from_cache() {
    let source = Arc::new(ScriptedCatalog {
        generations: Mutex::new(vec![catalog(2, 2, 4)]),
    });
    let transport = Arc::new(ScriptedTransport::default());
    let client = ChunkKvClient::new(config(), source, transport).unwrap();
    client.refresh_catalog().await.unwrap();
    assert!(client.put(b"object".to_vec(), b"metadata".to_vec()).await.is_ok());

    let cold_source = Arc::new(ScriptedCatalog {
        generations: Mutex::new(Vec::new()),
    });
    let cold = ChunkKvClient::new(config(), cold_source, Arc::new(ScriptedTransport::default())).unwrap();
    assert!(matches!(
        cold.get(b"object".to_vec(), None).await,
        Err(ClientError::CatalogUnavailable(_))
    ));
}
