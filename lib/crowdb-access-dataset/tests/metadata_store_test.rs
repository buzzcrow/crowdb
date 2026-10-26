// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_access_dataset::{
    ChunkKvDatasetStore, DatasetStore, MetadataScan, MetadataScanBudget, ReadCancellation,
};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogSource, ChunkKvTransport, ClientConfig, Result,
};
use crowdb_protocol::chunk_kv::*;
use crowdb_protocol::chunk_stream::StreamName;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
type Boundary<'a> = (&'a [u8], Option<&'a [u8]>, u64);

fn catalog(
    generation: u64,
    boundaries: &[Boundary<'_>],
) -> (ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>) {
    let entries = boundaries
        .iter()
        .map(|(start, end, owner)| ChunkKvRangeCatalogEntry {
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
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: generation,
                stream_name: StreamName {
                    high: generation,
                    low: *owner,
                },
                tail_overlay: None,
            },
            transition_id: None,
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

struct TestCatalog;
#[async_trait]
impl ChunkKvRangeCatalogSource for TestCatalog {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        Ok(catalog(1, &[(b"", None, 1)]))
    }
}
struct TestTransport {
    stall: AtomicBool,
    calls: AtomicUsize,
    deletion: AtomicUsize,
    values: Vec<RpcValue>,
}
#[async_trait]
impl ChunkKvTransport for TestTransport {
    async fn point(&self, _: &str, request: &PointRequest) -> Result<ChunkKvResponse> {
        assert!(matches!(request.operation, PointOperation::Delete { .. }));
        self.calls.fetch_add(1, Ordering::Relaxed);
        let result = match self.deletion.load(Ordering::Relaxed) {
            0 => Ok(OperationResult::Mutation {
                applied: true,
                revision: Some(8),
                observed: None,
            }),
            1 => Err(RpcFailure {
                code: ChunkKvRpcErrorCode::InvalidRequest,
                message: "delete rejected".into(),
                retry_after_ms: None,
                latest_map_revision: None,
                owner_hint: None,
            }),
            2 => Ok(OperationResult::Mutation {
                applied: false,
                revision: None,
                observed: None,
            }),
            3 => Ok(OperationResult::Value(None)),
            _ => {
                return Err(crowdb_chunk_kv_client::ClientError::Transport(
                    "lost reply".into(),
                ))
            }
        };
        Ok(ChunkKvResponse {
            map_revision: 1,
            journal_position: None,
            result,
        })
    }
    async fn scan(&self, _: &str, _: &ScanRequest) -> Result<ChunkKvResponse> {
        panic!("unbounded scan")
    }
    async fn scan_bounded(&self, _: &str, request: &ScanRequest, budget: usize) -> Result<ChunkKvResponse> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.stall.load(Ordering::Relaxed) {
            std::future::pending::<()>().await;
        }
        let mut items = Vec::new();
        let mut bytes = 0;
        let mut truncated = false;
        for value in &self.values {
            if request.start.as_ref().is_some_and(|key| value.key < *key)
                || request.end.as_ref().is_some_and(|key| value.key >= *key)
                || request
                    .continuation
                    .as_ref()
                    .is_some_and(|cursor| value.key <= cursor.last_key)
            {
                continue;
            }
            if items.len() == request.limit as usize || bytes + value.key.len() + value.value.len() > budget {
                if items.is_empty() {
                    return Err(crowdb_chunk_kv_client::ClientError::TooLarge);
                }
                truncated = true;
                break;
            }
            bytes += value.key.len() + value.value.len();
            items.push(value.clone());
        }
        let continuation = truncated.then(|| ScanContinuation {
            direction: ScanDirection::Forward,
            last_key: items.last().unwrap().key.clone(),
            partition_id: request.routing.partition_id,
            owner_epoch: request.routing.owner_epoch,
            map_revision: request.routing.map_revision,
        });
        Ok(ChunkKvResponse {
            map_revision: 1,
            journal_position: None,
            result: Ok(OperationResult::Scan { items, continuation }),
        })
    }
}
fn fixture() -> (ChunkKvDatasetStore, Arc<TestTransport>) {
    let transport = Arc::new(TestTransport {
        stall: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
        deletion: AtomicUsize::new(0),
        values: [b"p\0a".as_slice(), b"p\0b", b"q\0a"]
            .into_iter()
            .map(|key| RpcValue {
                key: key.to_vec(),
                value: vec![1; 8],
                revision: 7,
            })
            .collect(),
    });
    let client = ChunkKvClient::new(
        ClientConfig {
            max_attempts: 1,
            ..ClientConfig::default()
        },
        Arc::new(TestCatalog),
        transport.clone(),
    )
    .unwrap();
    (ChunkKvDatasetStore::new(Arc::new(client)), transport)
}
fn budget() -> MetadataScanBudget {
    MetadataScanBudget {
        items: 1,
        bytes: 64,
        timeout: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn metadata_delete_requires_an_applied_result_without_confirmation_reads() {
    for outcome in 0..=4 {
        let (store, transport) = fixture();
        transport.deletion.store(outcome, Ordering::Relaxed);
        let result = store.delete(b"retired metadata").await;
        assert_eq!(result.is_ok(), outcome == 0, "outcome={outcome}, {result:?}");
        assert_eq!(transport.calls.load(Ordering::Relaxed), 1);
    }
}
#[tokio::test]
async fn metadata_pages_preserve_binary_prefix_cursor_and_exact_keys() {
    let (store, _) = fixture();
    let cancel = ReadCancellation::new();
    let mut request = MetadataScan {
        prefix: b"p\0".to_vec(),
        after: None,
        budget: budget(),
    };
    let first = store.scan_page(&request, &cancel).await.unwrap();
    assert_eq!(first.entries[0].0, b"p\0a");
    request.after = first.after;
    let second = store.scan_page(&request, &cancel).await.unwrap();
    assert_eq!(second.entries[0].0, b"p\0b");
    assert!(second.after.is_none());
    request.after = Some(second.entries[0].0.clone());
    let end = store.scan_page(&request, &cancel).await.unwrap();
    assert!(end.entries.is_empty());
    assert!(end.after.is_none());
    assert!(store
        .get_bounded(b"p", budget(), &cancel)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .get_bounded(b"p\0a", budget(), &cancel)
            .await
            .unwrap()
            .unwrap()
            .revision,
        7
    );
}
#[tokio::test]
async fn metadata_terminal_page_preserves_the_complete_raw_prefix_without_an_empty_probe() {
    let (store, transport) = fixture();
    let page = store
        .scan_page(
            &MetadataScan {
                prefix: b"p\0".to_vec(),
                after: None,
                budget: MetadataScanBudget {
                    items: 256,
                    ..budget()
                },
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        page.entries
            .iter()
            .map(|(key, _)| key.as_slice())
            .collect::<Vec<_>>(),
        [b"p\0a".as_slice(), b"p\0b"]
    );
    assert!(page.after.is_none());
    assert_eq!(transport.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn metadata_limits_and_cancellation_never_fall_back_to_unbounded_reads() {
    let (store, transport) = fixture();
    let cancel = ReadCancellation::new();
    let mut request = MetadataScan {
        prefix: b"p\0".to_vec(),
        after: Some(b"q\0a".to_vec()),
        budget: budget(),
    };
    assert!(store.scan_page(&request, &cancel).await.is_err());
    assert_eq!(transport.calls.load(Ordering::Relaxed), 0);
    request.after = None;
    request.budget.bytes = 1;
    assert!(store.scan_page(&request, &cancel).await.is_err());
    cancel.cancel();
    let before = transport.calls.load(Ordering::Relaxed);
    assert!(store.get_bounded(b"p\0a", budget(), &cancel).await.is_err());
    assert_eq!(transport.calls.load(Ordering::Relaxed), before);
}

#[tokio::test]
async fn metadata_inflight_reads_observe_deadline_and_cancellation() {
    let (store, transport) = fixture();
    transport.stall.store(true, Ordering::Relaxed);
    let cancel = ReadCancellation::new();
    let short = MetadataScanBudget {
        timeout: Duration::from_millis(20),
        ..budget()
    };
    assert!(store.get_bounded(b"p\0a", short, &cancel).await.is_err());
    let read = store.get_bounded(b"p\0a", budget(), &cancel);
    let stop = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_millis(200), async { tokio::join!(read, stop) })
        .await
        .unwrap();
    assert!(matches!(
        result,
        Err(crowdb_access_dataset::StoreError::Invalid(
            crowdb_access_dataset::DatasetError::ReadCancelled
        ))
    ));
}

#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;

#[tokio::test]
async fn existing_identity_keys_remain_discoverable_after_authority_recreation() {
    use crowdb_access_dataset::{DatasetAuthority, DatasetIdentity, NamespacePath};
    let store = Arc::new(dataset_store::TestDatasetStore::default());
    let identity = DatasetIdentity::new(NamespacePath::root(), "existing").unwrap();
    let id = DatasetAuthority::new(store.clone())
        .create_dataset(identity.clone())
        .await
        .unwrap();
    let page = store
        .scan_page(
            &MetadataScan {
                prefix: b"DS\0D\0".to_vec(),
                after: None,
                budget: MetadataScanBudget {
                    bytes: 4096,
                    ..budget()
                },
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(
        DatasetAuthority::new(store)
            .get_dataset(&identity)
            .await
            .unwrap()
            .id,
        id
    );
    let invalid = MetadataScan {
        prefix: vec![0; 65_537],
        after: None,
        budget: budget(),
    };
    assert!(invalid.validate().is_err());
    for invalid_budget in [
        MetadataScanBudget { items: 0, ..budget() },
        MetadataScanBudget { bytes: 0, ..budget() },
        MetadataScanBudget {
            timeout: Duration::ZERO,
            ..budget()
        },
    ] {
        assert!(invalid_budget.validate().is_err());
    }
}
