// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use crowdb_access_multipart::SelectedPart;
use crowdb_access_s3::metadata::{
    BucketId, ChunkKvMetadataStore, CompletionPart, MetadataKey, MultipartPartRecord, MultipartPhase,
    MultipartRepository, MultipartRepositoryError, MultipartSessionRecord, ObjectRecord, TenantId,
};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogSource, ChunkKvTransport, ClientConfig, ClientError, Result,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, ChunkKvResponse, Id128, KeyRange, OperationResult, OwnerDescriptor,
    PartitionArtifact, PointOperation, PointRequest, RpcCompareCondition, RpcValue,
};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::common::ChunkId;
use tokio::sync::{mpsc, oneshot};

struct Catalog(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>);

#[async_trait]
impl ChunkKvRangeCatalogSource for Catalog {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        Ok((self.0.clone(), self.1.clone()))
    }
}

struct ActorTransport(mpsc::UnboundedSender<(PointOperation, oneshot::Sender<Result<ChunkKvResponse>>)>);

#[async_trait]
impl ChunkKvTransport for ActorTransport {
    async fn point(&self, _: &str, request: &PointRequest) -> Result<ChunkKvResponse> {
        let (response, receiver) = oneshot::channel();
        self.0
            .send((request.operation.clone(), response))
            .expect("actor is running");
        receiver.await.expect("actor replies")
    }

    async fn seek(&self, _: &str, _: &crowdb_protocol::chunk_kv::SeekRequest) -> Result<ChunkKvResponse> {
        unreachable!()
    }

    async fn scan(&self, _: &str, _: &crowdb_protocol::chunk_kv::ScanRequest) -> Result<ChunkKvResponse> {
        unreachable!()
    }
}

fn reply(operation: PointOperation, values: &mut HashMap<Vec<u8>, RpcValue>) -> ChunkKvResponse {
    let result = match operation {
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

async fn repository() -> (MultipartRepository, Arc<AtomicBool>, Arc<ChunkKvMetadataStore>) {
    let (sender, mut receiver) =
        mpsc::unbounded_channel::<(PointOperation, oneshot::Sender<Result<ChunkKvResponse>>)>();
    let lose_reply = Arc::new(AtomicBool::new(false));
    let actor_lose_reply = Arc::clone(&lose_reply);
    tokio::spawn(async move {
        let mut values = HashMap::new();
        while let Some((operation, response)) = receiver.recv().await {
            let is_mutation = !matches!(operation, PointOperation::Get { .. });
            let result = reply(operation, &mut values);
            if is_mutation && actor_lose_reply.swap(false, Ordering::SeqCst) {
                let _ = response.send(Err(ClientError::Transport("committed reply lost".into())));
            } else {
                let _ = response.send(Ok(result));
            }
        }
    });
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
                ..ClientConfig::default()
            },
            Arc::new(Catalog(head, vec![page])),
            Arc::new(ActorTransport(sender)),
        )
        .unwrap(),
    );
    client.refresh_catalog().await.unwrap();
    let store = Arc::new(ChunkKvMetadataStore::new(client));
    (
        MultipartRepository::new(Arc::clone(&store), TenantId::new(b"tenant".to_vec()).unwrap()),
        lose_reply,
        store,
    )
}

fn session() -> MultipartSessionRecord {
    MultipartSessionRecord {
        bucket_id: BucketId::new([3; 16]),
        object_key: b"object".to_vec(),
        upload_id: [7; 16],
        revision: 1,
        phase: MultipartPhase::Open,
        created_ms: 100,
        expires_ms: 200,
        content_type: "application/octet-stream".into(),
        max_parts: 10,
        max_part_bytes: 100,
        max_object_bytes: 500,
        max_staged_bytes: 1_000,
        part_count: 0,
        staged_bytes: 0,
        selection: None,
        completion_request_digest: None,
        publication_ms: None,
        object_predecessor: None,
        etag: None,
    }
}

fn part() -> MultipartPartRecord {
    MultipartPartRecord {
        bucket_id: BucketId::new([3; 16]),
        upload_id: [7; 16],
        number: 1,
        revision: 1,
        modified_ms: 0,
        length: 5,
        raw_md5: [9; 16],
        locations: vec![Location {
            chunk_id: Some(ChunkId { high: 1, low: 2 }),
            offset: 10,
            length: 39,
            logical_offset: 0,
            logical_length: 5,
        }],
    }
}

#[tokio::test]
async fn session_cas_and_independent_part_replacement_obey_the_freeze() {
    let (repository, lose_reply, _) = repository().await;
    let session = session();
    assert_eq!(repository.begin(&session).await.unwrap(), session);
    assert_eq!(repository.begin(&session).await.unwrap(), session);
    let mut changed = session.clone();
    changed.content_type = "text/plain".into();
    assert!(matches!(
        repository.begin(&changed).await,
        Err(MultipartRepositoryError::Conflict)
    ));

    let first = repository
        .put_stream_part(&session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.revision, 1);
    let second = repository
        .put_stream_part(&session, &part(), 111)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.revision, 2);
    assert_eq!(repository.part(&session, 1).await.unwrap(), Some(second));
    assert_eq!(
        repository.part_generation(&session, 1, 1).await.unwrap(),
        Some(first)
    );

    let mut frozen = session.clone();
    frozen.revision = 2;
    frozen.phase = MultipartPhase::Completing;
    frozen.part_count = 1;
    frozen.staged_bytes = 5;
    frozen.selection = Some(vec![SelectedPart {
        number: 1,
        revision: 2,
        digest: [4; 32],
    }]);
    frozen.completion_request_digest = Some([5; 32]);
    frozen.publication_ms = None;
    lose_reply.store(true, Ordering::SeqCst);
    assert!(repository.exchange(&session, &frozen).await.unwrap());
    assert!(repository.exchange(&session, &frozen).await.unwrap());
    assert!(matches!(
        repository.put_stream_part(&session, &part(), 112).await,
        Err(MultipartRepositoryError::Conflict)
    ));
}

#[tokio::test]
async fn completion_freezes_exact_part_revision_and_replays_the_same_request() {
    let (repository, _, store) = repository().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    let first = repository
        .put_stream_part(&session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    let requested = [CompletionPart {
        number: 1,
        etag: format!("\"{}\"", "09".repeat(16)),
    }];
    let frozen = repository
        .freeze_completion(&session, &requested, 120)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frozen.phase, MultipartPhase::Publishing);
    assert_eq!(frozen.selection.as_ref().unwrap()[0].revision, first.revision);
    assert!(frozen.etag.as_ref().unwrap().ends_with("-1"));
    assert_eq!(
        repository
            .freeze_completion(&session, &requested, 121)
            .await
            .unwrap(),
        Some(frozen.clone())
    );
    assert!(matches!(
        repository.put_stream_part(&session, &part(), 122).await,
        Err(MultipartRepositoryError::Conflict)
    ));
    let published = repository.publish_completion(&frozen).await.unwrap();
    assert_eq!(published.phase, MultipartPhase::Published);
    assert_eq!(repository.publish_completion(&session).await.unwrap(), published);
    let key = MetadataKey::object(
        &TenantId::new(b"tenant".to_vec()).unwrap(),
        session.bucket_id,
        &session.object_key,
    )
    .unwrap();
    let object = ObjectRecord::decode(&store.get(key).await.unwrap().unwrap().value).unwrap();
    assert_eq!(object.logical_length, 5);
    assert_eq!(object.etag, frozen.etag.unwrap());
    assert_eq!(object.checksum.len(), 18);
    let locations: Vec<Location> = bincode::deserialize(&object.data_reference).unwrap();
    assert_eq!(locations, part().locations);
}

#[tokio::test]
async fn completion_rejects_undersized_nonfinal_and_wrong_etag() {
    let (repository, _, _) = repository().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let mut second = part();
    second.number = 2;
    repository.put_stream_part(&session, &second, 111).await.unwrap();
    let etag = "09".repeat(16);
    let request = [
        CompletionPart {
            number: 1,
            etag: etag.clone(),
        },
        CompletionPart { number: 2, etag },
    ];
    assert!(matches!(
        repository.freeze_completion(&session, &request, 120).await,
        Err(MultipartRepositoryError::EntityTooSmall)
    ));
    let wrong = [CompletionPart {
        number: 1,
        etag: "00".repeat(16),
    }];
    assert!(matches!(
        repository.freeze_completion(&session, &wrong, 120).await,
        Err(MultipartRepositoryError::InvalidPart)
    ));
    assert_eq!(repository.load(&session).await.unwrap(), Some(session));
}

#[tokio::test]
async fn publication_recovers_a_lost_reply_without_replacing_a_competing_object() {
    let (repository, lose_reply, store) = repository().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let request = [CompletionPart {
        number: 1,
        etag: "09".repeat(16),
    }];
    let frozen = repository
        .freeze_completion(&session, &request, 120)
        .await
        .unwrap()
        .unwrap();
    lose_reply.store(true, Ordering::SeqCst);
    let published = repository.publish_completion(&frozen).await.unwrap();
    assert_eq!(published.phase, MultipartPhase::Published);
    assert_eq!(repository.publish_completion(&frozen).await.unwrap(), published);

    let mut second = session.clone();
    second.upload_id = [8; 16];
    repository.begin(&second).await.unwrap();
    repository
        .put_stream_part(&second, &part_for(&second), 130)
        .await
        .unwrap();
    let frozen_second = repository
        .freeze_completion(&second, &request, 140)
        .await
        .unwrap()
        .unwrap();
    let key = MetadataKey::object(
        &TenantId::new(b"tenant".to_vec()).unwrap(),
        second.bucket_id,
        &second.object_key,
    )
    .unwrap();
    let previous = store.get(key.clone()).await.unwrap().unwrap();
    assert!(store
        .compare_exchange(key.clone(), previous.value, b"competing generation".to_vec())
        .await
        .unwrap());
    assert!(matches!(
        repository.publish_completion(&frozen_second).await,
        Err(MultipartRepositoryError::Conflict)
    ));
    assert_eq!(
        store.get(key).await.unwrap().unwrap().value,
        b"competing generation"
    );
}

#[tokio::test]
async fn frozen_part_generation_survives_a_late_pointer_change() {
    let (repository, _, store) = repository().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let selected = repository
        .put_stream_part(&session, &part(), 111)
        .await
        .unwrap()
        .unwrap();
    let request = [CompletionPart {
        number: 1,
        etag: "09".repeat(16),
    }];
    let frozen = repository
        .freeze_completion(&session, &request, 120)
        .await
        .unwrap()
        .unwrap();
    let pointer_key = MetadataKey::multipart_part(
        &TenantId::new(b"tenant".to_vec()).unwrap(),
        session.bucket_id,
        &session.upload_id,
        1,
    )
    .unwrap();
    let mut late = selected.clone();
    late.revision = 3;
    late.locations[0].offset = 77;
    let previous = store.get(pointer_key.clone()).await.unwrap().unwrap();
    assert!(store
        .compare_exchange(pointer_key, previous.value, late.encode().unwrap())
        .await
        .unwrap());
    let published = repository.publish_completion(&frozen).await.unwrap();
    assert_eq!(published.phase, MultipartPhase::Published);
}

fn part_for(session: &MultipartSessionRecord) -> MultipartPartRecord {
    let mut value = part();
    value.upload_id = session.upload_id;
    value
}
