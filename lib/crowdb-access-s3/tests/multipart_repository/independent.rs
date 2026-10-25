// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{
    fmt::Write as _,
    sync::{atomic::Ordering, Arc},
};

use crowdb_access_s3::metadata::{ChunkKvMetadataStore, MultipartRepository, MultipartSessionRecord};
use crowdb_protocol::chunk_kv::RpcValue;

use super::{
    actor::TestProbe, part, repository_with_probe, CompletionPart, MetadataKey, MultipartPartRecord,
    MultipartPhase, ObjectRecord, TenantId,
};

#[path = "independent/codec.rs"]
mod codec;
#[path = "independent/faults.rs"]
mod faults;
#[path = "independent/quotas.rs"]
mod quotas;
#[path = "independent/races.rs"]
mod races;

fn tenant() -> TenantId {
    TenantId::new(b"tenant".to_vec()).unwrap()
}

fn modern_session() -> MultipartSessionRecord {
    let mut session = super::session();
    session.legacy_observation = None;
    session.max_parts = 10_000;
    session.max_part_bytes = 5 << 30;
    session.max_object_bytes = 5 << 40;
    session.max_staged_bytes = 10_000 * (5 << 30);
    session
}

fn strict_v4(session: &MultipartSessionRecord) -> MultipartSessionRecord {
    let mut bytes = session.encode().unwrap();
    bytes[4] = 4;
    MultipartSessionRecord::decode(&bytes, session.bucket_id, &session.object_key, &session.upload_id)
        .unwrap()
}

fn selection(part: &MultipartPartRecord) -> [CompletionPart; 1] {
    let mut etag = String::with_capacity(32);
    for byte in part.raw_md5 {
        write!(&mut etag, "{byte:02x}").unwrap();
    }
    [CompletionPart {
        number: part.number,
        etag,
    }]
}

struct TestIndependent {
    repository: Arc<MultipartRepository>,
    probe: Arc<TestProbe>,
    store: Arc<ChunkKvMetadataStore>,
    session: MultipartSessionRecord,
}

impl TestIndependent {
    async fn new() -> Self {
        Self::with_session(modern_session()).await
    }

    async fn with_session(session: MultipartSessionRecord) -> Self {
        let (repository, probe, store) = repository_with_probe().await;
        repository.begin(&session).await.unwrap();
        Self {
            repository: Arc::new(repository),
            probe,
            store,
            session,
        }
    }

    fn upload_key(&self) -> Vec<u8> {
        MetadataKey::multipart_session(&tenant(), self.session.bucket_id, &self.session.upload_id)
    }

    fn pointer_key(&self, number: u16) -> Vec<u8> {
        MetadataKey::multipart_part(&tenant(), self.session.bucket_id, &self.session.upload_id, number)
            .unwrap()
    }

    fn generation_key(&self, number: u16, revision: u64) -> Vec<u8> {
        MetadataKey::multipart_part_generation(
            &tenant(),
            self.session.bucket_id,
            &self.session.upload_id,
            number,
            revision,
        )
        .unwrap()
    }

    fn delta(&self, before: (usize, usize)) -> (usize, usize) {
        let after = self.probe.counts();
        (after.0 - before.0, after.1 - before.1)
    }

    fn assert_no_upload_writes(&self, start: usize) {
        assert!(!self.probe.operation_log()[start..]
            .iter()
            .any(|entry| entry.mutation && entry.key == self.upload_key()));
    }

    fn restart(&self) -> MultipartRepository {
        MultipartRepository::new(self.store.clone(), tenant())
    }

    async fn raw_upload(&self) -> Vec<RpcValue> {
        let prefix =
            MetadataKey::multipart_upload_prefix(&tenant(), self.session.bucket_id, &self.session.upload_id);
        let mut end = prefix.clone();
        end.push(u8::MAX);
        self.store.scan(prefix, end, 100, 1024 * 1024).await.unwrap()
    }

    async fn assert_object(&self, part: &MultipartPartRecord) {
        let key = MetadataKey::object(&tenant(), self.session.bucket_id, &self.session.object_key).unwrap();
        let value = self.store.get(key).await.unwrap().unwrap();
        let object = ObjectRecord::decode(&value.value).unwrap();
        assert_eq!(object.bucket_id, self.session.bucket_id);
        assert_eq!(object.key, self.session.object_key);
        assert_eq!(object.logical_length, part.length);
        assert_eq!(
            bincode::deserialize::<Vec<crowdb_protocol::chunkdb::rpc::Location>>(&object.data_reference)
                .unwrap(),
            part.locations
        );
        assert_eq!(object.etag, format!("{:x}-1", md5::compute(part.raw_md5)));
    }
}

#[tokio::test]
async fn modern_part_is_three_reads_two_conditional_writes_without_upload_mutation() {
    let test = TestIndependent::new().await;
    let before = test.probe.counts();
    let start = test.probe.operation_log().len();
    let saved = test
        .repository
        .put_stream_part(&test.session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(test.delta(before), (3, 2));
    test.assert_no_upload_writes(start);
    let log = test.probe.operation_log();
    let writes: Vec<_> = log[start..]
        .iter()
        .filter(|entry| entry.mutation)
        .map(|entry| entry.key.clone())
        .collect();
    assert_eq!(writes, [test.generation_key(1, 1), test.pointer_key(1)]);
    assert_eq!(saved.raw_md5, part().raw_md5);
    let open = test.repository.load(&test.session).await.unwrap().unwrap();
    assert_eq!(open.revision, 1);
    assert_eq!((open.part_count, open.staged_bytes), (0, 0));
    assert!(open.pending.is_none());
}

#[tokio::test]
async fn fresh_matching_part_keeps_two_read_no_write_replay() {
    let test = TestIndependent::new().await;
    let saved = test
        .repository
        .put_stream_part(&test.session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    let before = test.probe.counts();
    let start = test.probe.operation_log().len();
    let mut relocated = part();
    relocated.locations[0].offset += 390;
    assert_eq!(
        test.repository
            .put_stream_part(&test.session, &relocated, 111)
            .await
            .unwrap(),
        Some(saved)
    );
    assert_eq!(test.delta(before), (2, 0));
    assert_eq!(test.probe.operation_log().len() - start, 2);
}

#[tokio::test]
async fn distinct_part_numbers_share_no_upload_cas_or_counter() {
    let test = TestIndependent::new().await;
    let before = test.probe.counts();
    let start = test.probe.operation_log().len();
    let mut writes = tokio::task::JoinSet::new();
    for number in 1..=6 {
        let repository = test.repository.clone();
        let session = test.session.clone();
        writes.spawn(async move {
            let mut value = part();
            value.number = number;
            repository
                .put_stream_part(&session, &value, 110)
                .await
                .unwrap()
                .unwrap()
        });
    }
    let mut numbers = Vec::new();
    while let Some(result) = writes.join_next().await {
        numbers.push(result.unwrap().number);
    }
    numbers.sort_unstable();
    assert_eq!(numbers, [1, 2, 3, 4, 5, 6]);
    assert_eq!(test.delta(before), (18, 12));
    test.assert_no_upload_writes(start);
    let listed = test.repository.list_parts(&test.session, 0, 10).await.unwrap();
    assert_eq!(
        listed.parts.iter().map(|part| part.number).collect::<Vec<_>>(),
        numbers
    );
}

#[tokio::test]
async fn modern_complete_reads_selected_generations_without_current_pointer_reloads() {
    use super::retrieval_storage::TestReadStorage;
    use crowdb_access_s3::{execution::S3Executor, storage::S3StorageClients};
    use crowdb_kv_client::{ClientConfig, CrowdbKvClient};

    let mut session = modern_session();
    session.expires_ms = u64::MAX;
    let test = TestIndependent::with_session(session).await;
    test.repository
        .put_stream_part(&test.session, &part(), 110)
        .await
        .unwrap();
    let chunks = Arc::new(TestReadStorage { queries: 0.into() });
    let executor = S3Executor::new(
        S3StorageClients {
            control: Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new()))),
            metadata: test.store.clone(),
            chunks: Arc::new(chunks.client()),
        },
        tenant(),
    );
    let before = test.probe.counts();
    executor
        .complete_multipart(&test.session, &selection(&part()))
        .await
        .unwrap();
    assert_eq!(test.delta(before), (3, 2));
    let before = test.probe.counts();
    executor
        .complete_multipart(&test.session, &selection(&part()))
        .await
        .unwrap();
    assert_eq!(test.delta(before), (2, 1));
    assert_eq!(chunks.queries.load(Ordering::SeqCst), 0);
    test.assert_object(&part()).await;
}
