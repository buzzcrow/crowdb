// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{common::TestStore, file, fixtures};
use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    file::{
        FileContent, FileIdentity, MultipartMutation, MultipartPart, MultipartPhase, MultipartRepository,
        MultipartSelection, MultipartSession, MultipartStreamPart,
    },
    key::{CatalogScope, FileId, IcebergKey},
    operation::mutation_identity,
    record::StorageRecord,
};
use crowdb_protocol::chunk_kv::ClientRequestId;
use crowdb_protocol::{chunkdb::rpc::Location, common::ChunkId};
use md5::{Digest, Md5};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub struct TestSessionConflictStore {
    pub inner: Arc<TestStore>,
    pub armed: AtomicBool,
}

#[async_trait]
impl CatalogStore for TestSessionConflictStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        let decoded = IcebergKey::decode(key)?;
        if matches!(
            decoded,
            IcebergKey::Catalog {
                scope: CatalogScope::MultipartSession,
                ..
            }
        ) && self.armed.swap(false, Ordering::SeqCst)
        {
            let StorageRecord::MultipartSession(mut session) =
                StorageRecord::decode(&decoded, expected.unwrap())?
            else {
                panic!("session precondition");
            };
            session.revision += 1;
            session.phase = MultipartPhase::Aborted;
            let bytes = StorageRecord::MultipartSession(session).encode()?;
            let outcome = self
                .inner
                .compare_exchange(key, expected, &bytes, mutation_identity(key, expected, &bytes))
                .await?;
            let CasOutcome::Applied(revision) = outcome else {
                panic!("injected winner")
            };
            return Ok(CasOutcome::Conflict(Some(StoredValue { bytes, revision })));
        }
        self.inner.compare_exchange(key, expected, value, identity).await
    }
}

pub struct TestUpload {
    pub file: file::TestFile,
    pub session: MultipartSession,
    pub repository: MultipartRepository,
}

impl TestUpload {
    pub async fn new() -> Self {
        let file = file::TestFile::new(TestStore::default()).await;
        let mut session = fixtures::session();
        session.context = file.context;
        session.owner.table = file.table;
        session.location = file.table.file("data/complete.parquet").unwrap();
        let repository = MultipartRepository::new(file.store.clone());
        repository.begin(&session, 100).await.unwrap();
        Self {
            file,
            session,
            repository,
        }
    }

    pub fn counts(&self) -> (usize, usize) {
        (
            self.file.store.reads.load(Ordering::SeqCst),
            self.file.store.cas_attempts.load(Ordering::SeqCst),
        )
    }

    pub fn delta(&self, before: (usize, usize)) -> (usize, usize) {
        let after = self.counts();
        (after.0 - before.0, after.1 - before.1)
    }

    pub fn part(&self, number: u16, identity: u64, bytes: &[u8]) -> MultipartPart {
        MultipartPart {
            upload: self.session.upload,
            number,
            revision: 1,
            modified_ms: 101,
            owner: FileIdentity {
                file: FileId::random(),
                ..self.session.owner
            },
            tree: None,
            stream: Some(MultipartStreamPart {
                length: bytes.len() as u64,
                content: FileContent::from_locations(
                    &[Location {
                        chunk_id: Some(ChunkId {
                            high: 7,
                            low: identity,
                        }),
                        offset: 100,
                        length: bytes.len() as u64 + 34,
                        logical_offset: 0,
                        logical_length: bytes.len() as u64,
                    }],
                    bytes.len() as u64,
                    format!("{:x}", Md5::digest(bytes)),
                )
                .unwrap(),
            }),
        }
    }

    pub async fn selection(&self) -> MultipartSelection {
        let mut parts = Vec::new();
        for number in 1..=2 {
            parts.push(
                self.repository
                    .put_stream_part(
                        &self.session,
                        &self.part(number, u64::from(number), b"content"),
                        101,
                    )
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        MultipartSelection::with_stream_parts(&parts).unwrap()
    }
}

pub fn applied<T>(mutation: MultipartMutation<T>) -> (MultipartSession, T) {
    match mutation {
        MultipartMutation::Applied { session, value } => (session, value),
        MultipartMutation::Conflict(observed) => panic!("unexpected conflict: {observed:?}"),
    }
}
