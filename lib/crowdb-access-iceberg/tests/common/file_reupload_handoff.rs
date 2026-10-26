// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    file::{canonical_file_key, DeletedFile, FileRecord, FileRepository},
    gc::{CandidatePhase, GcCandidate, GcLimits, GcPhase, GcRepository, GcStalledReason},
    key::{CatalogScope, IcebergKey, StorageKey},
    record::StorageRecord,
};
use crowdb_protocol::chunk_kv::ClientRequestId;

use crate::common::{file::TestFile, TestStore};

pub const DELETED_MS: u64 = 2_000;

#[derive(Clone, Copy)]
pub enum TestWrite {
    Task = 1,
    Claim,
    Candidate,
    Replacement,
}

#[derive(Clone, Copy)]
pub enum TestFault {
    Unknown = 1,
    Winner,
    EmptyConflict,
    Rejected,
    ChangedAuthority,
    CompleteTask,
    PausedTask,
    QuarantinedTask,
}

pub struct TestHandoffStore {
    pub inner: Arc<TestStore>,
    target: AtomicUsize,
    fault: AtomicUsize,
}

impl TestHandoffStore {
    pub fn arm(&self, target: TestWrite, fault: TestFault) {
        self.target.store(target as usize, Ordering::SeqCst);
        self.fault.store(fault as usize, Ordering::SeqCst);
    }

    pub fn reset_counts(&self) {
        self.inner.reads.store(0, Ordering::SeqCst);
        self.inner.cas_attempts.store(0, Ordering::SeqCst);
    }

    pub fn counts(&self) -> (usize, usize) {
        (
            self.inner.reads.load(Ordering::SeqCst),
            self.inner.cas_attempts.load(Ordering::SeqCst),
        )
    }

    fn fault_for(&self, key: &[u8], expected: Option<&[u8]>) -> usize {
        let target = match StorageKey::decode(key).unwrap() {
            StorageKey::Legacy(IcebergKey::Catalog { scope, .. }) => match scope {
                CatalogScope::GcTask => TestWrite::Task,
                CatalogScope::GcClaim => TestWrite::Claim,
                CatalogScope::GcCandidate => TestWrite::Candidate,
                CatalogScope::FileLocation if expected.is_some() => TestWrite::Replacement,
                _ => return 0,
            },
            StorageKey::NativeTable(_) if expected.is_some() => TestWrite::Replacement,
            _ => return 0,
        };
        if self.target.load(Ordering::SeqCst) == target as usize {
            self.fault.swap(0, Ordering::SeqCst)
        } else {
            0
        }
    }
}

fn conflicting_value(key: &[u8], bytes: &[u8], fault: usize) -> Option<Vec<u8>> {
    (fault == TestFault::Winner as usize || fault >= TestFault::ChangedAuthority as usize).then(|| {
        let key = StorageKey::decode(key).unwrap();
        let mut record = StorageRecord::decode(&key, bytes).unwrap();
        match &mut record {
            StorageRecord::GcTask(task) if fault == TestFault::ChangedAuthority as usize => {
                task.context.activation_epoch += 1;
            }
            StorageRecord::GcTask(task) if fault == TestFault::CompleteTask as usize => {
                task.phase = GcPhase::Complete;
            }
            StorageRecord::GcTask(task) if fault == TestFault::PausedTask as usize => {
                task.paused = true;
            }
            StorageRecord::GcTask(task) if fault == TestFault::QuarantinedTask as usize => {
                task.phase = GcPhase::Quarantined;
                task.quarantined_from = Some(GcPhase::Discover);
                task.stalled = GcStalledReason::Corruption;
            }
            StorageRecord::GcCandidate(candidate) if fault == TestFault::ChangedAuthority as usize => {
                candidate.file.location = candidate
                    .file
                    .location
                    .table()
                    .file("metadata/wrong.json")
                    .unwrap();
            }
            StorageRecord::GcTask(task) => task.revision += 1,
            StorageRecord::GcCandidate(candidate) => {
                if let StorageKey::Legacy(IcebergKey::Catalog {
                    scope: CatalogScope::GcClaim,
                    ..
                }) = &key
                {
                    candidate.generation = 7;
                } else {
                    candidate.revision += 1;
                    candidate.phase = CandidatePhase::Deleting;
                }
            }
            _ => {}
        }
        record.encode().unwrap()
    })
}

#[async_trait]
impl CatalogStore for TestHandoffStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        bytes: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        let fault = self.fault_for(key, expected);
        if fault == TestFault::Rejected as usize || fault == TestFault::EmptyConflict as usize {
            self.inner.cas_attempts.fetch_add(1, Ordering::SeqCst);
            return if fault == TestFault::Rejected as usize {
                Err(StoreError::Budget)
            } else {
                Ok(CasOutcome::Conflict(None))
            };
        }
        let winner = conflicting_value(key, bytes, fault);
        let result = self
            .inner
            .compare_exchange(key, expected, winner.as_deref().unwrap_or(bytes), identity)
            .await?;
        if fault == TestFault::Unknown as usize {
            return Err(StoreError::Response);
        }
        if let Some(bytes) = winner {
            return Ok(CasOutcome::Conflict(Some(StoredValue { bytes, revision: 2 })));
        }
        Ok(result)
    }
}

pub struct TestHandoff {
    pub fixture: TestFile,
    pub store: Arc<TestHandoffStore>,
    pub deleted: DeletedFile,
    pub replacement: FileRecord,
}

impl TestHandoff {
    pub async fn new(warm: bool) -> Self {
        let fixture = TestFile::new(TestStore::default()).await;
        let store = Arc::new(TestHandoffStore {
            inner: fixture.store.clone(),
            target: AtomicUsize::new(0),
            fault: AtomicUsize::new(0),
        });
        let files = FileRepository::new(store.clone());
        let old = fixture.record("metadata/object.json", b"old bytes");
        files.publish(fixture.context, &old).await.unwrap();
        let deleted = files
            .mark_deleted(fixture.context, &old, DELETED_MS)
            .await
            .unwrap();
        if warm {
            GcRepository::new(fixture.store.clone())
                .admit_multipart(fixture.context, DELETED_MS, GcLimits::default())
                .await
                .unwrap();
        }
        let replacement = fixture.record("metadata/object.json", b"new bytes");
        store.reset_counts();
        Self {
            fixture,
            store,
            deleted,
            replacement,
        }
    }

    pub async fn publish(&self) -> Result<FileRecord, crowdb_access_iceberg::catalog::CatalogError> {
        FileRepository::new(self.store.clone())
            .publish(self.fixture.context, &self.replacement)
            .await
    }

    pub fn authority(&self) -> StorageRecord {
        let key = canonical_file_key(&self.replacement.location);
        let values = self.fixture.store.values.load();
        StorageRecord::decode(&key, &values.get(&key.encode().unwrap()).unwrap().bytes).unwrap()
    }

    pub fn candidate(&self) -> Option<GcCandidate> {
        self.gc_fact(CatalogScope::GcCandidate)
    }

    pub fn claim(&self) -> Option<GcCandidate> {
        self.gc_fact(CatalogScope::GcClaim)
    }

    fn gc_fact(&self, expected: CatalogScope) -> Option<GcCandidate> {
        let values = self.fixture.store.values.load();
        values.iter().find_map(|(key, value)| {
            let IcebergKey::Catalog { scope, .. } = IcebergKey::decode(key).ok()? else {
                return None;
            };
            if scope != expected {
                return None;
            }
            let StorageRecord::GcCandidate(candidate) =
                StorageRecord::decode(&IcebergKey::decode(key).unwrap(), &value.bytes).unwrap()
            else {
                panic!("expected GC resource fact");
            };
            Some(*candidate)
        })
    }

    pub fn assert_handoff(&self) -> GcCandidate {
        let candidate = self.candidate().unwrap();
        assert_eq!(candidate.file, self.deleted.file);
        assert_eq!(candidate.first_seen_ms, DELETED_MS);
        assert_eq!(candidate.not_before_ms, DELETED_MS + 20 * 60 * 1_000);
        let claim_key = candidate.claim_key();
        let values = self.fixture.store.values.load();
        let StorageRecord::GcCandidate(claim) = StorageRecord::decode(
            &claim_key,
            &values.get(&claim_key.encode().unwrap()).unwrap().bytes,
        )
        .unwrap() else {
            panic!("expected claim");
        };
        assert_eq!(claim.file, candidate.file);
        assert_eq!(claim.key(), candidate.key());
        candidate
    }
}
