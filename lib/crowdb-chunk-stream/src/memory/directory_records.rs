// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_kv_client::{BatchOp, Error, GetOutcome, Result, ScanOutcome, WriteOutcome};
use crowdb_protocol::key::{BinaryKey, StreamManifestHeadKey};
use tokio::sync::Mutex;

use super::MemoryState;
use crate::directory::DirectoryRecords;

#[derive(Debug)]
pub(super) struct MemoryRecord {
    pub value: Bytes,
    pub revision: u64,
}

pub(super) struct MemoryDirectoryRecords {
    state: Arc<Mutex<MemoryState>>,
    group_id: u64,
    fail_next_publish: Arc<AtomicBool>,
    metadata_publishes: Arc<AtomicU64>,
}

impl MemoryDirectoryRecords {
    pub(super) fn new(
        state: Arc<Mutex<MemoryState>>,
        group_id: u64,
        fail_next_publish: Arc<AtomicBool>,
        metadata_publishes: Arc<AtomicU64>,
    ) -> Self {
        Self {
            state,
            group_id,
            fail_next_publish,
            metadata_publishes,
        }
    }
}

#[async_trait]
impl DirectoryRecords for MemoryDirectoryRecords {
    async fn get(&self, key: &[u8]) -> Result<GetOutcome> {
        let state = self.state.lock().await;
        Ok(state.metadata_records.get(&(self.group_id, key.to_vec())).map_or(
            GetOutcome::NotFound,
            |record| GetOutcome::Found {
                value: record.value.clone(),
                revision: record.revision,
            },
        ))
    }

    async fn scan_keys(&self, prefix: &[u8], after: &[u8], limit: u32) -> Result<ScanOutcome> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        if limit == 0 || limit > u32::try_from(crate::MAX_DIRECTORY_SCAN_KEYS).expect("bounded key count") {
            return Err(Error::Server("directory key scan limit is invalid".into()));
        }
        let limit = usize::try_from(limit).expect("bounded key count");
        let state = self.state.lock().await;
        let start = if after.is_empty() {
            Included((self.group_id, prefix.to_vec()))
        } else {
            Excluded((self.group_id, after.to_vec()))
        };
        let mut records = state
            .metadata_records
            .range((start, Unbounded))
            .take_while(|((group, key), _)| *group == self.group_id && key.starts_with(prefix));
        let mut items = Vec::with_capacity(limit);
        let mut commit_slots = Vec::with_capacity(limit);
        for _ in 0..limit {
            let Some(((_, key), record)) = records.next() else {
                break;
            };
            items.push((Bytes::copy_from_slice(key), Bytes::new()));
            commit_slots.push(record.revision);
        }
        Ok(ScanOutcome {
            truncated: records.next().is_some(),
            timed_out: false,
            read_slot: state.metadata_revision,
            scan_cutoff: state.metadata_revision,
            items,
            commit_slots,
        })
    }
    async fn put_cas(&self, key: &[u8], value: &[u8], revision: u64) -> Result<WriteOutcome> {
        self.batch_write_cas(
            &[BatchOp::Put {
                key: Bytes::copy_from_slice(key),
                value: Bytes::copy_from_slice(value),
            }],
            key,
            revision,
        )
        .await
    }

    async fn batch_write_cas(&self, ops: &[BatchOp], key: &[u8], revision: u64) -> Result<WriteOutcome> {
        if !ops.iter().any(|op| match op {
            BatchOp::Put { key: candidate, .. } | BatchOp::Delete { key: candidate } => {
                candidate.as_ref() == key
            }
        }) {
            return Err(Error::Server(
                "conditional batch must mutate its guarded key".into(),
            ));
        }
        let head_name = StreamManifestHeadKey::from_bytes(key)
            .ok()
            .map(|head| head.stream_name);
        let head = head_name.is_some();
        if head && self.fail_next_publish.swap(false, Ordering::AcqRel) {
            return Err(Error::Server("injected metadata publication failure".into()));
        }
        let mut state = self.state.lock().await;
        let current = state
            .metadata_records
            .get(&(self.group_id, key.to_vec()))
            .map_or(0, |record| record.revision);
        if current != revision {
            return Err(Error::CasFailed {
                current_revision: current,
            });
        }
        let next = state
            .metadata_revision
            .checked_add(1)
            .ok_or_else(|| Error::Server("test metadata revision overflows".into()))?;
        state.metadata_revision = next;
        if let Some(name) = head_name {
            state.metadata_heads.insert(name, self.group_id);
        }
        for op in ops {
            match op {
                BatchOp::Put { key, value } => {
                    state.metadata_records.insert(
                        (self.group_id, key.to_vec()),
                        MemoryRecord {
                            value: value.clone(),
                            revision: next,
                        },
                    );
                }
                BatchOp::Delete { key } => {
                    state.metadata_records.remove(&(self.group_id, key.to_vec()));
                }
            }
        }
        if head {
            self.metadata_publishes.fetch_add(1, Ordering::AcqRel);
        }
        Ok(WriteOutcome {
            revision: next,
            request_id: 0,
        })
    }
}
