// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_paxos_kv_client::{BatchOp, Error, GetOutcome, Result, ScanOutcome, WriteOutcome};
use crowdb_protocol::key::{BinaryKey, StreamChunkRecordKey, StreamChunkRecordOrder};
use tokio::sync::{Mutex, Notify};

use super::MemoryState;
#[derive(Debug)]
pub(super) struct MemoryRecord {
    pub(super) value: Bytes,
    pub(super) revision: u64,
}
use crate::records::RecordBackend;

#[derive(Debug, Default)]
pub(super) struct RecordFaults {
    pub(super) fail: AtomicBool,
    pub(super) lose_response: AtomicBool,
    pub(super) pause: AtomicBool,
    pub(super) entered: Notify,
    pub(super) resume: Notify,
    pub(super) publications: AtomicU64,
    pub(super) loads: AtomicU64,
    pub(super) renewals_to_fail: AtomicU64,
    pub(super) renewals: AtomicU64,
    pub(super) fail_delete: AtomicBool,
}

pub(super) struct MemoryRecordBackend {
    state: Arc<Mutex<MemoryState>>,
    group_id: u64,
    faults: Arc<RecordFaults>,
}
impl MemoryRecordBackend {
    pub(super) fn new(state: Arc<Mutex<MemoryState>>, group_id: u64, faults: Arc<RecordFaults>) -> Self {
        Self {
            state,
            group_id,
            faults,
        }
    }
}

#[async_trait]
impl RecordBackend for MemoryRecordBackend {
    async fn get(&self, key: &[u8]) -> Result<GetOutcome> {
        self.faults.loads.fetch_add(1, Ordering::Relaxed);
        let state = self.state.lock().await;
        Ok(state.metadata_records.get(&(self.group_id, key.to_vec())).map_or(
            GetOutcome::NotFound,
            |record| GetOutcome::Found {
                value: record.value.clone(),
                revision: record.revision,
            },
        ))
    }

    async fn scan(&self, prefix: &[u8], after: &[u8], limit: u32, reverse: bool) -> Result<ScanOutcome> {
        self.faults.loads.fetch_add(1, Ordering::Relaxed);
        let state = self.state.lock().await;
        let mut found: Vec<_> = state
            .metadata_records
            .iter()
            .filter(|((group, key), _)| {
                *group == self.group_id
                    && key.starts_with(prefix)
                    && (after.is_empty()
                        || if reverse {
                            key.as_slice() < after
                        } else {
                            key.as_slice() > after
                        })
            })
            .collect();
        if reverse {
            found.reverse();
        }
        let truncated = found.len() > limit as usize;
        let items = found
            .iter()
            .take(limit as usize)
            .map(|((_, key), record)| (Bytes::copy_from_slice(key), record.value.clone()))
            .collect();
        let commit_slots = found
            .iter()
            .take(limit as usize)
            .map(|(_, record)| record.revision)
            .collect();
        Ok(ScanOutcome {
            items,
            commit_slots,
            truncated,
            timed_out: false,
            read_slot: state.metadata_revision,
            scan_cutoff: state.metadata_revision,
        })
    }
    async fn batch(&self, ops: &[BatchOp], key: &[u8], revision: u64) -> Result<WriteOutcome> {
        if !ops.iter().any(|op| match op {
            BatchOp::Put { key: candidate, .. } | BatchOp::Delete { key: candidate } => {
                candidate.as_ref() == key
            }
        }) {
            return Err(Error::Server(
                "conditional batch must mutate its guarded key".into(),
            ));
        }
        let mut state = self.state.lock().await;
        if ops.iter().any(|op| {
            matches!(op, BatchOp::Delete {key} if StreamChunkRecordKey::from_bytes(key)
            .is_ok_and(|key| matches!(key.order,StreamChunkRecordOrder::Attached(_))))
        }) && self.faults.fail_delete.swap(false, Ordering::AcqRel)
        {
            return Err(Error::Server("injected chunk record deletion failure".into()));
        }
        let renewing = ops.iter().any(|op| match op {
            BatchOp::Put { key, .. } => {
                key.starts_with(&[crowdb_protocol::key::CROWDB_KEY_MAGIC, 0, 0x1e])
                    && state
                        .metadata_records
                        .contains_key(&(self.group_id, key.to_vec()))
            }
            BatchOp::Delete { .. } => false,
        });
        if renewing
            && self
                .faults
                .renewals_to_fail
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_sub(1))
                .is_ok()
        {
            return Err(Error::CasBusy);
        }
        let tail_name = ops.iter().find_map(|op| match op {
            BatchOp::Put { key, .. } => StreamChunkRecordKey::from_bytes(key)
                .ok()
                .filter(|key| matches!(key.order, StreamChunkRecordOrder::Attached(_)))
                .map(|key| key.stream_id),
            BatchOp::Delete { .. } => None,
        });
        let publication = is_publication(&state, self.group_id, ops, key);
        if publication && self.faults.fail.swap(false, Ordering::AcqRel) {
            return Err(Error::Server(
                "injected Stream chunk record publication failure".into(),
            ));
        }
        if publication && self.faults.pause.swap(false, Ordering::AcqRel) {
            drop(state);
            self.faults.entered.notify_one();
            self.faults.resume.notified().await;
            state = self.state.lock().await;
        }
        let observed = state.metadata_records.get(&(self.group_id, key.to_vec()));
        let current = observed.map_or(0, |record| record.revision);
        if current != revision {
            return Err(Error::CasFailed {
                current_revision: current,
                observation: Some(Box::new(
                    crowdb_paxos_kv_client::KvCompareObservation::from_versioned(
                        Bytes::copy_from_slice(key),
                        observed.map(|record| (record.revision, record.value.clone())),
                    ),
                )),
            });
        }
        let next = state
            .metadata_revision
            .checked_add(1)
            .ok_or_else(|| Error::Server("test metadata revision overflows".into()))?;
        state.metadata_revision = next;
        if let Some(name) = tail_name {
            state.metadata_groups.insert(name, self.group_id);
        }
        apply_ops(&mut state, self.group_id, next, ops);
        if renewing {
            self.faults.renewals.fetch_add(1, Ordering::Relaxed);
        }
        if publication {
            self.faults.publications.fetch_add(1, Ordering::AcqRel);
            if self.faults.lose_response.swap(false, Ordering::AcqRel) {
                return Err(Error::OutcomeUnknown);
            }
        }
        Ok(WriteOutcome {
            revision: next,
            request_id: 0,
        })
    }
}

fn is_publication(state: &MemoryState, group: u64, ops: &[BatchOp], guarded: &[u8]) -> bool {
    let generation = state
        .metadata_records
        .get(&(group, guarded.to_vec()))
        .and_then(|record| crowdb_protocol::chunk_stream::decode_stream_chunk_record(&record.value).ok())
        .map_or(0, |record| record.generation);
    ops.iter().any(|op| match op {
        BatchOp::Put { key, value }
            if StreamChunkRecordKey::from_bytes(key)
                .is_ok_and(|key| matches!(key.order, StreamChunkRecordOrder::Attached(_))) =>
        {
            crowdb_protocol::chunk_stream::decode_stream_chunk_record(value)
                .is_ok_and(|record| record.generation > generation)
        }
        _ => false,
    })
}

fn apply_ops(state: &mut MemoryState, group: u64, revision: u64, ops: &[BatchOp]) {
    for op in ops {
        match op {
            BatchOp::Put { key, value } => {
                state.metadata_records.insert(
                    (group, key.to_vec()),
                    MemoryRecord {
                        value: value.clone(),
                        revision,
                    },
                );
            }
            BatchOp::Delete { key } => {
                state.metadata_records.remove(&(group, key.to_vec()));
            }
        }
    }
}
