// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::record::{decode_references, decode_words, operation_key, window_key, CandidateRecord};
use super::{catalog_key, KvRootCatalogStore};
use bytes::Bytes;
use crowdb_paxos_kv_client::BatchOp;
use crowdb_tree_ffi::chunk::protection::CandidateState;
use crowdb_tree_ffi::chunk::stage::StageId;
use crowdb_tree_ffi::CtError;

impl KvRootCatalogStore {
    pub(super) fn terminal_candidate_ops(&self, record: &CandidateRecord) -> Vec<BatchOp> {
        let mut ops = vec![BatchOp::Put {
            key: Bytes::from(operation_key(self.tree_id, b"candidate", record.status.id)),
            value: Bytes::from(record.encode()),
        }];
        if record.status.released_windows == record.status.prepared_windows {
            ops.push(BatchOp::Delete {
                key: Bytes::from(operation_key(
                    self.tree_id,
                    b"candidate-manifest",
                    record.status.id,
                )),
            });
        }
        ops
    }

    pub(super) async fn recover_candidate(&self, id: StageId) -> Result<(), CtError> {
        self.authority_mutation(|| async {
            self.require_completed_recovery().await?;
            let authority = self.protection_authority(true).await?;
            let mut record = self.candidate_record(id).await?.ok_or(CtError::NotFound)?;
            if matches!(
                record.status.state,
                CandidateState::Aborted | CandidateState::Reclaimed
            ) {
                return Ok(());
            }
            if record.status.state == CandidateState::Committed || id.owner_epoch > self.owner_epoch {
                return Err(CtError::Unavailable.into());
            }
            if id.owner_epoch == self.owner_epoch {
                let key = operation_key(self.tree_id, b"candidate-owner", id);
                let (bytes, _) = self
                    .get(&key)
                    .await
                    .map_err(|_| CtError::Unavailable)?
                    .ok_or(CtError::Unavailable)?;
                let creator = super::writer::WriterOwner::decode(&bytes)?;
                if self.writer_owner()? == Some(creator)
                    || !self.writer_is_excluded(id.owner_epoch, creator).await?
                {
                    return Err(CtError::Unavailable.into());
                }
            }
            record.status.state = CandidateState::Aborted;
            self.protection_write(authority, self.terminal_candidate_ops(&record))
                .await
        })
        .await
    }

    pub(super) async fn release_candidate(&self, id: StageId, window: u64) -> Result<(), CtError> {
        self.authority_mutation(|| async {
            let authority = self.protection_authority(true).await?;
            let mut record = self.candidate_record(id).await?.ok_or(CtError::NotFound)?;
            if !matches!(
                record.status.state,
                CandidateState::Aborted | CandidateState::Reclaimed
            ) {
                return Err(CtError::Unavailable.into());
            }
            if window < record.status.released_windows {
                return Ok(());
            }
            if window != record.status.released_windows || window >= record.status.prepared_windows {
                return Err(CtError::InvalidArgument.into());
            }
            let key = window_key(self.tree_id, id, window);
            let (bytes, _) = self
                .get(&key)
                .await
                .map_err(|_| CtError::Unavailable)?
                .ok_or(CtError::Corruption)?;
            let refs = decode_references(&bytes)?;
            let mut ops = Vec::with_capacity(refs.len() + 4);
            for reference in refs {
                let mut guard = self.load_stage_guard(reference).await?;
                guard.references = guard.references.checked_sub(1).ok_or(CtError::Corruption)?;
                ops.push(self.stage_guard_put(reference, guard));
            }
            record.status.released_windows += 1;
            if record.status.released_windows == record.status.prepared_windows {
                ops.push(BatchOp::Delete {
                    key: Bytes::from(operation_key(self.tree_id, b"candidate-manifest", id)),
                });
            }
            ops.push(BatchOp::Delete {
                key: Bytes::from(key),
            });
            ops.push(BatchOp::Put {
                key: Bytes::from(operation_key(self.tree_id, b"candidate", id)),
                value: Bytes::from(record.encode()),
            });
            self.protection_write(authority, ops).await
        })
        .await
    }

    /// Called while the caller still owns the root authority revision used to
    /// reclaim the matching immutable manifest and its generation-pin proof.
    pub(in super::super) async fn reclaim_candidate_ops(
        &self,
        generation: u64,
    ) -> Result<Vec<BatchOp>, CtError> {
        let key = catalog_key(self.tree_id, b"manifest-candidate", generation);
        let Some((value, _)) = self.get(&key).await.map_err(|_| CtError::Unavailable)? else {
            return Ok(Vec::new());
        };
        let words = decode_words::<3>(&value)?;
        let id = StageId {
            owner_epoch: words[0],
            operation_high: words[1],
            operation_low: words[2],
        };
        let mut record = self.candidate_record(id).await?.ok_or(CtError::Corruption)?;
        if record.status.state != CandidateState::Committed || record.status.generation != generation {
            return Err(CtError::Corruption);
        }
        record.status.state = CandidateState::Reclaimed;
        let mut ops = self.terminal_candidate_ops(&record);
        ops.push(BatchOp::Delete {
            key: Bytes::from(key),
        });
        Ok(ops)
    }
}
