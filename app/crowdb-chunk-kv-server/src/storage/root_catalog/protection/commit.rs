// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::record::{encode_words, operation_key};
use super::{catalog_key, KvRootCatalogStore};
use bytes::Bytes;
use crowdb_paxos_kv_client::BatchOp;
use crowdb_tree_ffi::chunk::protection::CandidateState;
use crowdb_tree_ffi::chunk::stage::StageId;
use crowdb_tree_ffi::{crc32c, CtError};

impl KvRootCatalogStore {
    pub(super) async fn commit_candidate(&self, id: StageId) -> Result<(), CtError> {
        self.authority_mutation(|| async {
            let mut authority = self.protection_authority(true).await?;
            let mut record = self.candidate_record(id).await?.ok_or(CtError::NotFound)?;
            if record.status.state == CandidateState::Committed {
                return Ok(());
            }
            if record.status.state != CandidateState::Ready
                || authority.generation.checked_add(1) != Some(record.status.generation)
            {
                return Err(CtError::Unavailable.into());
            }
            let (manifest, _) = self
                .get(&operation_key(self.tree_id, b"candidate-manifest", id))
                .await
                .map_err(|_| CtError::Unavailable)?
                .ok_or(CtError::Corruption)?;
            if manifest.len() as u64 != record.manifest_bytes || crc32c(&manifest) != record.manifest_checksum
            {
                return Err(CtError::Corruption.into());
            }
            record.status.state = CandidateState::Committed;
            authority.value = Bytes::copy_from_slice(&super::super::encode_authority(
                authority.epoch,
                record.status.generation,
            ));
            let ops = vec![
                BatchOp::Put {
                    key: Bytes::from(catalog_key(self.tree_id, b"current", 0)),
                    value: manifest.clone(),
                },
                BatchOp::Put {
                    key: Bytes::from(catalog_key(self.tree_id, b"manifest", record.status.generation)),
                    value: manifest,
                },
                BatchOp::Put {
                    key: Bytes::from(catalog_key(
                        self.tree_id,
                        b"manifest-candidate",
                        record.status.generation,
                    )),
                    value: Bytes::from(encode_words(&[
                        id.owner_epoch,
                        id.operation_high,
                        id.operation_low,
                    ])),
                },
                BatchOp::Put {
                    key: Bytes::from(operation_key(self.tree_id, b"candidate", id)),
                    value: Bytes::from(record.encode()),
                },
            ];
            // Intents already own every reference. This CAS changes only the root
            // and candidate state, so publication never creates an unprotected gap.
            self.protection_write(authority, ops).await
        })
        .await
    }

    pub(super) async fn abort_candidate(&self, id: StageId) -> Result<(), CtError> {
        self.authority_mutation(|| async {
            let authority = self.protection_authority(true).await?;
            let mut record = self.candidate_record(id).await?.ok_or(CtError::NotFound)?;
            if record.status.state == CandidateState::Aborted {
                return Ok(());
            }
            if !matches!(
                record.status.state,
                CandidateState::Preparing | CandidateState::Ready
            ) {
                return Err(CtError::Unavailable.into());
            }
            record.status.state = CandidateState::Aborted;
            self.protection_write(authority, self.terminal_candidate_ops(&record))
                .await
        })
        .await
    }
}
