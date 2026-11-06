// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::record::{encode_references, operation_key, window_key, CandidateRecord};
use super::KvRootCatalogStore;
use bytes::Bytes;
use crowdb_paxos_kv_client::BatchOp;
use crowdb_tree_ffi::chunk::protection::{
    reference_checksum, validate_operation, CandidateState, CandidateStatus, ManifestCandidate,
    MAX_REFERENCE_WINDOW,
};
use crowdb_tree_ffi::chunk::stage::StageId;
use crowdb_tree_ffi::{crc32c, CtError};

impl KvRootCatalogStore {
    pub(super) async fn begin_candidate(&self, candidate: &ManifestCandidate) -> Result<(), CtError> {
        self.authority_mutation(|| async {
            let authority = self.protection_authority(true).await?;
            let manifest_key = operation_key(self.tree_id, b"candidate-manifest", candidate.id);
            if let Some(current) = self.candidate_record(candidate.id).await? {
                let manifest = self.get(&manifest_key).await.map_err(|_| CtError::Unavailable)?;
                if current.status.generation == candidate.expected_generation + 1
                    && current.status.reference_count == candidate.reference_count
                    && current.expected_checksum == candidate.reference_checksum
                    && current.manifest_bytes == candidate.manifest.len() as u64
                    && current.manifest_checksum == crc32c(&candidate.manifest)
                    && (manifest.as_ref().map(|(bytes, _)| bytes.as_ref())
                        == Some(candidate.manifest.as_slice())
                        || (manifest.is_none()
                            && matches!(
                                current.status.state,
                                CandidateState::Aborted | CandidateState::Reclaimed
                            )))
                {
                    return Ok(());
                }
                return Err(CtError::InvalidArgument.into());
            }
            if authority.generation != candidate.expected_generation {
                return Err(CtError::Unavailable.into());
            }
            let record = CandidateRecord {
                status: CandidateStatus {
                    id: candidate.id,
                    state: if candidate.reference_count == 0 {
                        CandidateState::Ready
                    } else {
                        CandidateState::Preparing
                    },
                    generation: candidate.expected_generation + 1,
                    reference_count: candidate.reference_count,
                    prepared_references: 0,
                    prepared_windows: 0,
                    released_windows: 0,
                },
                expected_checksum: candidate.reference_checksum,
                prepared_checksum: 0,
                manifest_checksum: crc32c(&candidate.manifest),
                manifest_bytes: candidate.manifest.len() as u64,
                last_stage: StageId::default(),
            };
            let mut ops = vec![
                BatchOp::Put {
                    key: Bytes::from(operation_key(self.tree_id, b"candidate", candidate.id)),
                    value: Bytes::from(record.encode()),
                },
                BatchOp::Put {
                    key: Bytes::from(manifest_key),
                    value: Bytes::copy_from_slice(&candidate.manifest),
                },
            ];
            if let Some(owner) = self.writer_owner()? {
                ops.push(BatchOp::Put {
                    key: Bytes::from(operation_key(self.tree_id, b"candidate-owner", candidate.id)),
                    value: Bytes::from(owner.encode()),
                });
            }
            self.protection_write(authority, ops).await
        })
        .await
    }

    pub(super) async fn prepare_candidate(
        &self,
        id: StageId,
        window: u64,
        refs: &[StageId],
    ) -> Result<(), CtError> {
        self.authority_mutation(|| async {
            if refs.is_empty()
                || refs.len() > MAX_REFERENCE_WINDOW
                || refs.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err(CtError::InvalidArgument.into());
            }
            for reference in refs {
                validate_operation(*reference)?;
            }
            let authority = self.protection_authority(true).await?;
            let key = window_key(self.tree_id, id, window);
            let encoded = encode_references(refs);
            if let Some((current, _)) = self.get(&key).await.map_err(|_| CtError::Unavailable)? {
                return if current.as_ref() == encoded {
                    Ok(())
                } else {
                    Err(CtError::InvalidArgument.into())
                };
            }
            let mut record = self.candidate_record(id).await?.ok_or(CtError::NotFound)?;
            if record.status.state != CandidateState::Preparing
                || window != record.status.prepared_windows
                || refs[0] <= record.last_stage
                || authority.generation.checked_add(1) != Some(record.status.generation)
            {
                return Err(CtError::Unavailable.into());
            }
            let total = record
                .status
                .prepared_references
                .checked_add(refs.len() as u64)
                .ok_or(CtError::ResourceExhausted)?;
            if total > record.status.reference_count {
                return Err(CtError::InvalidArgument.into());
            }
            let checksum = reference_checksum(record.prepared_checksum, refs);
            if total == record.status.reference_count && checksum != record.expected_checksum {
                return Err(CtError::Corruption.into());
            }
            let mut ops = Vec::with_capacity(refs.len() + 3);
            for reference in refs {
                self.readable_stage(*reference).await?;
                let mut guard = self.load_stage_guard(*reference).await?;
                guard.references = guard
                    .references
                    .checked_add(1)
                    .ok_or(CtError::ResourceExhausted)?;
                ops.push(self.stage_guard_put(*reference, guard));
            }
            record.status.prepared_references = total;
            record.status.prepared_windows += 1;
            record.prepared_checksum = checksum;
            record.last_stage = *refs.last().ok_or(CtError::InvalidArgument)?;
            if total == record.status.reference_count {
                record.status.state = CandidateState::Ready;
            }
            ops.push(BatchOp::Put {
                key: Bytes::from(key),
                value: Bytes::from(encoded),
            });
            ops.push(BatchOp::Put {
                key: Bytes::from(operation_key(self.tree_id, b"candidate", id)),
                value: Bytes::from(record.encode()),
            });
            self.protection_write(authority, ops).await
        })
        .await
    }
}
