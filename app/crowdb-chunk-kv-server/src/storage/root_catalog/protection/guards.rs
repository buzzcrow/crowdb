// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::record::{decode_words, encode_words, operation_key};
use super::writer::WriterOwner;
use super::KvRootCatalogStore;
use bytes::Bytes;
use crowdb_paxos_kv_client::BatchOp;
use crowdb_tree_ffi::chunk::stage::{StageId, StageRecord, StageState};
use crowdb_tree_ffi::CtError;

#[derive(Clone, Copy, Default)]
pub(super) struct StageGuard {
    pub references: u64,
    pub pins: u64,
    pub creator: Option<WriterOwner>,
}

impl KvRootCatalogStore {
    pub(super) async fn load_stage_guard(&self, id: StageId) -> Result<StageGuard, CtError> {
        let key = operation_key(self.tree_id, b"stage-guard", id);
        let (value, _) = self
            .get(&key)
            .await
            .map_err(|_| CtError::Unavailable)?
            .ok_or(CtError::Unavailable)?;
        if value.len() == 28 {
            let words = decode_words::<3>(&value)?;
            if words[0] != 1 {
                return Err(CtError::Corruption);
            }
            return Ok(StageGuard {
                references: words[1],
                pins: words[2],
                creator: None,
            });
        }
        let words = decode_words::<12>(&value)?;
        if words[0] != 2 || words[3] > 1 {
            return Err(CtError::Corruption);
        }
        let creator = if words[3] == 0 {
            if words[4..] != [0; 8] {
                return Err(CtError::Corruption);
            }
            None
        } else {
            Some(WriterOwner::decode(&encode_words(&words[4..]))?)
        };
        Ok(StageGuard {
            references: words[1],
            pins: words[2],
            creator,
        })
    }
    pub(super) fn stage_guard_put(&self, id: StageId, guard: StageGuard) -> BatchOp {
        let mut words = [0; 12];
        words[..3].copy_from_slice(&[2, guard.references, guard.pins]);
        if let Some(owner) = guard.creator {
            words[3] = 1;
            words[4..].copy_from_slice(&decode_words::<8>(&owner.encode()).expect("valid writer encoding"));
        }
        BatchOp::Put {
            key: Bytes::from(operation_key(self.tree_id, b"stage-guard", id)),
            value: Bytes::from(encode_words(&words)),
        }
    }
    pub(super) async fn readable_stage(&self, id: StageId) -> Result<StageRecord, CtError> {
        let key = super::super::staging::stage_key(self.tree_id, id);
        let (value, _) = self
            .get(&key)
            .await
            .map_err(|_| CtError::Unavailable)?
            .ok_or(CtError::NotFound)?;
        let record = StageRecord::decode(&value)?;
        if record.id != id || record.tree_id != self.tree_id {
            return Err(CtError::Corruption);
        }
        if !matches!(record.state, StageState::Reloadable | StageState::Referenced) {
            return Err(CtError::Unavailable);
        }
        Ok(record)
    }

    pub(in super::super) async fn stage_guard_transition(
        &self,
        record: &StageRecord,
        creating: bool,
        next: Option<&StageRecord>,
    ) -> Result<Vec<BatchOp>, CtError> {
        let mut key = super::catalog_key(self.tree_id, b"chunk-stage", record.chunk_high);
        key.extend_from_slice(&record.chunk_low.to_be_bytes());
        let current = self.get(&key).await.map_err(|_| CtError::Unavailable)?;
        let identity = encode_words(&[
            record.id.owner_epoch,
            record.id.operation_high,
            record.id.operation_low,
        ]);
        if creating {
            // The identity tombstone remains after stage cleanup. A fixed ChunkId
            // cannot be allocated a second time under a different operation.
            if current.is_some() {
                return Err(CtError::Unavailable);
            }
            let creator = self.writer_owner()?;
            let mut ops = vec![
                BatchOp::Put {
                    key: Bytes::from(key),
                    value: Bytes::from(identity),
                },
                self.stage_guard_put(
                    record.id,
                    StageGuard {
                        creator,
                        pins: u64::from(creator.is_some()),
                        ..StageGuard::default()
                    },
                ),
            ];
            if let Some(owner) = creator {
                // Protect the output before Reloadable publication. The native
                // operation transfers this exact pin to its returned backing.
                let mut pin_key = operation_key(self.tree_id, b"stage-pin", record.id);
                for part in [
                    record.id.owner_epoch,
                    record.id.operation_high,
                    record.id.operation_low,
                ] {
                    pin_key.extend_from_slice(&part.to_be_bytes());
                }
                // A release that won before an unknown create reply leaves a
                // tombstone under this same authority revision. Never resurrect it.
                if self
                    .get(&pin_key)
                    .await
                    .map_err(|_| CtError::Unavailable)?
                    .is_some()
                {
                    return Err(CtError::Unavailable);
                }
                let pin = super::pin_owner::PinOwner {
                    live: true,
                    process: Some(owner.process),
                };
                ops.push(BatchOp::Put {
                    key: Bytes::from(pin_key),
                    value: Bytes::from(pin.encode()),
                });
            }
            return Ok(ops);
        }
        if current.as_ref().map(|(bytes, _)| bytes.as_ref()) != Some(identity.as_slice()) {
            return Err(CtError::Unavailable);
        }
        if next.map_or(true, |next| {
            matches!(
                next.state,
                StageState::Retired | StageState::Deleting | StageState::Reclaimed
            )
        }) {
            let guard = self.load_stage_guard(record.id).await?;
            if let Some(creator) = guard.creator {
                // Sealed packs have no outstanding disk writes. Unsealed packs
                // remain protected until their exact writer has stopped.
                if matches!(record.state, StageState::Reserved | StageState::Writing) {
                    self.require_completed_recovery().await?;
                    if !self.operation_is_excluded(record.id, creator).await? {
                        return Err(CtError::Unavailable);
                    }
                }
            } else if record.id.owner_epoch != self.owner_epoch
                && matches!(record.state, StageState::Reserved | StageState::Writing)
            {
                // Old unsealed records have no durable creator identity from
                // which a completed-write exclusion can be proved.
                return Err(CtError::Unavailable);
            }
            if guard.references != 0 || guard.pins != 0 {
                return Err(CtError::Unavailable);
            }
        }
        Ok(Vec::new())
    }

    pub(super) async fn change_stage_pin(
        &self,
        stage: StageId,
        pin: StageId,
        acquire: bool,
    ) -> Result<(), CtError> {
        self.authority_mutation(|| async {
            use super::pin_owner::PinOwner;
            let owner = PinOwner {
                live: acquire,
                process: self.process_identity()?,
            };
            if acquire && owner.process.is_some() {
                self.check_process_owner()?;
            }
            // Reader ownership is process-scoped. A retained old reader need not
            // be the current writer, but its pin still competes atomically with GC.
            let authority = self
                .protection_authority(acquire && owner.process.is_none())
                .await?;
            let mut key = operation_key(self.tree_id, b"stage-pin", stage);
            for value in [pin.owner_epoch, pin.operation_high, pin.operation_low] {
                key.extend_from_slice(&value.to_be_bytes());
            }
            let current = self.get(&key).await.map_err(|_| CtError::Unavailable)?;
            let current = current.map(|(bytes, _)| PinOwner::decode(&bytes)).transpose()?;
            if current == Some(owner) {
                return Ok(());
            }
            if current.is_some_and(|current| current.process != owner.process) {
                return Err(CtError::Unavailable.into());
            }
            if acquire {
                if current.is_some() {
                    return Err(CtError::Unavailable.into());
                }
                self.readable_stage(stage).await?;
            }
            let stage_key = super::super::staging::stage_key(self.tree_id, stage);
            let stage_exists = self
                .get(&stage_key)
                .await
                .map_err(|_| CtError::Unavailable)?
                .is_some();
            let mut ops = Vec::with_capacity(3);
            if stage_exists {
                let mut guard = self.load_stage_guard(stage).await?;
                if acquire {
                    guard.pins = guard.pins.checked_add(1).ok_or(CtError::ResourceExhausted)?;
                } else if current.is_some_and(|current| current.live) {
                    guard.pins = guard.pins.checked_sub(1).ok_or(CtError::Corruption)?;
                }
                if !acquire && pin == stage {
                    if let Some(creator) = guard.creator {
                        ops.push(self.operation_quiescence(stage, creator).await?);
                    }
                }
                ops.push(self.stage_guard_put(stage, guard));
            } else if acquire || current.is_some_and(|value| value.live) {
                return Err(CtError::Corruption.into());
            }
            // Missing stages are not evidence that an unknown create can no
            // longer commit. The inactive pin also fences any later exact retry.
            ops.push(BatchOp::Put {
                key: Bytes::from(key),
                value: Bytes::from(owner.encode()),
            });
            self.protection_write(authority, ops).await?;
            if acquire && owner.process.is_some() {
                self.check_process_owner()?;
            }
            Ok(())
        })
        .await
    }
}
