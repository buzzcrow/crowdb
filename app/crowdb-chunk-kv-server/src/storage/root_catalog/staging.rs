// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Conditional staging mutations share the root authority revision.

use bytes::Bytes;
use crowdb_paxos_kv_client::{BatchOp, ReadMode};
use crowdb_tree_ffi::chunk::stage::{validate_stage_transition, StageId, StageRecord, MAX_STAGE_SCAN};
use crowdb_tree_ffi::CtError;

use super::{catalog_key, decode_authority, KvRootCatalogStore};

impl KvRootCatalogStore {
    pub(super) fn stage_load(&self, tree_id: u64, id: StageId) -> Result<Option<StageRecord>, CtError> {
        if tree_id != self.tree_id || id.owner_epoch == 0 {
            return Err(CtError::InvalidArgument);
        }
        let value = self
            .wait(self.get(&stage_key(tree_id, id)))
            .map_err(|_| CtError::Unavailable)?;
        value
            .map(|(bytes, _)| {
                let record = StageRecord::decode(&bytes)?;
                if record.tree_id != tree_id || record.id != id {
                    return Err(CtError::Corruption);
                }
                Ok(record)
            })
            .transpose()
    }

    pub(super) fn stage_compare(
        &self,
        tree_id: u64,
        owner_epoch: u64,
        expected: Option<&StageRecord>,
        next: Option<&StageRecord>,
    ) -> Result<(), CtError> {
        validate_stage_transition(expected, next)?;
        let record = next.or(expected).ok_or(CtError::InvalidArgument)?;
        if tree_id != self.tree_id || record.tree_id != tree_id {
            return Err(CtError::InvalidArgument);
        }
        if owner_epoch != self.owner_epoch || record.id.owner_epoch > owner_epoch {
            return Err(CtError::Unavailable);
        }
        if record.id.owner_epoch != owner_epoch {
            use crowdb_tree_ffi::chunk::stage::StageState;
            if expected.is_none()
                || next.is_some_and(|value| {
                    !matches!(
                        value.state,
                        StageState::Retired | StageState::Deleting | StageState::Reclaimed
                    )
                })
            {
                return Err(CtError::Unavailable);
            }
            self.wait(self.require_completed_recovery())?;
        }
        if record.id.owner_epoch == owner_epoch {
            self.wait(self.authorize_staging())?;
        } else {
            self.wait(self.authorize_publication())?;
        }
        self.wait(self.stage_compare_authorized(owner_epoch, record.id, expected, next))
    }

    async fn stage_compare_authorized(
        &self,
        owner_epoch: u64,
        id: StageId,
        expected: Option<&StageRecord>,
        next: Option<&StageRecord>,
    ) -> Result<(), CtError> {
        self.authority_mutation(|| async {
            let authority_key = catalog_key(self.tree_id, b"authority", 0);
            let (authority, revision) = self
                .get(&authority_key)
                .await
                .map_err(|_| CtError::Unavailable)?
                .ok_or(CtError::Unavailable)?;
            let (epoch, _) = decode_authority(&authority).ok_or(CtError::Corruption)?;
            self.check_registered_writer().await?;
            if epoch != owner_epoch {
                if epoch > owner_epoch || id.owner_epoch != owner_epoch {
                    return Err(CtError::Unavailable.into());
                }
                self.check_prepared_staging(epoch).await?;
            }
            let key = stage_key(self.tree_id, id);
            let current = self.get(&key).await.map_err(|_| CtError::Unavailable)?;
            let expected_record = expected;
            let next_record = next;
            let expected = expected.map(StageRecord::encode).transpose()?;
            let next = next.map(StageRecord::encode).transpose()?;
            let current = current.as_ref().map(|(bytes, _)| bytes.as_ref());
            if current == next.as_deref() {
                return Ok(());
            }
            if current != expected.as_deref() {
                return Err(CtError::Unavailable.into());
            }
            let update = match next {
                Some(bytes) => BatchOp::Put {
                    key: Bytes::from(key),
                    value: Bytes::from(bytes),
                },
                None => BatchOp::Delete {
                    key: Bytes::from(key),
                },
            };
            // Writing the unchanged authority value advances its KV revision. Root
            // publication, stage transitions, pinning, and reclamation serialize here.
            let mut ops = vec![
                BatchOp::Put {
                    key: Bytes::from(authority_key.clone()),
                    value: authority,
                },
                update,
            ];
            let record = expected_record.or(next_record).ok_or(CtError::InvalidArgument)?;
            ops.extend(
                self.stage_guard_transition(record, expected_record.is_none(), next_record)
                    .await?,
            );
            self.kv
                .batch_write_cas(self.store_id, self.group_id, &ops, &authority_key, revision)
                .await
                .map(|_| ())
                .map_err(|error| super::transaction::MutationError::from_cas(&error))
        })
        .await
    }

    pub(super) fn stage_list(
        &self,
        tree_id: u64,
        after: Option<StageId>,
        limit: u32,
    ) -> Result<Vec<StageRecord>, CtError> {
        if tree_id != self.tree_id || !(1..=MAX_STAGE_SCAN).contains(&limit) {
            return Err(CtError::InvalidArgument);
        }
        let prefix = stage_prefix(tree_id);
        let start = after.map(|id| stage_key(tree_id, id)).unwrap_or_default();
        let result = self
            .wait(self.kv.scan(
                self.store_id,
                self.group_id,
                &prefix,
                &start,
                &[],
                limit,
                ReadMode::Linearizable,
                None,
                false,
                None,
            ))
            .map_err(|_| CtError::Unavailable)?;
        if result.timed_out {
            return Err(CtError::Unavailable);
        }
        result
            .items
            .into_iter()
            .map(|(key, value)| {
                let record = StageRecord::decode(&value)?;
                if record.tree_id != tree_id
                    || key.as_ref() != stage_key(tree_id, record.id).as_slice()
                    || after.is_some_and(|id| record.id <= id)
                {
                    return Err(CtError::Corruption);
                }
                Ok(record)
            })
            .collect()
    }
}

fn stage_prefix(tree_id: u64) -> Vec<u8> {
    let mut key = b"\0crowdb/chunk-kv/root/v1/".to_vec();
    key.extend_from_slice(&tree_id.to_be_bytes());
    key.extend_from_slice(b"/stage/v1/");
    key
}

pub(super) fn stage_key(tree_id: u64, id: StageId) -> Vec<u8> {
    let mut key = stage_prefix(tree_id);
    for value in [id.owner_epoch, id.operation_high, id.operation_low] {
        key.extend_from_slice(&value.to_be_bytes());
    }
    key
}
