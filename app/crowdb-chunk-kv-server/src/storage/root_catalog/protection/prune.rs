// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::pin_owner::PinOwner;
use super::record::operation_key;
use super::KvRootCatalogStore;
use bytes::Bytes;
use crowdb_paxos_kv_client::{BatchOp, ReadMode};
use crowdb_tree_ffi::chunk::protection::PinPruneProgress;
use crowdb_tree_ffi::chunk::stage::StageId;
use crowdb_tree_ffi::CtError;

impl KvRootCatalogStore {
    pub(super) async fn prune_pins(
        &self,
        stage: StageId,
        after: Option<StageId>,
        limit: u32,
    ) -> Result<PinPruneProgress, CtError> {
        self.authority_mutation(|| async {
            if !(1..=64).contains(&limit) {
                return Err(CtError::InvalidArgument.into());
            }
            // Every pruning transaction also updates the stage guard and authority.
            let limit = limit.min(
                u32::try_from(super::MAX_METADATA_MUTATIONS - 2).map_err(|_| CtError::ResourceExhausted)?,
            );
            self.require_completed_recovery().await?;
            let authority = self.protection_authority(true).await?;
            let prefix = operation_key(self.tree_id, b"stage-pin", stage);
            let start = after
                .map(|id| {
                    let mut key = prefix.clone();
                    for value in [id.owner_epoch, id.operation_high, id.operation_low] {
                        key.extend_from_slice(&value.to_be_bytes());
                    }
                    key
                })
                .unwrap_or_default();
            let result = self
                .kv
                .scan(
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
                )
                .await
                .map_err(|_| CtError::Unavailable)?;
            if result.timed_out {
                return Err(CtError::Unavailable.into());
            }
            let mut progress = PinPruneProgress {
                more: result.truncated,
                ..PinPruneProgress::default()
            };
            let mut ops = Vec::with_capacity(result.items.len() + 2);
            for (key, value) in result.items {
                if key.len() != prefix.len() + 24 {
                    return Err(CtError::Corruption.into());
                }
                let bytes = &key[prefix.len()..];
                let at = |offset| {
                    u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("pin key bounds"))
                };
                let id = StageId {
                    owner_epoch: at(0),
                    operation_high: at(8),
                    operation_low: at(16),
                };
                if after.is_some_and(|after| id <= after) {
                    return Err(CtError::Corruption.into());
                }
                let mut owner = PinOwner::decode(&value)?;
                progress.examined += 1;
                progress.after = Some(id);
                let Some(process) = owner.process else {
                    continue;
                };
                if !owner.live || !self.process_is_excluded(process).await? {
                    continue;
                }
                owner.live = false;
                progress.released += 1;
                ops.push(BatchOp::Put {
                    key,
                    value: Bytes::from(owner.encode()),
                });
            }
            if progress.released != 0 {
                let mut guard = self.load_stage_guard(stage).await?;
                guard.pins = guard
                    .pins
                    .checked_sub(progress.released.into())
                    .ok_or(CtError::Corruption)?;
                ops.push(self.stage_guard_put(stage, guard));
                self.protection_write(authority, ops).await?;
            }
            self.check_process_owner()?;
            Ok(progress)
        })
        .await
    }
}
