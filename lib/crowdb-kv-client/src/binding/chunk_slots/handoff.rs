// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group-0 durable handoff progress; publication must include the complete maps.

use super::{decode, invalid, put, ChunkSlotMapClient};
use crate::{Error, GetOutcome, ReadMode, Result};
use crowdb_protocol::chunk_slot::{ChunkServiceHandoff, ChunkServiceHandoffPhase};
use crowdb_protocol::key::{ChunkServiceHandoffKey, ChunkSlotFenceKey, ChunkSlotMapHeadKey, TextKey};

/// A validated plan and the revision used for the next compare-and-write.
#[derive(Clone, Debug)]
pub struct ChunkServiceHandoffSnapshot {
    plan: ChunkServiceHandoff,
    revision: u64,
}

impl ChunkServiceHandoffSnapshot {
    #[must_use]
    pub fn plan(&self) -> &ChunkServiceHandoff {
        &self.plan
    }

    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }
}

impl ChunkSlotMapClient {
    /// Fence one slot only after the cohort's Fence phase has been persisted.
    ///
    /// # Errors
    /// Rejects unexpected authority, missing source fences and unresolved CAS outcomes.
    pub async fn fence_handoff_slot(
        &self,
        snapshot: &ChunkServiceHandoffSnapshot,
        slot: crowdb_protocol::chunk_slot::ChunkSlot,
    ) -> Result<crowdb_protocol::chunk_slot::ChunkSlotFenceReceipt> {
        if snapshot.plan.record().phase != ChunkServiceHandoffPhase::Fence {
            return Err(invalid("handoff", "data fencing requires persisted Fence phase"));
        }
        let transfer = snapshot
            .plan
            .record()
            .transfers
            .iter()
            .find(|transfer| transfer.slot == slot)
            .ok_or_else(|| invalid("handoff", "slot is outside the handoff cohort"))?;
        let path = ChunkSlotFenceKey { slot }.to_path();
        let storage = transfer.storage;
        let target = transfer.target.to_fence_value();
        let current = self
            .kv
            .get(
                storage.store_id,
                storage.group_id,
                path.as_bytes(),
                ReadMode::Linearizable,
                None,
            )
            .await?;
        let revision = match current {
            GetOutcome::Found { value, revision } if value.as_ref() == target => {
                return Ok(crowdb_protocol::chunk_slot::ChunkSlotFenceReceipt { slot, revision });
            }
            GetOutcome::Found { value, revision }
                if transfer
                    .previous
                    .is_some_and(|previous| value.as_ref() == previous.to_fence_value()) =>
            {
                revision
            }
            GetOutcome::Found { revision, .. } => {
                return Err(Error::CasFailed {
                    current_revision: revision,
                })
            }
            GetOutcome::NotFound if transfer.previous.is_none() => 0,
            GetOutcome::NotFound => return Err(Error::CasFailed { current_revision: 0 }),
        };
        match self
            .kv
            .put_cas(
                storage.store_id,
                storage.group_id,
                path.as_bytes(),
                &target,
                revision,
            )
            .await
        {
            Ok(write) => Ok(crowdb_protocol::chunk_slot::ChunkSlotFenceReceipt {
                slot,
                revision: write.revision,
            }),
            Err(error @ (Error::OutcomeUnknown | Error::CasBusy | Error::CasFailed { .. })) => {
                match self
                    .kv
                    .get(
                        storage.store_id,
                        storage.group_id,
                        path.as_bytes(),
                        ReadMode::Linearizable,
                        None,
                    )
                    .await?
                {
                    GetOutcome::Found { value, revision } if value.as_ref() == target => {
                        Ok(crowdb_protocol::chunk_slot::ChunkSlotFenceReceipt { slot, revision })
                    }
                    _ => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Read a persisted cohort, rejecting malformed identity or phase receipts.
    ///
    /// # Errors
    /// Returns KV or schema errors without exposing a partial plan.
    pub async fn read_handoff(&self) -> Result<Option<ChunkServiceHandoffSnapshot>> {
        let path = ChunkServiceHandoffKey.to_path();
        match self
            .kv
            .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
            .await?
        {
            GetOutcome::NotFound => Ok(None),
            GetOutcome::Found { value, revision } => Ok(Some(ChunkServiceHandoffSnapshot {
                plan: decode(path.as_bytes(), &value)?,
                revision,
            })),
        }
    }

    /// Reserve one cohort by CAS on the current complete service-map head.
    ///
    /// # Errors
    /// Rejects competing unfinished cohorts, stale maps or mismatched destinations.
    pub async fn prepare_handoff(&self, plan: &ChunkServiceHandoff) -> Result<ChunkServiceHandoffSnapshot> {
        let path = ChunkServiceHandoffKey.to_path();
        if plan.record().phase != ChunkServiceHandoffPhase::Prepare {
            return Err(invalid(&path, "new handoff must start at Prepare"));
        }
        if let Some(current) = self.read_handoff().await? {
            if current.plan.can_follow(plan) {
                return Ok(current);
            }
            if current.plan.record().phase != ChunkServiceHandoffPhase::Activate {
                return Err(invalid(&path, "an unfinished handoff must be resumed"));
            }
        }
        let service = self.read_service().await?;
        let storage = self.read_storage().await?;
        let Some((head, revision)) = self.read_head::<u64>().await? else {
            return Err(invalid(&path, "service map disappeared"));
        };
        if head != *service.head()
            || head.generation != plan.record().base_service_generation
            || storage.head().generation != plan.record().storage_generation
            || plan.record().transfers.iter().any(|transfer| {
                service.owner(transfer.slot) != transfer.previous.unwrap_or(transfer.target).instance_id()
                    || storage.owner(transfer.slot) != transfer.storage
            })
        {
            return Err(invalid(&path, "handoff does not match the complete current maps"));
        }
        let head_path = ChunkSlotMapHeadKey::Service.to_path();
        // Rewriting the unchanged head advances its revision: concurrent cohort
        // creators cannot reserve the same source generation independently.
        let ops = [put(head_path.clone(), &head)?, put(path, plan)?];
        match self
            .kv
            .batch_write_cas(0, 0, &ops, head_path.as_bytes(), revision)
            .await
        {
            Ok(_) | Err(Error::OutcomeUnknown | Error::CasBusy | Error::CasFailed { .. }) => {
                self.reconcile_handoff(plan).await
            }
            Err(error) => Err(error),
        }
    }

    /// Save monotonic receipts/phase progress using the observed plan revision.
    ///
    /// # Errors
    /// Rejects rollback, changed targets or publication without an atomic map batch.
    pub async fn advance_handoff(
        &self,
        previous: &ChunkServiceHandoffSnapshot,
        next: &ChunkServiceHandoff,
    ) -> Result<ChunkServiceHandoffSnapshot> {
        let path = ChunkServiceHandoffKey.to_path();
        let phases = (previous.plan.record().phase, next.record().phase);
        if !next.can_follow(&previous.plan)
            || !matches!(
                phases,
                (
                    ChunkServiceHandoffPhase::Prepare,
                    ChunkServiceHandoffPhase::Prepare | ChunkServiceHandoffPhase::Fence
                ) | (ChunkServiceHandoffPhase::Fence, ChunkServiceHandoffPhase::Fence)
                    | (
                        ChunkServiceHandoffPhase::Publish,
                        ChunkServiceHandoffPhase::Publish | ChunkServiceHandoffPhase::Activate
                    )
                    | (
                        ChunkServiceHandoffPhase::Activate,
                        ChunkServiceHandoffPhase::Activate
                    )
            )
        {
            return Err(invalid(
                &path,
                "handoff progress requires monotonic identity and complete map publication",
            ));
        }
        self.verify_new_fences(previous, next).await?;
        let ops = [put(path.clone(), next)?];
        match self
            .kv
            .batch_write_cas(0, 0, &ops, path.as_bytes(), previous.revision)
            .await
        {
            Ok(_) | Err(Error::OutcomeUnknown | Error::CasBusy | Error::CasFailed { .. }) => {
                self.reconcile_handoff(next).await
            }
            Err(error) => Err(error),
        }
    }

    async fn verify_new_fences(
        &self,
        previous: &ChunkServiceHandoffSnapshot,
        next: &ChunkServiceHandoff,
    ) -> Result<()> {
        for receipt in &next.record().fences {
            if previous.plan.record().fences.contains(receipt) {
                continue;
            }
            let transfer = next
                .record()
                .transfers
                .iter()
                .find(|transfer| transfer.slot == receipt.slot)
                .ok_or_else(|| invalid("handoff", "fence receipt has no transfer"))?;
            let path = ChunkSlotFenceKey { slot: receipt.slot }.to_path();
            let actual = self
                .kv
                .get(
                    transfer.storage.store_id,
                    transfer.storage.group_id,
                    path.as_bytes(),
                    ReadMode::Linearizable,
                    None,
                )
                .await?;
            match actual {
                GetOutcome::Found { value, revision }
                    if revision == receipt.revision && value.as_ref() == transfer.target.to_fence_value() => {
                }
                GetOutcome::Found { revision, .. } => {
                    return Err(Error::CasFailed {
                        current_revision: revision,
                    })
                }
                GetOutcome::NotFound => return Err(Error::CasFailed { current_revision: 0 }),
            }
        }
        Ok(())
    }

    async fn reconcile_handoff(&self, expected: &ChunkServiceHandoff) -> Result<ChunkServiceHandoffSnapshot> {
        let current = self.read_handoff().await?.ok_or(Error::OutcomeUnknown)?;
        if current.plan.can_follow(expected) {
            Ok(current)
        } else {
            Err(Error::CasFailed {
                current_revision: current.revision,
            })
        }
    }
}
