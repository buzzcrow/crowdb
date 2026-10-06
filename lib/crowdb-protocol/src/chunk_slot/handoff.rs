// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable, forward-only service handoff cohorts. Storage placement is unchanged.

use super::{ChunkSlot, ChunkSlotAuthority, ChunkStorageGroup, CHUNK_SLOT_COUNT};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkSlotTransfer {
    pub slot: ChunkSlot,
    /// Absent only when bootstrapping a fenced layout with no admitted writers.
    pub previous: Option<ChunkSlotAuthority>,
    pub target: ChunkSlotAuthority,
    pub storage: ChunkStorageGroup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkServiceHandoffPhase {
    Prepare,
    Fence,
    Publish,
    Activate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkSlotFenceReceipt {
    pub slot: ChunkSlot,
    pub revision: u64,
}

/// Serialized cohort; decoding into the validated plan checks phase receipts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkServiceHandoffRecord {
    pub base_service_generation: u64,
    pub storage_generation: u64,
    pub transfers: Vec<ChunkSlotTransfer>,
    pub phase: ChunkServiceHandoffPhase,
    pub fences: Vec<ChunkSlotFenceReceipt>,
}

/// One cohort publishes one complete generation after every destination is fenced.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ChunkServiceHandoffRecord", into = "ChunkServiceHandoffRecord")]
pub struct ChunkServiceHandoff(ChunkServiceHandoffRecord);

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid service handoff identity, phase, generation or fence receipts")]
pub struct ChunkServiceHandoffError;

impl ChunkServiceHandoff {
    /// Prepare a cohort before any data-group ownership side effects.
    ///
    /// # Errors
    /// Rejects duplicate slots, invalid destinations or nonconsecutive authority epochs.
    pub fn prepare(
        base_service_generation: u64,
        storage_generation: u64,
        transfers: Vec<ChunkSlotTransfer>,
    ) -> Result<Self, ChunkServiceHandoffError> {
        Self::try_from(ChunkServiceHandoffRecord {
            base_service_generation,
            storage_generation,
            transfers,
            phase: ChunkServiceHandoffPhase::Prepare,
            fences: Vec::new(),
        })
    }

    #[must_use]
    pub fn record(&self) -> &ChunkServiceHandoffRecord {
        &self.0
    }

    #[must_use]
    pub fn publication_generation(&self) -> u64 {
        // Validation excludes overflow and the record is immutable to callers.
        self.0.base_service_generation + 1
    }

    /// A replay may observe a later phase, but cannot change the cohort or receipts.
    #[must_use]
    pub fn can_follow(&self, previous: &Self) -> bool {
        let rank = |phase| match phase {
            ChunkServiceHandoffPhase::Prepare => 0,
            ChunkServiceHandoffPhase::Fence => 1,
            ChunkServiceHandoffPhase::Publish => 2,
            ChunkServiceHandoffPhase::Activate => 3,
        };
        self.0.base_service_generation == previous.0.base_service_generation
            && self.0.storage_generation == previous.0.storage_generation
            && self.0.transfers == previous.0.transfers
            && rank(self.0.phase) >= rank(previous.0.phase)
            && previous
                .0
                .fences
                .iter()
                .all(|receipt| self.0.fences.contains(receipt))
    }

    /// Enter fencing only after target preparation; persist before changing KV fences.
    ///
    /// # Errors
    /// Rejects attempts to return from publication or activation to fencing.
    pub fn begin_fencing(&mut self) -> Result<(), ChunkServiceHandoffError> {
        match self.0.phase {
            ChunkServiceHandoffPhase::Prepare | ChunkServiceHandoffPhase::Fence => {
                self.0.phase = ChunkServiceHandoffPhase::Fence;
                Ok(())
            }
            _ => Err(ChunkServiceHandoffError),
        }
    }

    /// Record a confirmed fence CAS or its reconciled applied value/revision.
    ///
    /// # Errors
    /// Rejects unknown slots, zero revisions and contradictory replayed receipts.
    pub fn record_fence(&mut self, receipt: ChunkSlotFenceReceipt) -> Result<(), ChunkServiceHandoffError> {
        if self.0.phase != ChunkServiceHandoffPhase::Fence
            || receipt.revision == 0
            || !self
                .0
                .transfers
                .iter()
                .any(|transfer| transfer.slot == receipt.slot)
        {
            return Err(ChunkServiceHandoffError);
        }
        if let Some(prior) = self.0.fences.iter().find(|prior| prior.slot == receipt.slot) {
            return if *prior == receipt {
                Ok(())
            } else {
                Err(ChunkServiceHandoffError)
            };
        }
        self.0.fences.push(receipt);
        self.0.fences.sort_unstable_by_key(|fence| fence.slot);
        Ok(())
    }

    /// Include this phase change in the complete routing/authority publication CAS.
    ///
    /// # Errors
    /// Rejects publication with incomplete fences or a different map generation.
    pub fn record_publication(&mut self, generation: u64) -> Result<(), ChunkServiceHandoffError> {
        if generation != self.publication_generation()
            || self.0.fences.len() != self.0.transfers.len()
            || self.0.phase == ChunkServiceHandoffPhase::Prepare
        {
            return Err(ChunkServiceHandoffError);
        }
        if self.0.phase != ChunkServiceHandoffPhase::Activate {
            self.0.phase = ChunkServiceHandoffPhase::Publish;
        }
        Ok(())
    }

    /// Record confirmed recovery of every target incarnation after publication.
    ///
    /// # Errors
    /// Rejects activation before complete fenced routing publication.
    pub fn record_activation(&mut self) -> Result<(), ChunkServiceHandoffError> {
        match self.0.phase {
            ChunkServiceHandoffPhase::Publish | ChunkServiceHandoffPhase::Activate => {
                self.0.phase = ChunkServiceHandoffPhase::Activate;
                Ok(())
            }
            _ => Err(ChunkServiceHandoffError),
        }
    }
}

impl TryFrom<ChunkServiceHandoffRecord> for ChunkServiceHandoff {
    type Error = ChunkServiceHandoffError;

    fn try_from(mut record: ChunkServiceHandoffRecord) -> Result<Self, Self::Error> {
        if record.base_service_generation == 0
            || record.base_service_generation == u64::MAX
            || record.storage_generation == 0
            || record.transfers.is_empty()
            || record.transfers.len() > usize::from(CHUNK_SLOT_COUNT)
        {
            return Err(ChunkServiceHandoffError);
        }
        let mut slots = BTreeSet::new();
        for transfer in &record.transfers {
            let next = transfer
                .previous
                .map_or(Some(1), |previous| previous.generation().checked_add(1));
            if !slots.insert(transfer.slot)
                || transfer.storage.group_id == 0
                || next != Some(transfer.target.generation())
            {
                return Err(ChunkServiceHandoffError);
            }
        }
        let mut fenced = BTreeSet::new();
        for receipt in &record.fences {
            if receipt.revision == 0 || !slots.contains(&receipt.slot) || !fenced.insert(receipt.slot) {
                return Err(ChunkServiceHandoffError);
            }
        }
        record.transfers.sort_unstable_by_key(|transfer| transfer.slot);
        record.fences.sort_unstable_by_key(|receipt| receipt.slot);
        match record.phase {
            ChunkServiceHandoffPhase::Prepare if !fenced.is_empty() => Err(ChunkServiceHandoffError),
            ChunkServiceHandoffPhase::Publish | ChunkServiceHandoffPhase::Activate
                if fenced.len() != slots.len() =>
            {
                Err(ChunkServiceHandoffError)
            }
            _ => Ok(Self(record)),
        }
    }
}

impl From<ChunkServiceHandoff> for ChunkServiceHandoffRecord {
    fn from(plan: ChunkServiceHandoff) -> Self {
        plan.0
    }
}
