// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reference intents, live pins and GC use one root authority revision.

use bytes::Bytes;
use crowdb_paxos_kv_client::BatchOp;
use crowdb_tree_ffi::chunk::protection::{
    validate_operation, CandidateStatus, ManifestCandidate, StageProtectionCatalog,
};
use crowdb_tree_ffi::chunk::stage::StageId;
use crowdb_tree_ffi::CtError;

use super::{catalog_key, decode_authority, KvRootCatalogStore};
mod candidate;
mod commit;
mod guards;
mod operation;
mod pin_owner;
mod process;
mod prune;
mod record;
mod release;
mod scan;
mod writer;
pub(super) use process::RecoveryReceipt;
use record::{operation_key, CandidateRecord};

// Native metadata proposals reserve progress for at most 64 distinct keys.
pub(super) const MAX_METADATA_MUTATIONS: usize = 64;

pub(super) struct Authority {
    key: Vec<u8>,
    value: Bytes,
    revision: u64,
    pub(super) epoch: u64,
    generation: u64,
}

impl KvRootCatalogStore {
    pub(super) async fn protection_authority(&self, require_owner: bool) -> Result<Authority, CtError> {
        let key = catalog_key(self.tree_id, b"authority", 0);
        let (value, revision) = self
            .get(&key)
            .await
            .map_err(|_| CtError::Unavailable)?
            .ok_or(CtError::Unavailable)?;
        let (epoch, generation) = decode_authority(&value).ok_or(CtError::Corruption)?;
        if require_owner {
            self.check_registered_writer().await?;
        }
        if require_owner && epoch != self.owner_epoch {
            return Err(CtError::Unavailable);
        }
        Ok(Authority {
            key,
            value,
            revision,
            epoch,
            generation,
        })
    }

    pub(super) async fn protection_write(
        &self,
        authority: Authority,
        mut ops: Vec<BatchOp>,
    ) -> super::transaction::MutationResult<()> {
        ops.push(BatchOp::Put {
            key: Bytes::from(authority.key.clone()),
            value: authority.value,
        });
        self.kv
            .batch_write_cas(
                self.store_id,
                self.group_id,
                &ops,
                &authority.key,
                authority.revision,
            )
            .await
            .map(|_| ())
            .map_err(|error| super::transaction::MutationError::from_cas(&error))
    }

    fn protection_identity(&self, tree_id: u64, id: StageId, owner: bool) -> Result<(), CtError> {
        validate_operation(id)?;
        if tree_id != self.tree_id {
            return Err(CtError::InvalidArgument);
        }
        if owner && id.owner_epoch != self.owner_epoch {
            return Err(CtError::Unavailable);
        }
        Ok(())
    }

    async fn candidate_record(&self, id: StageId) -> Result<Option<CandidateRecord>, CtError> {
        let key = operation_key(self.tree_id, b"candidate", id);
        self.get(&key)
            .await
            .map_err(|_| CtError::Unavailable)?
            .map(|(bytes, _)| CandidateRecord::decode(id, &bytes))
            .transpose()
    }
}

impl StageProtectionCatalog for KvRootCatalogStore {
    fn recover_manifest_candidate(&self, tree: u64, id: StageId) -> Result<(), CtError> {
        self.protection_identity(tree, id, false)?;
        self.wait(self.recover_candidate(id))
    }
    fn writer_quiesced(&self, tree: u64, epoch: u64) -> Result<(), CtError> {
        if tree != self.tree_id || epoch != self.owner_epoch {
            return Err(CtError::InvalidArgument);
        }
        self.wait(self.publish_writer_quiescence())
    }
    fn check_read_owner(&self) -> Result<(), CtError> {
        self.check_process_owner()
    }
    fn prune_stage_pins(
        &self,
        tree: u64,
        stage: StageId,
        after: Option<StageId>,
        limit: u32,
    ) -> Result<crowdb_tree_ffi::chunk::protection::PinPruneProgress, CtError> {
        self.protection_identity(tree, stage, false)?;
        self.wait(self.prune_pins(stage, after, limit))
    }

    fn load_manifest_candidate(&self, tree: u64, id: StageId) -> Result<Option<CandidateStatus>, CtError> {
        self.protection_identity(tree, id, false)?;
        self.wait(self.candidate_record(id))
            .map(|record| record.map(|record| record.status))
    }
    fn list_manifest_candidates(
        &self,
        tree: u64,
        after: Option<StageId>,
        limit: u32,
    ) -> Result<Vec<CandidateStatus>, CtError> {
        if tree != self.tree_id {
            return Err(CtError::InvalidArgument);
        }
        self.wait(self.scan_candidates(after, limit))
    }
    fn begin_manifest_candidate(&self, tree: u64, candidate: &ManifestCandidate) -> Result<(), CtError> {
        candidate.validate()?;
        self.protection_identity(tree, candidate.id, true)?;
        self.wait(self.authorize_publication())?;
        self.wait(self.begin_candidate(candidate))
    }
    fn prepare_manifest_references(
        &self,
        tree: u64,
        id: StageId,
        window: u64,
        refs: &[StageId],
    ) -> Result<(), CtError> {
        self.protection_identity(tree, id, true)?;
        self.wait(self.prepare_candidate(id, window, refs))
    }
    fn commit_manifest_candidate(&self, tree: u64, id: StageId) -> Result<(), CtError> {
        self.protection_identity(tree, id, true)?;
        self.wait(self.commit_candidate(id))
    }
    fn abort_manifest_candidate(&self, tree: u64, id: StageId) -> Result<(), CtError> {
        self.protection_identity(tree, id, true)?;
        self.wait(self.abort_candidate(id))
    }
    fn release_manifest_references(&self, tree: u64, id: StageId, window: u64) -> Result<(), CtError> {
        self.protection_identity(tree, id, false)?;
        self.wait(self.release_candidate(id, window))
    }
    fn pin_stage(&self, tree: u64, stage: StageId, pin: StageId) -> Result<(), CtError> {
        self.protection_identity(tree, stage, false)?;
        self.protection_identity(tree, pin, true)?;
        self.wait(self.change_stage_pin(stage, pin, true))
    }
    fn unpin_stage(&self, tree: u64, stage: StageId, pin: StageId) -> Result<(), CtError> {
        self.protection_identity(tree, stage, false)?;
        self.protection_identity(tree, pin, true)?;
        self.wait(self.change_stage_pin(stage, pin, false))
    }
}
