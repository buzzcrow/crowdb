// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_protocol::chunk_kv::{SplitPhase, TransferPhase};

use super::{SplitStateMachine, TransferStateMachine, TransitionExecutor};
use crate::{Group0ControlStore, MonitorError};

/// Periodic safety-net processor for locally relevant persisted transitions.
pub struct TransitionProcessor {
    instance_id: u64,
    store: Arc<Group0ControlStore>,
    executor: Arc<TransitionExecutor>,
}

impl TransitionProcessor {
    #[must_use]
    pub fn new(instance_id: u64, store: Arc<Group0ControlStore>, executor: Arc<TransitionExecutor>) -> Self {
        Self {
            instance_id,
            store,
            executor,
        }
    }

    /// Processes one complete fixed-snapshot observation.
    ///
    /// Both transition prefixes are read before any action. A read failure
    /// therefore cannot be interpreted as an empty control plane. Every phase
    /// that authorizes external work is persisted before that work begins.
    ///
    /// # Errors
    ///
    /// Returns the first scan, storage, worker, or revision-fence failure.
    pub async fn tick(&self) -> Result<(), MonitorError> {
        let transfers = self.store.list_transfer_transitions().await?;
        let splits = self.store.list_split_transitions().await?;
        for (transition, revision) in transfers {
            self.process_transfer(transition, revision).await?;
        }
        for (transition, revision) in splits {
            self.process_split(transition, revision).await?;
        }
        Ok(())
    }

    async fn process_transfer(
        &self,
        transition: crowdb_protocol::chunk_kv::TransferTransition,
        mut revision: u64,
    ) -> Result<(), MonitorError> {
        let mut machine = TransferStateMachine::restore(transition)?;
        if machine.transition().source.instance_id == self.instance_id
            && machine.transition().phase == TransferPhase::Planned
        {
            machine.begin_source_prepare()?;
            revision = self
                .store
                .persist_transfer_transition(machine.transition(), revision)
                .await?;
        }
        if machine.transition().source.instance_id == self.instance_id
            && machine.transition().phase == TransferPhase::SourcePreparing
        {
            let artifact = self
                .executor
                .prepare_transfer_source(machine.transition())
                .await?;
            machine.record_source_base(artifact)?;
            revision = self
                .store
                .persist_transfer_transition(machine.transition(), revision)
                .await?;
        }
        if machine.transition().target.instance_id == self.instance_id
            && machine.transition().phase == TransferPhase::TargetPreparing
        {
            let proof = self
                .executor
                .prepare_transfer_target(machine.transition())
                .await?;
            machine.record_target_ready(proof)?;
            revision = self
                .store
                .persist_transfer_transition(machine.transition(), revision)
                .await?;
        }
        if machine.transition().source.instance_id == self.instance_id
            && machine.transition().phase == TransferPhase::AwaitingFence
        {
            let proof = self.executor.fence_transfer_source(machine.transition()).await?;
            machine.record_source_fence(proof)?;
            revision = self
                .store
                .persist_transfer_transition(machine.transition(), revision)
                .await?;
        }
        if machine.transition().target.instance_id == self.instance_id
            && machine.transition().phase == TransferPhase::CatchupPublished
        {
            let proof = self
                .executor
                .prepare_transfer_target(machine.transition())
                .await?;
            machine.record_target_caught_up(proof)?;
            self.store
                .persist_transfer_transition(machine.transition(), revision)
                .await?;
        }
        if (machine.transition().target.instance_id == self.instance_id
            && machine.transition().phase == TransferPhase::CatalogCommitted)
            || (machine.transition().source.instance_id == self.instance_id
                && machine.transition().phase == TransferPhase::Aborted)
        {
            self.executor
                .release_transfer_generation_pin(machine.transition())?;
        }
        Ok(())
    }

    async fn process_split(
        &self,
        transition: crowdb_protocol::chunk_kv::SplitTransition,
        mut revision: u64,
    ) -> Result<(), MonitorError> {
        let mut machine = SplitStateMachine::restore(transition)?;
        if machine.transition().parent_owner.instance_id != self.instance_id {
            return Ok(());
        }
        if machine.transition().phase == SplitPhase::Planned {
            machine.begin_parent_prepare()?;
            revision = self
                .store
                .persist_split_transition(machine.transition(), revision)
                .await?;
        }
        if machine.transition().phase == SplitPhase::ParentPreparing {
            let proof = self.executor.prepare_split_parent(machine.transition()).await?;
            machine.record_child_ready(proof)?;
            self.store
                .persist_split_transition(machine.transition(), revision)
                .await?;
        }
        if matches!(
            machine.transition().phase,
            SplitPhase::CatalogCommitted | SplitPhase::Aborted
        ) {
            self.executor.release_split_generation_pin(machine.transition())?;
        }
        Ok(())
    }
}
