// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Target-monitor ownership of accepted node storage and service setup.

use super::Result;
use crowdb_protocol::mgmt::node::setup::{NodeSetupPhase, NodeSetupRequest, NodeSetupStatus};
use std::{
    path::Path,
    time::{Duration, Instant},
};

mod execution;
mod journal;

#[derive(Default)]
pub(super) struct NodeSetup {
    task: Option<tokio::task::JoinHandle<Result<()>>>,
    next: Option<Instant>,
}

impl NodeSetup {
    pub(super) fn running(&self) -> bool {
        self.task.as_ref().is_some_and(|task| !task.is_finished())
    }

    pub(super) fn accept(&mut self, root: &Path, request: NodeSetupRequest) -> Result<NodeSetupStatus> {
        let status = journal::accept(root, request, self.running())?;
        self.next = None;
        Ok(status)
    }

    pub(super) fn status(root: &Path) -> Result<Option<NodeSetupStatus>> {
        Ok(journal::read(root)?.map(|state| state.status))
    }

    pub(super) fn intent(root: &Path) -> Result<Option<NodeSetupRequest>> {
        Ok(journal::read(root)?.map(|state| state.request))
    }

    pub(super) fn prepare_cleanup(
        &self,
        root: &Path,
        bootstrap: &crowdb_protocol::mgmt::SystemBootstrapIdentity,
        confirmed: bool,
    ) -> Result<()> {
        let Some(state) = journal::read(root)? else {
            return Ok(());
        };
        let accepted: super::AcceptedNode = super::durable::read(&root.join("accepted-node.json"))?
            .ok_or("setup cleanup requires accepted bootstrap")?;
        if !confirmed || accepted.bootstrap != *bootstrap {
            return Err("setup cleanup differs from accepted bootstrap".into());
        }
        super::durable::write(
            &root.join(format!("node-setup-retired-{}.json", state.request.operation_id)),
            bootstrap,
        )?;
        if self.running() {
            return Err("node setup is draining for explicit cleanup; retry cleanup after it settles".into());
        }
        std::fs::rename(
            root.join("node-setup.json"),
            root.join(format!("retired-node-setup-{}.json", state.request.operation_id)),
        )?;
        std::fs::File::open(root)?.sync_all()?;
        Ok(())
    }

    pub(super) async fn tick(&mut self, profile: &crate::DeploymentProfile) -> Result<()> {
        if self
            .task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            match self.task.take().expect("finished setup task").await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => eprintln!("node setup deferred: {error}"),
                Err(error) => eprintln!("node setup worker failed: {error}"),
            }
            self.next = Some(Instant::now() + Duration::from_secs(1));
        }
        if self.task.is_some() || self.next.is_some_and(|next| Instant::now() < next) {
            return Ok(());
        }
        let Some(state) = journal::read(&profile.paths.data_root)? else {
            return Ok(());
        };
        if profile
            .paths
            .data_root
            .join(format!("node-setup-retired-{}.json", state.request.operation_id))
            .exists()
        {
            return Ok(());
        }
        if matches!(state.status.phase, NodeSetupPhase::Ready | NodeSetupPhase::Failed) {
            return Ok(());
        }
        let profile = profile.clone();
        self.task = Some(tokio::spawn(execution::run(profile, state)));
        Ok(())
    }

    pub(super) async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            if let Err(error) = task.await {
                eprintln!("node setup shutdown failed: {error}");
            }
        }
    }
}

/// Exercise the real durable acceptance and one background pass without services.
#[cfg(feature = "test-util")]
#[derive(Default)]
pub struct NodeSetupForTests(NodeSetup);

#[cfg(feature = "test-util")]
impl NodeSetupForTests {
    /// # Errors
    /// Propagates target identity, authorization and immutable intent failures.
    pub fn accept_for_tests(&mut self, root: &Path, request: NodeSetupRequest) -> Result<NodeSetupStatus> {
        self.0.accept(root, request)
    }

    /// # Errors
    /// Propagates journal validation errors.
    pub fn status_for_tests(root: &Path) -> Result<Option<NodeSetupStatus>> {
        NodeSetup::status(root)
    }

    /// # Errors
    /// Propagates the same background pass and journal errors as production.
    pub async fn advance_for_tests(profile: crate::DeploymentProfile) -> Result<()> {
        let state = journal::read(&profile.paths.data_root)?.ok_or("setup not accepted")?;
        execution::run(profile, state).await
    }

    /// # Errors
    /// Propagates the real explicit-cleanup authorization and retirement errors.
    pub fn prepare_cleanup_for_tests(
        &self,
        root: &Path,
        bootstrap: &crowdb_protocol::mgmt::SystemBootstrapIdentity,
        confirmed: bool,
    ) -> Result<()> {
        self.0.prepare_cleanup(root, bootstrap, confirmed)
    }
}
