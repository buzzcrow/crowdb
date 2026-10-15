// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    execute, failures, pending, DeploymentProfile, NodeServiceIntent, ProcessManager, Result, Value,
};

/// Exercise durable service execution with the real process manager.
pub struct NodeServiceForTests {
    profile: DeploymentProfile,
    processes: ProcessManager,
}

impl NodeServiceForTests {
    /// # Errors
    /// Propagates production namespace validation and bounded read errors.
    pub fn processes_for_tests(
        root: &std::path::Path,
        manager: &mut ProcessManager,
        cluster: &str,
        node: u64,
    ) -> Result<Value> {
        super::super::observation::processes(root, manager, cluster, node)
    }

    /// # Errors
    /// Propagates process log setup errors.
    pub async fn new_for_tests(profile: DeploymentProfile) -> Result<Self> {
        let processes = ProcessManager::new(profile.paths.log_root.clone(), profile.logs.clone()).await?;
        Ok(Self { profile, processes })
    }

    /// # Errors
    /// Propagates production authority, process and readiness errors.
    pub async fn execute_for_tests(&mut self, intent: NodeServiceIntent) -> Result<Value> {
        execute(&self.profile, &mut self.processes, intent).await
    }

    /// # Errors
    /// Propagates production journal and process observation errors.
    pub fn pending_for_tests(&mut self, intent: &NodeServiceIntent) -> Result<bool> {
        pending(&self.profile, intent, &mut self.processes)
    }

    /// # Errors
    /// Propagates bounded failure observation errors.
    pub fn failures_for_tests(&self) -> Result<Value> {
        failures(&self.profile.paths.data_root)
    }
}
