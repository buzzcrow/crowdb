// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    execute, failures, pending, DeploymentProfile, NodeServiceIntent, ProcessManager, Result, Value,
};

/// Exercise durable service execution with the real process manager.
pub struct NodeServiceForTests {
    profile: DeploymentProfile,
    processes: ProcessManager,
    health: Option<super::super::health::NodeHealth>,
}

impl NodeServiceForTests {
    /// # Errors
    /// Runs the production dependency probes without launching a process.
    pub async fn dependencies_ready_for_tests(
        profile: &DeploymentProfile,
        intent: &NodeServiceIntent,
        intents: &[NodeServiceIntent],
    ) -> Result<()> {
        super::dependencies_ready(profile, intent, intents).await
    }

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
        Ok(Self {
            profile,
            processes,
            health: None,
        })
    }

    /// Resume retained KV ownership without waiting for a healthy quorum.
    ///
    /// # Errors
    /// Propagates production retained-trust and process startup errors.
    pub async fn resume_for_tests(&mut self) -> Result<()> {
        super::super::kv::resume(&self.profile, &mut self.processes).await
    }

    /// # Errors
    /// Propagates production recovery and current health observation errors.
    pub async fn tick_for_tests(&mut self) -> Result<()> {
        let web = super::super::service(&self.profile, "web")?;
        if !self.processes.owns("web") {
            self.processes
                .start(&web, &std::collections::BTreeMap::new())
                .await?;
        }
        if self.health.is_none() {
            self.health = Some(super::super::health::NodeHealth::new(&self.profile)?);
        }
        self.health
            .as_mut()
            .ok_or("missing test health worker")?
            .tick(&self.profile, &web, &mut self.processes)
            .await
    }

    /// # Errors
    /// Propagates failure to stop the owned KV child.
    pub async fn stop_kv_for_tests(&mut self) -> Result<()> {
        self.processes
            .stop("kv", std::time::Duration::from_secs(1))
            .await?;
        Ok(())
    }

    /// # Errors
    /// Propagates production authority, process and readiness errors.
    pub async fn execute_for_tests(&mut self, intent: NodeServiceIntent) -> Result<Value> {
        execute(&self.profile, &mut self.processes, intent).await
    }

    /// Reconcile committed recovery intents through the production dependency gate.
    ///
    /// # Errors
    /// Propagates authority, journal and process observation errors.
    pub async fn reconcile_for_tests(&mut self) -> Result<()> {
        if self.health.is_none() {
            self.health = Some(super::super::health::NodeHealth::new(&self.profile)?);
        }
        self.health
            .as_mut()
            .ok_or("missing test health worker")?
            .reconcile_services(&self.profile, &mut self.processes)
            .await
    }

    /// # Errors
    /// Propagates production journal and process observation errors.
    pub fn pending_for_tests(&mut self, intent: &NodeServiceIntent) -> Result<bool> {
        pending(&self.profile, intent, &mut self.processes)
    }

    /// # Errors
    /// Propagates bounded failure observation errors.
    pub fn failures_for_tests(&mut self) -> Result<Value> {
        failures(&self.profile.paths.data_root, &self.processes.live_pids()?)
    }
}
