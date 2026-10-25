// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::Duration;

use super::{durable, AcceptedNode, Result};
use crate::{
    DeploymentProfile, MonitorPhase, MonitorStatus, ProcessManager, ServiceProfile, ServiceStatus,
    StatusStore,
};

pub(super) struct NodeHealth {
    store: StatusStore,
    status: MonitorStatus,
    attempts: BTreeMap<String, u32>,
    probes: crate::ProbeExecutor,
}

impl NodeHealth {
    pub(super) fn new(profile: &DeploymentProfile) -> Result<Self> {
        let identity = super::super::NodeIdentity::load_or_create(&profile.paths.data_root)?;
        Ok(Self {
            store: StatusStore::new(&profile.paths.run_root)?,
            status: MonitorStatus::new(identity.uuid(), MonitorPhase::Initializing),
            attempts: BTreeMap::new(),
            probes: crate::ProbeExecutor::new(true)?,
        })
    }

    pub(super) async fn tick(
        &mut self,
        profile: &DeploymentProfile,
        web: &ServiceProfile,
        processes: &mut ProcessManager,
    ) -> Result<()> {
        if !processes.alive("web")? {
            self.restart_allowed("web")?;
            processes.stop("web", Duration::from_secs(5)).await?;
            processes.start(web, &BTreeMap::new()).await?;
        }
        if let Some(accepted) =
            durable::read::<AcceptedNode>(&profile.paths.data_root.join("accepted-node.json"))?
        {
            if super::admission::is_cancelled(&profile.paths.data_root, &accepted) {
                self.retire_services(profile, processes).await?;
                return Ok(());
            }
            if super::kv::resume_allowed(profile, &accepted)?
                && (!processes.owns("kv") || !processes.alive("kv")?)
            {
                self.restart_allowed("kv")?;
                super::kv::ensure_process(profile, processes, &accepted).await?;
            }
        }
        self.reconcile_services(profile, processes).await?;
        self.status.services.clear();
        let retained = super::services::retained(profile)?;
        let mut ids = vec!["web".to_owned(), "kv".to_owned()];
        ids.extend(retained.iter().cloned().map(|intent| intent.service_id));
        for id in &ids {
            if processes.owns(id) {
                let mut healthy = processes.alive(id)?;
                if healthy {
                    if id == "web" || id == "kv" {
                        let service = if id == "web" {
                            web.clone()
                        } else {
                            super::service(profile, "kv")?
                        };
                        healthy = self
                            .probes
                            .probe_service(&service, &BTreeMap::new())
                            .await
                            .is_ok();
                    } else if let Some(intent) = retained
                        .iter()
                        .find(|intent| intent.service_id == *id && intent.kind != "kv")
                    {
                        healthy = match super::probes::for_intent(profile, intent) {
                            Ok(service) => self
                                .probes
                                .probe_service(&service, &BTreeMap::new())
                                .await
                                .is_ok(),
                            Err(_) => false,
                        };
                    }
                }
                self.status.services.insert(
                    id.clone(),
                    ServiceStatus {
                        operation_id: retained
                            .iter()
                            .find(|intent| intent.service_id == *id || (id == "kv" && intent.kind == "kv"))
                            .map(|intent| intent.operation_id.clone()),
                        pid: processes.pid(id),
                        generation: 1 + u64::from(*self.attempts.get(id.as_str()).unwrap_or(&0)),
                        healthy,
                        restart_attempts: *self.attempts.get(id.as_str()).unwrap_or(&0),
                    },
                );
            }
        }
        self.status.phase = if self.status.services.values().all(|service| service.healthy) {
            MonitorPhase::Ready
        } else {
            MonitorPhase::Restarting
        };
        self.store.publish(&mut self.status)?;
        Ok(())
    }

    pub(in crate::node::runtime) async fn reconcile_services(
        &mut self,
        profile: &DeploymentProfile,
        processes: &mut ProcessManager,
    ) -> Result<()> {
        if !profile.paths.data_root.join("node-binding.json").exists() {
            return Ok(());
        }
        let intents = match super::services::desired(profile).await {
            Ok(intents) => intents,
            Err(error) => {
                tracing::debug!(%error, "service recovery awaits committed intents");
                return Ok(());
            }
        };
        for intent in &intents {
            if !super::services::pending(profile, intent, processes)? {
                continue;
            }
            if let Err(error) = super::services::dependencies_ready(profile, intent, &intents).await {
                tracing::debug!(service = %intent.service_id, %error, "service recovery waits for dependencies");
                continue;
            }
            match super::services::execute(profile, processes, intent.clone()).await {
                Ok(_) => {
                    *self.attempts.entry(intent.service_id.clone()).or_default() += 1;
                }
                Err(error) => eprintln!("service recovery deferred for {}: {error}", intent.service_id),
            }
        }
        Ok(())
    }

    fn restart_allowed(&mut self, id: &str) -> Result<()> {
        let attempts = self.attempts.entry(id.into()).or_default();
        if *attempts >= 5 {
            self.status.phase = MonitorPhase::Failed;
            self.store.publish(&mut self.status)?;
            return Err(format!("{id} exceeded monitor restart budget").into());
        }
        *attempts += 1;
        Ok(())
    }

    async fn retire_services(
        &mut self,
        profile: &DeploymentProfile,
        processes: &mut ProcessManager,
    ) -> Result<()> {
        let mut ids: Vec<_> = super::services::retained(profile)?
            .into_iter()
            .map(|intent| intent.service_id)
            .collect();
        ids.reverse();
        ids.push("kv".into());
        for id in ids {
            if processes.owns(&id) && processes.alive(&id)? {
                processes.stop(&id, Duration::from_secs(5)).await?;
            }
        }
        self.status.services.clear();
        self.status.phase = MonitorPhase::Failed;
        self.store.publish(&mut self.status)?;
        Ok(())
    }
}

#[cfg(feature = "test-util")]
/// Collect one production health pass for owned processes.
///
/// # Errors
/// Propagates process, durable state and status publication failures.
pub async fn node_health_for_tests(
    profile: &DeploymentProfile,
    web: &ServiceProfile,
    processes: &mut ProcessManager,
) -> Result<MonitorStatus> {
    let mut health = NodeHealth::new(profile)?;
    health.tick(profile, web, processes).await?;
    Ok(health.status)
}
