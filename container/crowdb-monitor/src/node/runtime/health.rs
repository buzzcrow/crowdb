// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::Duration;

use super::{durable, AcceptedNode, Result};
use crate::{DeploymentProfile, MonitorPhase, MonitorStatus, ProcessManager, ServiceProfile, StatusStore};

mod intents;
mod observations;

pub(super) struct NodeHealth {
    store: StatusStore,
    status: MonitorStatus,
    attempts: BTreeMap<String, u32>,
    probe_failures: BTreeMap<String, u32>,
    probes: crate::ProbeExecutor,
    intents: intents::ServiceIntents,
}

impl NodeHealth {
    pub(super) fn new(profile: &DeploymentProfile) -> Result<Self> {
        let identity = super::super::NodeIdentity::load_or_create(&profile.paths.data_root)?;
        Ok(Self {
            store: StatusStore::new(&profile.paths.run_root)?,
            status: MonitorStatus::new(identity.uuid(), MonitorPhase::Initializing),
            attempts: BTreeMap::new(),
            probe_failures: BTreeMap::new(),
            probes: crate::ProbeExecutor::new(true)?,
            intents: intents::ServiceIntents::default(),
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
        self.observe(profile, web, processes).await
    }

    pub(in crate::node::runtime) async fn reconcile_services(
        &mut self,
        profile: &DeploymentProfile,
        processes: &mut ProcessManager,
    ) -> Result<()> {
        if let Some(intents) = self.intents.poll(profile).await {
            self.apply_services(profile, processes, &intents).await?;
        }
        Ok(())
    }

    async fn apply_services(
        &mut self,
        profile: &DeploymentProfile,
        processes: &mut ProcessManager,
        intents: &[crowdb_protocol::mgmt::node::NodeServiceIntent],
    ) -> Result<()> {
        for intent in intents {
            let pending = super::services::pending(profile, intent, processes)?;
            let restart_unhealthy = if !pending
                && matches!(
                    intent.action,
                    crowdb_protocol::mgmt::node::NodeServiceAction::Start
                        | crowdb_protocol::mgmt::node::NodeServiceAction::Restart
                ) {
                self.restart_if_unhealthy(profile, intent, processes).await?
            } else {
                false
            };
            if !pending && !restart_unhealthy {
                continue;
            }
            if let Err(error) = super::services::dependencies_ready(profile, intent, intents).await {
                tracing::debug!(service = %intent.service_id, %error, "service recovery waits for dependencies");
                continue;
            }
            if restart_unhealthy {
                if let Err(error) = super::services::stop_unhealthy(profile, processes, intent).await {
                    tracing::debug!(service = %intent.service_id, %error, "unhealthy service recovery awaits current authority");
                    continue;
                }
            }
            match super::services::reconcile(profile, processes, intent.clone()).await {
                Ok(_) => {
                    *self.attempts.entry(intent.service_id.clone()).or_default() += 1;
                    self.probe_failures.remove(&intent.service_id);
                }
                Err(error) => eprintln!("service recovery deferred for {}: {error}", intent.service_id),
            }
        }
        Ok(())
    }

    pub(super) async fn stop(&mut self) {
        self.intents.stop().await;
    }

    #[cfg(feature = "test-util")]
    pub(in crate::node::runtime) async fn reconcile_services_for_tests(
        &mut self,
        profile: &DeploymentProfile,
        processes: &mut ProcessManager,
    ) -> Result<()> {
        let intents = super::services::desired(profile).await?;
        self.apply_services(profile, processes, &intents).await
    }

    async fn restart_if_unhealthy(
        &mut self,
        profile: &DeploymentProfile,
        intent: &crowdb_protocol::mgmt::node::NodeServiceIntent,
        processes: &mut ProcessManager,
    ) -> Result<bool> {
        let process_id = if intent.kind == "kv" {
            "kv"
        } else {
            &intent.service_id
        };
        if !processes.owns(process_id) || !processes.alive(process_id)? {
            self.probe_failures.remove(&intent.service_id);
            return Ok(false);
        }
        // An ownership handover can withdraw readiness while the service is
        // still advancing its durable transition. Restart only a failed live
        // listener; keep readiness probes for the published health snapshot.
        let service = match super::probes::liveness_for_intent(profile, intent) {
            Ok(service) => service,
            Err(error) => {
                tracing::debug!(service = %intent.service_id, %error, "service health probe unavailable");
                return Ok(false);
            }
        };
        if self
            .probes
            .probe_service(&service, &BTreeMap::new())
            .await
            .is_ok()
        {
            self.probe_failures.remove(&intent.service_id);
            return Ok(false);
        }
        let failures = self.probe_failures.entry(intent.service_id.clone()).or_default();
        *failures = failures.saturating_add(1);
        Ok(*failures >= service.probe.failure_threshold)
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
