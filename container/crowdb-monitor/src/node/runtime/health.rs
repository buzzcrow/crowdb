// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::Duration;

use super::{durable, start_kv, AcceptedNode, Result};
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
            let retired = profile.paths.data_root.join(format!(
                "retired-bootstrap-{}.json",
                accepted.bootstrap.operation_id
            ));
            if accepted.prepared
                && !super::admission::is_cancelled(&profile.paths.data_root, &accepted)
                && !retired.exists()
                && !super::services::kv_paused(profile)?
                && (!processes.owns("kv") || !processes.alive("kv")?)
            {
                self.restart_allowed("kv")?;
                start_kv(profile, processes, &accepted).await?;
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
                    if let Some(intent) = retained
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

    async fn reconcile_services(
        &mut self,
        profile: &DeploymentProfile,
        processes: &mut ProcessManager,
    ) -> Result<()> {
        if !profile.paths.data_root.join("node-binding.json").exists() {
            return Ok(());
        }
        let Ok(intents) = super::services::desired(profile).await else {
            return Ok(());
        };
        for intent in intents {
            if !super::services::pending(profile, &intent, processes)? {
                continue;
            }
            if *self.attempts.get(&intent.service_id).unwrap_or(&0) >= 5 {
                continue;
            }
            match super::services::execute(profile, processes, intent.clone()).await {
                Ok(_) => {
                    *self.attempts.entry(intent.service_id).or_default() += 1;
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
}
