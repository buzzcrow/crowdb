// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read-only probes share a health pass instead of serializing their deadlines.

use super::{NodeHealth, Result};
use crate::{DeploymentProfile, MonitorPhase, ProcessManager, ServiceProfile, ServiceStatus};
use std::collections::BTreeMap;

impl NodeHealth {
    pub(super) async fn observe(
        &mut self,
        profile: &DeploymentProfile,
        web: &ServiceProfile,
        processes: &mut ProcessManager,
    ) -> Result<()> {
        let retained = super::super::services::retained(profile)?;
        let mut ids = vec!["web".to_owned(), "kv".to_owned()];
        ids.extend(retained.iter().map(|intent| intent.service_id.clone()));
        let mut jobs = Vec::new();
        for id in ids {
            if !processes.owns(&id) {
                continue;
            }
            let intent = retained
                .iter()
                .find(|intent| intent.service_id == id || (id == "kv" && intent.kind == "kv"));
            let service = match id.as_str() {
                "web" => Some(web.clone()),
                "kv" => Some(super::super::service(profile, "kv")?),
                _ => intent.and_then(|intent| super::super::probes::for_intent(profile, intent).ok()),
            };
            let attempts = *self.attempts.get(&id).unwrap_or(&0);
            jobs.push((
                id.clone(),
                service,
                ServiceStatus {
                    operation_id: intent.map(|intent| intent.operation_id.clone()),
                    pid: processes.pid(&id),
                    generation: 1 + u64::from(attempts),
                    healthy: processes.alive(&id)?,
                    restart_attempts: attempts,
                },
            ));
        }
        let probes = &self.probes;
        let observations =
            futures::future::join_all(jobs.into_iter().map(|(id, service, mut status)| async move {
                status.healthy = if status.healthy {
                    match service {
                        Some(service) => probes.probe_service(&service, &BTreeMap::new()).await.is_ok(),
                        None => false,
                    }
                } else {
                    false
                };
                (id, status)
            }))
            .await;
        self.status.services.clear();
        for (id, mut status) in observations {
            let alive = processes.alive(&id)?;
            let current_pid = processes.pid(&id);
            status.healthy &= alive && current_pid == status.pid;
            status.pid = current_pid;
            self.status.services.insert(id, status);
        }
        self.status.phase = if self.status.services.values().all(|service| service.healthy) {
            MonitorPhase::Ready
        } else {
            MonitorPhase::Restarting
        };
        self.store.publish(&mut self.status)?;
        Ok(())
    }
}
