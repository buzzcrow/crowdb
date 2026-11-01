// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read-only authority work cannot hold up current local health observations.

use std::time::{Duration, Instant};

use crowdb_protocol::mgmt::node::{NodeBinding, NodeServiceIntent};

use super::super::{durable, services, Result};
use crate::DeploymentProfile;

struct Observation {
    binding: NodeBinding,
    started: Instant,
    intents: Vec<NodeServiceIntent>,
}

#[derive(Default)]
pub(super) struct ServiceIntents {
    task: Option<tokio::task::JoinHandle<Result<Observation>>>,
}

impl ServiceIntents {
    pub(super) async fn poll(&mut self, profile: &DeploymentProfile) -> Option<Vec<NodeServiceIntent>> {
        let binding = match durable::read::<NodeBinding>(&profile.paths.data_root.join("node-binding.json")) {
            Ok(Some(binding)) => binding,
            Ok(None) => {
                self.stop().await;
                return None;
            }
            Err(error) => {
                tracing::debug!(%error, "service intent observation awaits a valid local binding");
                return None;
            }
        };
        let mut ready = None;
        if self
            .task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            let task = self.task.take()?;
            match task.await {
                Ok(Ok(observation))
                    if observation.binding == binding
                        && observation.started.elapsed() <= Duration::from_secs(3) =>
                {
                    ready = Some(observation.intents);
                }
                Ok(Ok(_)) => tracing::debug!("discarded expired or rebound service intent observation"),
                Ok(Err(error)) => tracing::debug!(%error, "service recovery awaits committed intents"),
                Err(error) => {
                    tracing::warn!(%error, "service intent worker failed; next health pass starts another read");
                }
            }
        }
        if self.task.is_none() {
            let profile = profile.clone();
            self.task = Some(tokio::spawn(async move {
                let started = Instant::now();
                let intents = services::desired(&profile).await?;
                if durable::read::<NodeBinding>(&profile.paths.data_root.join("node-binding.json"))?.as_ref()
                    != Some(&binding)
                {
                    return Err("service intent binding changed during observation".into());
                }
                Ok(Observation {
                    binding,
                    started,
                    intents,
                })
            }));
        }
        ready
    }

    pub(super) async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            if let Err(error) = task.await {
                if !error.is_cancelled() {
                    tracing::warn!(%error, "service intent worker stopped unexpectedly");
                }
            }
        }
    }
}

impl Drop for ServiceIntents {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
