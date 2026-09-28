// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Launch-only service lifecycle shared by CLI and bare-metal Web.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::config::web::{LaunchRecord, LaunchRegistry};
use crate::error::{Error, Result};

mod local;
mod remote;
mod runtime;

pub use runtime::ProcessIdentity;

/// Private runtime process identities, separate from durable launch policy.
pub struct LaunchRuntime {
    root: PathBuf,
    credential_root: PathBuf,
}

impl LaunchRuntime {
    /// Use the registry's sibling runtime directory so CLI and Web share process identities.
    /// # Errors
    /// Rejects a missing or inaccessible registry path.
    pub fn for_registry(path: &Path) -> Result<Self> {
        Ok(Self::new(std::fs::canonicalize(path)?.with_extension("runtime")))
    }

    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        let credential_root =
            dirs::home_dir().map_or_else(|| root.join("credentials"), |home| home.join(".ssh"));
        Self {
            root,
            credential_root,
        }
    }

    #[must_use]
    pub fn with_credential_root(mut self, root: PathBuf) -> Self {
        self.credential_root = root;
        self
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// # Errors
    /// Rejects invalid launch inputs or corrupt runtime identity.
    pub async fn status(&self, launch: &LaunchRecord) -> Result<Option<ProcessIdentity>> {
        validate(launch)?;
        let Some(identity) = runtime::load(&self.root, launch)? else {
            return Ok(None);
        };
        let live = if launch.is_local() {
            runtime::local_identity(identity.pid)?
        } else {
            remote::identity(launch, &self.credential_root, identity.pid).await?
        };
        Ok((live == Some(identity)).then_some(identity))
    }

    /// Start a configured process, preserving an already running matching identity.
    /// # Errors
    /// Reports invalid configuration, launch, readiness, and runtime write failures.
    pub async fn start(&self, launch: &LaunchRecord) -> Result<ProcessIdentity> {
        if let Some(identity) = self.status(launch).await? {
            return Ok(identity);
        }
        if let Some(url) = &launch.readiness_url {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_millis(500))
                .build()
                .map_err(|error| Error::Config(error.to_string()))?;
            if client
                .get(url)
                .send()
                .await
                .is_ok_and(|reply| reply.status().is_success())
            {
                return Err(Error::Conflict {
                    kind: "running service without matching launch identity".into(),
                    id: format!("{}/{}", launch.node_id, launch.service_id),
                });
            }
        }
        if launch.is_local() {
            local::start(&self.root, launch).await
        } else {
            remote::start(&self.root, launch, &self.credential_root).await
        }
    }

    /// Stop only the process whose identity matches this runtime record.
    /// # Errors
    /// Reports corrupt runtime state or a process that cannot stop.
    pub async fn stop(&self, launch: &LaunchRecord) -> Result<()> {
        if let Some(identity) = self.status(launch).await? {
            if launch.is_local() {
                crate::lifecycle::stop_pid_with_timeout(identity.pid, Duration::from_secs(15))?;
                if runtime::local_identity(identity.pid)? == Some(identity) {
                    return Err(Error::NodeUnreachable {
                        node_id: launch.node_id.to_string(),
                        reason: "process did not stop".into(),
                    });
                }
            } else {
                remote::stop(launch, &self.credential_root, identity).await?;
            }
        }
        match std::fs::remove_file(runtime::path(&self.root, launch)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// # Errors
    /// Returns a stop or launch failure.
    pub async fn restart(&self, launch: &LaunchRecord) -> Result<ProcessIdentity> {
        self.stop(launch).await?;
        self.start(launch).await
    }

    /// # Errors
    /// Rejects invalid registry entries or any failed auto-start.
    pub async fn start_enabled(&self, registry: &LaunchRegistry) -> Result<Vec<ProcessIdentity>> {
        registry.validate()?;
        let mut started = Vec::new();
        for launch in registry.launches.iter().filter(|launch| launch.auto_start) {
            started.push(self.start(launch).await?);
        }
        Ok(started)
    }
}

fn validate(launch: &LaunchRecord) -> Result<()> {
    LaunchRegistry {
        version: 1,
        launches: vec![launch.clone()],
    }
    .validate()
}
