use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;
use tokio::process::Command;
use tokio::time::{sleep, Instant};
use uuid::Uuid;

use crate::{
    BootstrapSession, DeploymentProfile, ManifestError, MonitorEvent, MonitorEventKind, MonitorLog,
    MonitorLogError, ServerCredentials,
};

const INITIALIZE: &str = "iceberg-initialize";
const ACTIVATE: &str = "iceberg-activate";
const CAPABILITIES: &str = "0x3fff";
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum IcebergBootstrapError {
    #[error("Iceberg bootstrap profile is invalid: {0}")]
    Profile(&'static str),
    #[error("Iceberg catalog state conflicts with the preview: {0}")]
    Conflict(&'static str),
    #[error("Iceberg management command failed: {0}")]
    Command(&'static str),
    #[error("Iceberg management process failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Iceberg bootstrap manifest failed: {0}")]
    Manifest(#[from] ManifestError),
    #[error("monitor lifecycle log failed: {0}")]
    MonitorLog(#[from] MonitorLogError),
}

#[derive(Deserialize)]
struct CatalogInspection {
    initialized: bool,
    catalog_id: Option<Uuid>,
    display_name: Option<String>,
    activation_epoch: Option<u64>,
    state: Option<String>,
    capability_bits: Option<String>,
    root_operation_id: Option<String>,
}

#[must_use]
pub fn iceberg_step_names() -> [&'static str; 2] {
    [INITIALIZE, ACTIVATE]
}

pub struct IcebergBootstrap;

impl IcebergBootstrap {
    /// # Errors
    /// Refuses a foreign catalog, uncertain identity, or incomplete prior step.
    pub async fn reconcile(
        session: &mut BootstrapSession,
        profile: &DeploymentProfile,
        credentials: &ServerCredentials,
        events: &mut MonitorLog,
    ) -> Result<(), IcebergBootstrapError> {
        let result = Self::reconcile_inner(session, profile, credentials, events).await;
        if result.is_err() {
            record(events, MonitorEventKind::BootstrapFailed, "iceberg").await?;
        }
        result
    }

    async fn reconcile_inner(
        session: &mut BootstrapSession,
        profile: &DeploymentProfile,
        credentials: &ServerCredentials,
        events: &mut MonitorLog,
    ) -> Result<(), IcebergBootstrapError> {
        let service = profile
            .services
            .iter()
            .find(|service| service.id == "iceberg")
            .ok_or(IcebergBootstrapError::Profile("Iceberg service is absent"))?;
        let seeds = service
            .env
            .get("CROWDB_MANAGEMENT_SEEDS")
            .ok_or(IcebergBootstrapError::Profile("management seeds are absent"))?;
        let command = ManagementCommand {
            program: &service.program,
            seeds,
            credentials,
        };
        let name = &profile.iceberg_catalog;
        let initial = command.inspect().await?;
        let catalog_id = Self::initialize(session, &command, name, initial, events).await?;
        Self::activate(session, &command, name, catalog_id, events).await
    }

    async fn initialize(
        session: &mut BootstrapSession,
        command: &ManagementCommand<'_>,
        name: &str,
        mut inspection: CatalogInspection,
        events: &mut MonitorLog,
    ) -> Result<Uuid, IcebergBootstrapError> {
        let complete = session
            .manifest()
            .step_complete(INITIALIZE)
            .ok_or(IcebergBootstrapError::Profile("initialize step is absent"))?;
        if complete {
            let expected =
                session
                    .manifest()
                    .step_catalog(INITIALIZE)
                    .ok_or(IcebergBootstrapError::Conflict(
                        "completed catalog identity is absent",
                    ))?;
            verify_identity(&inspection, name, expected)?;
            return Ok(expected);
        }
        if session.manifest().next_step() != Some(INITIALIZE) {
            return Err(IcebergBootstrapError::Profile("initialize step is out of order"));
        }
        record(events, MonitorEventKind::BootstrapStepStarted, INITIALIZE).await?;
        if !inspection.initialized || inspection.state.as_deref() != Some("Ready") {
            let operation = session.reserve_operation(INITIALIZE)?;
            let arguments = ["initialize", &operation.to_string(), name];
            let _ = command.execute(&arguments).await;
            inspection = command.wait_initialized().await?;
        }
        verify_name_and_epoch(&inspection, name)?;
        let operation = session
            .manifest()
            .step_operation(INITIALIZE)
            .ok_or(IcebergBootstrapError::Conflict("foreign initialized catalog"))?;
        verify_operation(&inspection, operation)?;
        let catalog_id = inspection
            .catalog_id
            .ok_or(IcebergBootstrapError::Conflict("catalog identity is absent"))?;
        session.complete_catalog_step(INITIALIZE, catalog_id)?;
        record(events, MonitorEventKind::BootstrapStepCompleted, INITIALIZE).await?;
        Ok(catalog_id)
    }

    async fn activate(
        session: &mut BootstrapSession,
        command: &ManagementCommand<'_>,
        name: &str,
        catalog_id: Uuid,
        events: &mut MonitorLog,
    ) -> Result<(), IcebergBootstrapError> {
        let complete = session
            .manifest()
            .step_complete(ACTIVATE)
            .ok_or(IcebergBootstrapError::Profile("activate step is absent"))?;
        let mut inspection = command.inspect().await?;
        verify_identity(&inspection, name, catalog_id)?;
        if complete {
            return verify_capabilities(&inspection);
        }
        if session.manifest().next_step() != Some(ACTIVATE) {
            return Err(IcebergBootstrapError::Profile("activate step is out of order"));
        }
        record(events, MonitorEventKind::BootstrapStepStarted, ACTIVATE).await?;
        if inspection.capability_bits.as_deref() == Some("0x0000") {
            let operation = session.reserve_operation(ACTIVATE)?;
            let arguments = ["activate", &operation.to_string(), name, "1", CAPABILITIES];
            let _ = command.execute(&arguments).await;
            inspection = command.wait_capabilities().await?;
        }
        verify_identity(&inspection, name, catalog_id)?;
        verify_capabilities(&inspection)?;
        let operation = session
            .manifest()
            .step_operation(ACTIVATE)
            .ok_or(IcebergBootstrapError::Conflict("foreign catalog activation"))?;
        verify_operation(&inspection, operation)?;
        session.complete_step(ACTIVATE)?;
        record(events, MonitorEventKind::BootstrapStepCompleted, ACTIVATE).await?;
        Ok(())
    }
}

fn verify_name_and_epoch(inspection: &CatalogInspection, name: &str) -> Result<(), IcebergBootstrapError> {
    if !inspection.initialized
        || inspection.display_name.as_deref() != Some(name)
        || inspection.activation_epoch != Some(1)
        || inspection.state.as_deref() != Some("Ready")
    {
        return Err(IcebergBootstrapError::Conflict(
            "catalog name, epoch, or state differs",
        ));
    }
    Ok(())
}

fn verify_identity(
    inspection: &CatalogInspection,
    name: &str,
    catalog_id: Uuid,
) -> Result<(), IcebergBootstrapError> {
    verify_name_and_epoch(inspection, name)?;
    if inspection.catalog_id != Some(catalog_id) {
        return Err(IcebergBootstrapError::Conflict("catalog identity differs"));
    }
    Ok(())
}

fn verify_operation(inspection: &CatalogInspection, operation: Uuid) -> Result<(), IcebergBootstrapError> {
    if inspection.root_operation_id.as_deref() != Some(operation.simple().to_string().as_str()) {
        return Err(IcebergBootstrapError::Conflict(
            "management operation identity differs",
        ));
    }
    Ok(())
}

fn verify_capabilities(inspection: &CatalogInspection) -> Result<(), IcebergBootstrapError> {
    if inspection.capability_bits.as_deref() != Some(CAPABILITIES) {
        return Err(IcebergBootstrapError::Conflict("catalog capabilities differ"));
    }
    Ok(())
}

struct ManagementCommand<'a> {
    program: &'a std::path::Path,
    seeds: &'a str,
    credentials: &'a ServerCredentials,
}

impl ManagementCommand<'_> {
    async fn inspect(&self) -> Result<CatalogInspection, IcebergBootstrapError> {
        let output = self.execute(&["inspect"]).await?;
        let body = std::str::from_utf8(&output)
            .map_err(|_| IcebergBootstrapError::Command("inspection output is invalid"))?;
        let mut states = body
            .lines()
            .filter_map(|line| serde_json::from_str::<CatalogInspection>(line).ok());
        let state = states
            .next()
            .ok_or(IcebergBootstrapError::Command("inspection output is absent"))?;
        if states.next().is_some() {
            return Err(IcebergBootstrapError::Command("inspection output is ambiguous"));
        }
        Ok(state)
    }

    async fn execute(&self, arguments: &[&str]) -> Result<Vec<u8>, IcebergBootstrapError> {
        let server_env = self.credentials.server_env();
        let output = tokio::time::timeout(
            COMMAND_TIMEOUT,
            Command::new(self.program)
                .args(arguments)
                .env("CROWDB_MANAGEMENT_SEEDS", self.seeds)
                .env("CROWDB_ICEBERG_TOKEN", self.credentials.iceberg_manage_token())
                .envs(server_env.lines().filter_map(|line| line.split_once('=')))
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| IcebergBootstrapError::Command("management command timed out"))??;
        if !output.status.success() {
            return Err(IcebergBootstrapError::Command(
                "management command exited unsuccessfully",
            ));
        }
        Ok(output.stdout)
    }

    async fn wait_initialized(&self) -> Result<CatalogInspection, IcebergBootstrapError> {
        self.wait_for(|inspection| inspection.initialized && inspection.state.as_deref() == Some("Ready"))
            .await
    }

    async fn wait_capabilities(&self) -> Result<CatalogInspection, IcebergBootstrapError> {
        self.wait_for(|inspection| inspection.capability_bits.as_deref() == Some(CAPABILITIES))
            .await
    }

    async fn wait_for(
        &self,
        ready: impl Fn(&CatalogInspection) -> bool,
    ) -> Result<CatalogInspection, IcebergBootstrapError> {
        let deadline = Instant::now() + SETTLE_TIMEOUT;
        loop {
            let inspection = self.inspect().await?;
            if ready(&inspection) {
                return Ok(inspection);
            }
            if Instant::now() >= deadline {
                return Err(IcebergBootstrapError::Command(
                    "management result is not yet visible",
                ));
            }
            sleep(Duration::from_millis(200)).await;
        }
    }
}

async fn record(events: &mut MonitorLog, kind: MonitorEventKind, step: &str) -> Result<(), MonitorLogError> {
    events
        .record(&MonitorEvent {
            kind,
            service: Some(step),
            pid: None,
            attempt: None,
        })
        .await
}
