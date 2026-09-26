use std::time::Duration;

use thiserror::Error;
use tokio::process::Command;

use crate::{
    BootstrapSession, ClientCredentials, CredentialError, DeploymentProfile, ManifestError, MonitorEvent,
    MonitorEventKind, MonitorLog, MonitorLogError, ServerCredentials,
};

const STEP: &str = "s3-user";
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum S3BootstrapError {
    #[error("S3 bootstrap profile is invalid: {0}")]
    Profile(&'static str),
    #[error("S3 credential command failed: {0}")]
    Command(&'static str),
    #[error("S3 credential process failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("S3 client credentials failed: {0}")]
    Credentials(#[from] CredentialError),
    #[error("S3 bootstrap manifest failed: {0}")]
    Manifest(#[from] ManifestError),
    #[error("monitor lifecycle log failed: {0}")]
    MonitorLog(#[from] MonitorLogError),
}

#[must_use]
pub fn s3_step_names() -> [&'static str; 1] {
    [STEP]
}

pub struct S3Bootstrap;

impl S3Bootstrap {
    /// # Errors
    /// Rejects absent or conflicting durable users, malformed command output,
    /// and a client credential file that differs from the Group 0 record.
    pub async fn reconcile(
        session: &mut BootstrapSession,
        profile: &DeploymentProfile,
        credentials: &ServerCredentials,
        events: &mut MonitorLog,
    ) -> Result<(), S3BootstrapError> {
        let complete = session
            .manifest()
            .step_complete(STEP)
            .ok_or(S3BootstrapError::Profile("S3 step is absent from manifest"))?;
        if !complete && session.manifest().next_step() != Some(STEP) {
            return Err(S3BootstrapError::Profile("S3 step is out of order"));
        }
        if !complete {
            record(events, MonitorEventKind::BootstrapStepStarted).await?;
        }
        let result = Self::reconcile_inner(session, profile, credentials, complete).await;
        match result {
            Ok(()) => {
                if !complete {
                    record(events, MonitorEventKind::BootstrapStepCompleted).await?;
                }
                Ok(())
            }
            Err(error) => {
                record(events, MonitorEventKind::BootstrapFailed).await?;
                Err(error)
            }
        }
    }

    async fn reconcile_inner(
        session: &mut BootstrapSession,
        profile: &DeploymentProfile,
        credentials: &ServerCredentials,
        complete: bool,
    ) -> Result<(), S3BootstrapError> {
        let service = profile
            .services
            .iter()
            .find(|service| service.id == "s3")
            .ok_or(S3BootstrapError::Profile("S3 service is missing"))?;
        let seeds = service
            .env
            .get("CROWDB_MANAGEMENT_SEEDS")
            .ok_or(S3BootstrapError::Profile("S3 management seeds are missing"))?;
        let s3_endpoint = endpoint(profile, "s3")?;
        let iceberg_endpoint = profile
            .services
            .iter()
            .find(|service| service.id == "iceberg")
            .and_then(|service| service.env.get("CROWDB_ICEBERG_PUBLIC_URI"))
            .cloned()
            .ok_or(S3BootstrapError::Profile("Iceberg public URI is missing"))?;
        let user = format!("preview-{}", session.manifest().deployment_id());
        let action = if complete { "lookup-user" } else { "ensure-user" };
        let output = tokio::time::timeout(
            COMMAND_TIMEOUT,
            Command::new(&service.program)
                .args([action, &user])
                .env("CROWDB_MANAGEMENT_SEEDS", seeds)
                .env("CROWDB_S3_MASTER_KEY", credentials.s3_master_key())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| S3BootstrapError::Command("credential command timed out"))??;
        if !output.status.success() {
            return Err(S3BootstrapError::Command(
                "credential command exited unsuccessfully",
            ));
        }
        let (access_key_id, secret_access_key) = parse_token(&output.stdout)?;
        let client = ClientCredentials {
            s3_endpoint,
            iceberg_endpoint,
            region: service
                .env
                .get("CROWDB_S3_REGION")
                .cloned()
                .ok_or(S3BootstrapError::Profile("S3 region is missing"))?,
            access_key_id,
            secret_access_key,
        };
        if complete {
            credentials.verify_client(&client)?;
        } else {
            credentials.persist_client(&client)?;
            session.complete_step(STEP)?;
        }
        Ok(())
    }
}

fn endpoint(profile: &DeploymentProfile, id: &str) -> Result<String, S3BootstrapError> {
    let port = profile
        .public_endpoints
        .iter()
        .find(|endpoint| endpoint.id == id)
        .ok_or(S3BootstrapError::Profile("public endpoint is missing"))?
        .port;
    Ok(format!("http://localhost:{port}"))
}

fn parse_token(output: &[u8]) -> Result<(String, String), S3BootstrapError> {
    let body =
        std::str::from_utf8(output).map_err(|_| S3BootstrapError::Command("credential output is invalid"))?;
    let mut access_key_id = None;
    let mut secret_access_key = None;
    for line in body.lines() {
        if let Some(value) = line.strip_prefix("AWS_ACCESS_KEY_ID=") {
            if value.is_empty() || access_key_id.replace(value).is_some() {
                return Err(S3BootstrapError::Command("credential output is invalid"));
            }
        }
        if let Some(value) = line.strip_prefix("AWS_SECRET_ACCESS_KEY=") {
            if value.is_empty() || secret_access_key.replace(value).is_some() {
                return Err(S3BootstrapError::Command("credential output is invalid"));
            }
        }
    }
    let (Some(access_key_id), Some(secret_access_key)) = (access_key_id, secret_access_key) else {
        return Err(S3BootstrapError::Command("credential output is invalid"));
    };
    Ok((access_key_id.to_owned(), secret_access_key.to_owned()))
}

async fn record(events: &mut MonitorLog, kind: MonitorEventKind) -> Result<(), MonitorLogError> {
    events
        .record(&MonitorEvent {
            kind,
            service: Some(STEP),
            pid: None,
            attempt: None,
        })
        .await
}
