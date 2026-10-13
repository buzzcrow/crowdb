// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::path::Path;
use std::time::Duration;

use thiserror::Error;
use tokio::time::sleep;

use crate::{
    render_configs, BootstrapSession, ChunkBootstrapError, CrashRetention, CredentialError,
    DeploymentProfile, DiskBootstrapError, HardwareBootstrapError, IcebergBootstrapError, KvBootstrap,
    KvBootstrapError, LivenessError, LivenessServer, LogicalBootstrapError, ManifestError, ManifestState,
    MonitorLogError, ProfileError, RenderError, S3BootstrapError, ServerCredentials, StorageProbeError,
    Supervisor, SupervisorError,
};

mod probe;
mod profile;
mod recovery;
mod startup;

use profile::{
    bind_single_node, config_digest_input, ensure_directory, kv_root, management_seed, require_directory,
};
use recovery::run_ready_services;
use startup::bootstrap_services;

const PROFILE_NAME: &str = "single-node-container";
const MAX_TEMPLATE_BYTES: u64 = 1024 * 1024;
const KV_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Error)]
pub enum PreviewError {
    #[error("preview profile failed: {0}")]
    Profile(#[from] ProfileError),
    #[error("preview filesystem failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("preview configuration failed: {0}")]
    Render(#[from] RenderError),
    #[error("preview manifest failed: {0}")]
    Manifest(#[from] ManifestError),
    #[error("preview credentials failed: {0}")]
    Credentials(#[from] CredentialError),
    #[error("preview liveness service failed: {0}")]
    Liveness(#[from] LivenessError),
    #[error("preview lifecycle log failed: {0}")]
    MonitorLog(#[from] MonitorLogError),
    #[error("preview supervision failed: {0}")]
    Supervisor(#[from] SupervisorError),
    #[error("preview KV bootstrap failed: {0}")]
    Kv(#[from] KvBootstrapError),
    #[error("preview disk bootstrap failed: {0}")]
    Disk(#[from] DiskBootstrapError),
    #[error("preview hardware bootstrap failed: {0}")]
    Hardware(#[from] HardwareBootstrapError),
    #[error("preview logical bootstrap failed: {0}")]
    Logical(#[from] LogicalBootstrapError),
    #[error("preview disk readiness failed: {0}")]
    Storage(#[from] StorageProbeError),
    #[error("preview chunk readiness failed: {0}")]
    Chunk(#[from] ChunkBootstrapError),
    #[error("preview S3 bootstrap failed: {0}")]
    S3(#[from] S3BootstrapError),
    #[error("preview Iceberg bootstrap failed: {0}")]
    Iceberg(#[from] IcebergBootstrapError),
    #[error("preview Web authority probe failed: {0}")]
    WebAuthority(&'static str),
    #[error("preview Web authority rejected bootstrap: {0}")]
    WebAuthorityUnavailable(String),
    #[error("preview state is invalid: {0}")]
    Invalid(&'static str),
}

/// Describe the durable bootstrap sequence for a validated release service set.
/// # Errors
/// Rejects invalid profiles, unknown services and missing runtime dependencies.
pub fn preview_step_names(profile: &DeploymentProfile) -> Result<Vec<String>, PreviewError> {
    profile.validate()?;
    if profile.name != PROFILE_NAME {
        return Err(PreviewError::Invalid("unsupported automatic startup profile"));
    }
    management_seed(profile)?;
    profile::kv_root(profile)?;
    profile::step_names(profile)
}

/// # Errors
/// Fails closed on incompatible durable state or any unready child.
pub async fn run_preview(profile_path: &Path) -> Result<(), PreviewError> {
    let profile_bytes = fs::read(profile_path)?;
    let mut profile = DeploymentProfile::parse(
        std::str::from_utf8(&profile_bytes).map_err(|_| PreviewError::Invalid("profile is not UTF-8"))?,
    )?;
    if profile.name != PROFILE_NAME {
        return Err(PreviewError::Invalid(
            "run supports only the named preview profile",
        ));
    }
    apply_public_uri_overrides(&mut profile);
    let config_bytes = config_digest_input(&profile)?;
    let steps = preview_step_names(&profile)?;
    let management_seed = management_seed(&profile)?;
    let step_refs = steps.iter().map(String::as_str).collect::<Vec<_>>();
    let mut session = BootstrapSession::open(
        &profile.paths.data_root,
        &profile_bytes,
        &config_bytes,
        &step_refs,
    )?;
    if let Some(root) = std::env::var_os("CROWDB_CORE_DIR") {
        let crashes = CrashRetention::open(root.into())?;
        std::env::set_current_dir(crashes.root())?;
    }
    let credentials = if session.manifest().state() == ManifestState::Ready
        || session.manifest().step_complete("s3-user") == Some(true)
    {
        ServerCredentials::load_existing(&profile.paths.data_root)?
    } else {
        ServerCredentials::load_or_create(&profile.paths.data_root)?
    };
    ensure_directory(&profile.paths.run_root)?;
    let _liveness = LivenessServer::start(&profile.paths.run_root)?;
    ensure_directory(&profile.paths.log_root)?;
    let monitor_log_root = profile.paths.log_root.join("monitor");
    ensure_directory(&monitor_log_root)?;
    crowdb_rpc_ffi::logging::init_logging(
        &monitor_log_root.to_string_lossy(),
        "warn",
        usize::try_from(profile.logs.max_file_bytes.div_ceil(1024 * 1024))
            .map_err(|_| PreviewError::Invalid("RPC log limit is invalid"))?,
        usize::from(profile.logs.max_files),
        "rpc",
    );
    crowdb_rpc_ffi::logging::add_log_stderr("error");
    let kv_root = kv_root(&profile)?;
    if session.manifest().state() == ManifestState::Ready {
        require_directory(&profile.paths.data_root.join("kv"))?;
        require_directory(&kv_root)?;
    } else {
        ensure_directory(&profile.paths.data_root.join("kv"))?;
        ensure_directory(&kv_root)?;
    }
    render_configs(&profile, &profile.paths.template_root, &profile.paths.run_root)?;
    bind_single_node(&profile, &session, &management_seed)?;
    let mut supervisor = Supervisor::new(
        profile.clone(),
        session.manifest().deployment_id(),
        &profile.paths.log_root,
        &profile.paths.run_root,
    )
    .await?;
    supervisor.require_recovery_validation();
    let startup = async {
        bootstrap_services(
            &mut supervisor,
            &mut session,
            &profile,
            &credentials,
            &management_seed,
        )
        .await?;
        session.mark_ready()?;
        supervisor.mark_ready().await?;
        Ok::<(), PreviewError>(())
    }
    .await;
    if let Err(error) = startup {
        supervisor.shutdown().await?;
        return Err(error);
    }
    eprintln!("CROWDB Single-Node Container preview ready; retrieve credentials with crowdb-monitor credentials show --format env");
    let runtime = run_ready_services(
        &mut supervisor,
        &profile,
        &profile_bytes,
        &config_bytes,
        &step_refs,
        &credentials,
        &management_seed,
    )
    .await;
    supervisor.shutdown().await?;
    runtime
}

fn apply_public_uri_overrides(profile: &mut DeploymentProfile) {
    let Some(access) = profile.services.iter_mut().find(|service| service.id == "access") else {
        return;
    };
    for name in ["CROWDB_S3_PUBLIC_URI", "CROWDB_ICEBERG_PUBLIC_URI"] {
        if let Ok(value) = std::env::var(name) {
            if !value.is_empty() {
                access.env.insert(name.to_owned(), value);
            }
        }
    }
}

async fn reconcile_kv_with_retry(
    supervisor: &mut Supervisor,
    session: &mut BootstrapSession,
    profile: &DeploymentProfile,
    management_seed: &str,
) -> Result<(), PreviewError> {
    let bootstrap = KvBootstrap::new(management_seed)?;
    loop {
        match bootstrap
            .reconcile(session, profile, supervisor.monitor_log_mut())
            .await
        {
            Ok(()) => return Ok(()),
            Err(KvBootstrapError::LeadershipDeadline) => {
                eprintln!("KV leadership has not converged; continuing supervision and retrying");
                supervisor.refresh_status()?;
                supervisor.poll_once().await?;
                sleep(KV_RETRY_DELAY).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}
