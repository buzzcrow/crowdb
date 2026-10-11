// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::probe::verify_web_authority;
use super::profile::{has_service, kv_root};
use super::{reconcile_kv_with_retry, PreviewError};
use crate::{
    ensure_disk_files, verify_chunk_services, verify_diskio_disks, BootstrapSession, DeploymentProfile,
    HardwareBootstrap, IcebergBootstrap, IcebergBootstrapError, LogicalBootstrap, ManifestState, S3Bootstrap,
    ServerCredentials, Supervisor,
};
use std::time::Duration;
use tokio::time::{sleep, Instant};

pub(super) async fn run_ready_services(
    supervisor: &mut Supervisor,
    profile: &DeploymentProfile,
    profile_bytes: &[u8],
    config_bytes: &[u8],
    steps: &[&str],
    credentials: &ServerCredentials,
    management_seed: &str,
) -> Result<(), PreviewError> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            _ = terminate.recv() => break,
            _ = tokio::signal::ctrl_c() => break,
            result = supervisor.poll_once() => result?,
        }
        if supervisor.recovery_pending() {
            let recovery_epoch = supervisor.recovery_epoch();
            tokio::select! {
                _ = terminate.recv() => break,
                _ = tokio::signal::ctrl_c() => break,
                result = validate_recovery(supervisor, profile, profile_bytes, config_bytes, steps, credentials, management_seed) => result?,
            }
            supervisor.poll_once().await?;
            if supervisor.recovery_pending()
                && supervisor.recovery_epoch() == recovery_epoch
                && supervisor
                    .status()
                    .services
                    .values()
                    .all(|service| service.healthy)
            {
                supervisor.finish_recovery().await?;
            }
        }
        tokio::select! {
            _ = terminate.recv() => break,
            _ = tokio::signal::ctrl_c() => break,
            () = sleep(Duration::from_secs(1)) => {},
        }
    }
    Ok(())
}

async fn validate_recovery(
    supervisor: &mut Supervisor,
    profile: &DeploymentProfile,
    profile_bytes: &[u8],
    config_bytes: &[u8],
    steps: &[&str],
    credentials: &ServerCredentials,
    management_seed: &str,
) -> Result<(), PreviewError> {
    let mut session = BootstrapSession::open(&profile.paths.data_root, profile_bytes, config_bytes, steps)?;
    if session.manifest().state() != ManifestState::Ready
        || session.manifest().deployment_id() != supervisor.status().deployment_id
    {
        return Err(PreviewError::Invalid("recovered deployment identity differs"));
    }
    let persisted_credentials = ServerCredentials::load_existing(&profile.paths.data_root)?;
    if persisted_credentials.server_env() != credentials.server_env() {
        return Err(PreviewError::Invalid("recovered server credentials differ"));
    }
    super::profile::require_directory(&kv_root(profile)?)?;
    reconcile_kv_with_retry(supervisor, &mut session, profile, management_seed).await?;
    if has_service(profile, "diskdb") {
        ensure_disk_files(&mut session, profile, supervisor.monitor_log_mut()).await?;
        HardwareBootstrap::new(management_seed.to_owned())
            .reconcile(&mut session, profile, supervisor.monitor_log_mut())
            .await?;
    }
    LogicalBootstrap::new(management_seed.to_owned())
        .reconcile(&mut session, profile, supervisor.monitor_log_mut())
        .await?;
    if has_service(profile, "diskio") {
        verify_diskio_disks(management_seed, profile).await?;
    }
    if has_service(profile, "chunkdb") {
        verify_chunk_services(management_seed, profile).await?;
    }
    if has_service(profile, "access") {
        S3Bootstrap::reconcile(&mut session, profile, credentials, supervisor.monitor_log_mut()).await?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match IcebergBootstrap::reconcile(
                &mut session,
                profile,
                credentials,
                supervisor.monitor_log_mut(),
            )
            .await
            {
                Ok(()) => break,
                Err(IcebergBootstrapError::Command(_)) if Instant::now() < deadline => {
                    supervisor.refresh_status()?;
                    sleep(Duration::from_millis(200)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    if !has_service(profile, "web") {
        return Ok(());
    }
    verify_web_authority(supervisor, profile).await
}
