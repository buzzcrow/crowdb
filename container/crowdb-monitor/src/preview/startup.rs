// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;

use super::probe::{record_bootstrap_probe, verify_bootstrap_probe, verify_web_authority};
use super::profile::has_service;
use super::{reconcile_kv_with_retry, PreviewError};
use crate::{
    ensure_disk_files, verify_chunk_services, verify_diskio_disks, BootstrapSession, DeploymentProfile,
    HardwareBootstrap, IcebergBootstrap, LogicalBootstrap, MonitorEventKind, S3Bootstrap, ServerCredentials,
    Supervisor,
};

pub(super) async fn bootstrap_services(
    supervisor: &mut Supervisor,
    session: &mut BootstrapSession,
    profile: &DeploymentProfile,
    credentials: &ServerCredentials,
    management_seed: &str,
) -> Result<(), PreviewError> {
    supervisor.start_service("kv", BTreeMap::new()).await?;
    reconcile_kv_with_retry(supervisor, session, profile, management_seed).await?;
    if has_service(profile, "diskdb") {
        ensure_disk_files(session, profile, supervisor.monitor_log_mut()).await?;
        HardwareBootstrap::new(management_seed.to_owned())
            .reconcile(session, profile, supervisor.monitor_log_mut())
            .await?;
    }
    LogicalBootstrap::new(management_seed.to_owned())
        .reconcile(session, profile, supervisor.monitor_log_mut())
        .await?;
    if has_service(profile, "diskdb") {
        supervisor.start_service("diskdb", BTreeMap::new()).await?;
    }
    if has_service(profile, "diskio") {
        supervisor.start_service("diskio", BTreeMap::new()).await?;
        verify_bootstrap_probe(supervisor, "diskio-authority", async {
            verify_diskio_disks(management_seed, profile)
                .await
                .map_err(Into::into)
        })
        .await?;
    }
    if has_service(profile, "chunkdb") {
        supervisor.start_service("chunkdb", BTreeMap::new()).await?;
    }
    if has_service(profile, "chunk-kv") {
        supervisor.start_service("chunk-kv", BTreeMap::new()).await?;
    }
    if has_service(profile, "chunkdb") {
        verify_bootstrap_probe(supervisor, "chunk-authority", async {
            verify_chunk_services(management_seed, profile)
                .await
                .map_err(Into::into)
        })
        .await?;
    }
    if !has_service(profile, "access") {
        return Ok(());
    }
    S3Bootstrap::reconcile(session, profile, credentials, supervisor.monitor_log_mut()).await?;
    IcebergBootstrap::reconcile(session, profile, credentials, supervisor.monitor_log_mut()).await?;
    start_access(supervisor, profile, credentials).await
}

async fn start_access(
    supervisor: &mut Supervisor,
    profile: &DeploymentProfile,
    credentials: &ServerCredentials,
) -> Result<(), PreviewError> {
    let mut access_environment: BTreeMap<String, String> = credentials
        .server_env()
        .lines()
        .filter_map(|line| line.split_once('='))
        .filter(|(name, _)| name.starts_with("CROWDB_ICEBERG_"))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect();
    access_environment.insert("CROWDB_S3_MASTER_KEY".into(), credentials.s3_master_key().into());
    supervisor.start_service("access", access_environment).await?;
    if !has_service(profile, "web") {
        return Ok(());
    }
    let access = profile
        .services
        .iter()
        .find(|service| service.id == "access")
        .ok_or(PreviewError::Invalid("Access service is absent"))?;
    let mut web_environment = BTreeMap::from([(
        "CROWDB_ICEBERG_MANAGE_TOKEN".into(),
        credentials.iceberg_manage_token().into(),
    )]);
    let reader = credentials
        .server_env()
        .lines()
        .find_map(|line| line.strip_prefix("CROWDB_ICEBERG_READ_TOKEN=").map(str::to_owned))
        .ok_or(PreviewError::Invalid("Iceberg reader credential is absent"))?;
    web_environment.insert("CROWDB_ICEBERG_READ_TOKEN".into(), reader);
    for name in ["CROWDB_S3_PUBLIC_URI", "CROWDB_ICEBERG_PUBLIC_URI"] {
        let value = access
            .env
            .get(name)
            .ok_or(PreviewError::Invalid("Access public URI is absent"))?;
        web_environment.insert(name.to_owned(), value.clone());
    }
    web_environment.insert(
        "CROWDB_CONSOLE_CREDENTIAL_ROOT".into(),
        profile.paths.data_root.to_string_lossy().into_owned(),
    );
    supervisor.start_service("web", web_environment).await?;
    record_bootstrap_probe(
        supervisor,
        "web-authority",
        MonitorEventKind::BootstrapStepStarted,
    )
    .await?;
    let web_result = verify_web_authority(supervisor, profile).await;
    record_bootstrap_probe(
        supervisor,
        "web-authority",
        if web_result.is_ok() {
            MonitorEventKind::BootstrapStepCompleted
        } else {
            MonitorEventKind::BootstrapFailed
        },
    )
    .await?;
    web_result?;
    Ok(())
}
