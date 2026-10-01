use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::Path;
use std::time::Duration;

use thiserror::Error;
use tokio::time::{sleep, Instant};

use crate::{
    disk_step_names, ensure_disk_files, hardware_step_names, iceberg_step_names, kv_step_names,
    logical_step_names, render_configs, s3_step_names, verify_chunk_services, verify_diskio_disks,
    BootstrapSession, ChunkBootstrapError, CrashRetention, CredentialError, DeploymentProfile,
    DiskBootstrapError, HardwareBootstrap, HardwareBootstrapError, IcebergBootstrap, IcebergBootstrapError,
    KvBootstrap, KvBootstrapError, LivenessError, LivenessServer, LogicalBootstrap, LogicalBootstrapError,
    ManifestError, ManifestState, MonitorEvent, MonitorEventKind, MonitorLogError, ProfileError, RenderError,
    S3Bootstrap, S3BootstrapError, ServerCredentials, StorageProbeError, Supervisor, SupervisorError,
};

const PROFILE_NAME: &str = "single-node-container";
const MAX_TEMPLATE_BYTES: u64 = 1024 * 1024;

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

/// # Errors
/// Fails closed on incompatible durable state or any unready child.
pub async fn run_preview(profile_path: &Path) -> Result<(), PreviewError> {
    let profile_bytes = fs::read(profile_path)?;
    let profile = DeploymentProfile::parse(
        std::str::from_utf8(&profile_bytes).map_err(|_| PreviewError::Invalid("profile is not UTF-8"))?,
    )?;
    if profile.name != PROFILE_NAME {
        return Err(PreviewError::Invalid(
            "run supports only the named preview profile",
        ));
    }
    let config_bytes = config_digest_input(&profile)?;
    let steps = step_names(&profile)?;
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
    let management_seed = management_seed(&profile)?;
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

async fn run_ready_services(
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
    require_directory(&kv_root(profile)?)?;
    KvBootstrap::new(management_seed)?
        .reconcile(&mut session, profile, supervisor.monitor_log_mut())
        .await?;
    ensure_disk_files(&mut session, profile, supervisor.monitor_log_mut()).await?;
    HardwareBootstrap::new(management_seed.to_owned())
        .reconcile(&mut session, profile, supervisor.monitor_log_mut())
        .await?;
    LogicalBootstrap::new(management_seed.to_owned())
        .reconcile(&mut session, profile, supervisor.monitor_log_mut())
        .await?;
    verify_diskio_disks(management_seed, profile).await?;
    verify_chunk_services(management_seed, profile).await?;
    S3Bootstrap::reconcile(&mut session, profile, credentials, supervisor.monitor_log_mut()).await?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match IcebergBootstrap::reconcile(&mut session, profile, credentials, supervisor.monitor_log_mut())
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
    verify_web_authority(supervisor, profile).await
}

async fn bootstrap_services(
    supervisor: &mut Supervisor,
    session: &mut BootstrapSession,
    profile: &DeploymentProfile,
    credentials: &ServerCredentials,
    management_seed: &str,
) -> Result<(), PreviewError> {
    supervisor.start_service("kv", BTreeMap::new()).await?;
    KvBootstrap::new(management_seed)?
        .reconcile(session, profile, supervisor.monitor_log_mut())
        .await?;
    ensure_disk_files(session, profile, supervisor.monitor_log_mut()).await?;
    HardwareBootstrap::new(management_seed.to_owned())
        .reconcile(session, profile, supervisor.monitor_log_mut())
        .await?;
    LogicalBootstrap::new(management_seed.to_owned())
        .reconcile(session, profile, supervisor.monitor_log_mut())
        .await?;
    supervisor.start_service("diskdb", BTreeMap::new()).await?;
    supervisor.start_service("diskio", BTreeMap::new()).await?;
    verify_bootstrap_probe(supervisor, "diskio-authority", async {
        verify_diskio_disks(management_seed, profile)
            .await
            .map_err(Into::into)
    })
    .await?;
    supervisor.start_service("chunkdb", BTreeMap::new()).await?;
    supervisor.start_service("chunk-kv", BTreeMap::new()).await?;
    verify_bootstrap_probe(supervisor, "chunk-authority", async {
        verify_chunk_services(management_seed, profile)
            .await
            .map_err(Into::into)
    })
    .await?;
    S3Bootstrap::reconcile(session, profile, credentials, supervisor.monitor_log_mut()).await?;
    IcebergBootstrap::reconcile(session, profile, credentials, supervisor.monitor_log_mut()).await?;
    let mut access_environment: BTreeMap<String, String> = credentials
        .server_env()
        .lines()
        .filter_map(|line| line.split_once('='))
        .filter(|(name, _)| name.starts_with("CROWDB_ICEBERG_"))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect();
    access_environment.insert("CROWDB_S3_MASTER_KEY".into(), credentials.s3_master_key().into());
    supervisor.start_service("access", access_environment).await?;
    supervisor
        .start_service(
            "web",
            BTreeMap::from([(
                "CROWDB_ICEBERG_MANAGE_TOKEN".into(),
                credentials.iceberg_manage_token().into(),
            )]),
        )
        .await?;
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

async fn verify_bootstrap_probe(
    supervisor: &mut Supervisor,
    name: &'static str,
    probe: impl Future<Output = Result<(), PreviewError>>,
) -> Result<(), PreviewError> {
    record_bootstrap_probe(supervisor, name, MonitorEventKind::BootstrapStepStarted).await?;
    let result = probe.await;
    let kind = if result.is_ok() {
        MonitorEventKind::BootstrapStepCompleted
    } else {
        MonitorEventKind::BootstrapFailed
    };
    record_bootstrap_probe(supervisor, name, kind).await?;
    result
}

async fn record_bootstrap_probe(
    supervisor: &mut Supervisor,
    name: &'static str,
    kind: MonitorEventKind,
) -> Result<(), PreviewError> {
    supervisor
        .monitor_log_mut()
        .record(&MonitorEvent {
            kind,
            service: Some(name),
            pid: None,
            attempt: None,
        })
        .await?;
    Ok(())
}

async fn verify_web_authority(
    supervisor: &mut Supervisor,
    profile: &DeploymentProfile,
) -> Result<(), PreviewError> {
    let web = profile
        .services
        .iter()
        .find(|service| service.id == "web")
        .ok_or(PreviewError::Invalid("Web service is absent"))?;
    let origin = web
        .probe
        .target
        .strip_suffix("/healthz")
        .ok_or(PreviewError::Invalid("Web health endpoint is incompatible"))?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| PreviewError::WebAuthority("cannot construct authority probe"))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_status_refresh = Instant::now();
    loop {
        if last_status_refresh.elapsed() >= Duration::from_secs(5) {
            supervisor.refresh_status()?;
            last_status_refresh = Instant::now();
        }
        let response = client
            .get(format!("{origin}/api/authority"))
            .send()
            .await
            .map_err(|_| PreviewError::WebAuthority("authority endpoint is unavailable"))?;
        let status = response.status();
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|_| PreviewError::WebAuthority("authority response is invalid"))?;
        if status.is_success() {
            if body.get("source").and_then(serde_json::Value::as_str) == Some("group0")
                && body.get("available").and_then(serde_json::Value::as_bool) == Some(true)
            {
                return Ok(());
            }
            return Err(PreviewError::WebAuthority("Web is not serving Group 0 authority"));
        }
        let reason = body
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unspecified");
        if reason != "group0_unavailable" || Instant::now() >= deadline {
            return Err(PreviewError::WebAuthorityUnavailable(format!(
                "HTTP {status}: {reason}"
            )));
        }
        sleep(Duration::from_millis(200)).await;
    }
}

fn step_names(profile: &DeploymentProfile) -> Result<Vec<String>, PreviewError> {
    let steps = kv_step_names(profile)?
        .into_iter()
        .chain(disk_step_names(profile))
        .chain(hardware_step_names())
        .chain(logical_step_names().map(str::to_owned))
        .chain(s3_step_names().map(str::to_owned))
        .chain(iceberg_step_names().map(str::to_owned))
        .collect();
    Ok(steps)
}

fn management_seed(profile: &DeploymentProfile) -> Result<String, PreviewError> {
    let service = profile
        .services
        .iter()
        .find(|service| service.id == "access")
        .ok_or(PreviewError::Invalid("Access service is absent"))?;
    let seeds = service
        .env
        .get("CROWDB_MANAGEMENT_SEEDS")
        .ok_or(PreviewError::Invalid("management seeds are absent"))?;
    let parts = seeds.split(',').collect::<Vec<_>>();
    let [seed] = parts.as_slice() else {
        return Err(PreviewError::Invalid("preview requires one management seed"));
    };
    if seed.is_empty() {
        return Err(PreviewError::Invalid("management seed is empty"));
    }
    Ok((*seed).to_owned())
}

fn kv_root(profile: &DeploymentProfile) -> Result<std::path::PathBuf, PreviewError> {
    let service = profile
        .services
        .iter()
        .find(|service| service.id == "kv")
        .ok_or(PreviewError::Invalid("KV service is absent"))?;
    let mut roots = service.args.windows(2).filter(|pair| pair[0] == "--root");
    let path = roots
        .next()
        .ok_or(PreviewError::Invalid("KV root argument is absent"))?[1]
        .as_str();
    if roots.next().is_some() {
        return Err(PreviewError::Invalid("KV root argument is duplicated"));
    }
    let path = Path::new(path);
    if !path.starts_with(profile.paths.data_root.join("kv"))
        || path == profile.paths.data_root.join("kv")
        || !crate::layout::is_clean_absolute(path)
    {
        return Err(PreviewError::Invalid(
            "KV root is outside the durable KV directory",
        ));
    }
    Ok(path.to_owned())
}

fn config_digest_input(profile: &DeploymentProfile) -> Result<Vec<u8>, PreviewError> {
    let mut files = BTreeMap::new();
    for service in &profile.services {
        if let Some(path) = &service.config_template {
            let name = path
                .file_name()
                .ok_or(PreviewError::Invalid("template has no name"))?;
            if files
                .insert(name.to_os_string(), path)
                .is_some_and(|existing| existing != path)
            {
                return Err(PreviewError::Invalid("template name is duplicated"));
            }
        }
    }
    let mut input = Vec::new();
    for name in files.keys() {
        let path = profile.paths.template_root.join(name);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_file() || metadata.len() > MAX_TEMPLATE_BYTES {
            return Err(PreviewError::Invalid("template is not a bounded regular file"));
        }
        let body = fs::read(&path)?;
        for field in [name.as_encoded_bytes(), body.as_slice()] {
            input.extend_from_slice(&(field.len() as u64).to_be_bytes());
            input.extend_from_slice(field);
        }
    }
    Ok(input)
}

fn ensure_directory(path: &Path) -> Result<(), PreviewError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_dir() => {
            Err(PreviewError::Invalid("runtime path is not a directory"))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path)?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn require_directory(path: &Path) -> Result<(), PreviewError> {
    if !fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(PreviewError::Invalid("required durable directory is missing"));
    }
    Ok(())
}
