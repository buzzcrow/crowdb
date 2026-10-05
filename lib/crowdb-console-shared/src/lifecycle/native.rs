// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{
    collections::BTreeMap,
    io::Write,
    path::{Path, PathBuf},
};

use crate::{
    config::{LocalLaunchSpec, ServiceType},
    error::{Error, Result},
};

/// Stages a Chunk-KV or Access executable and writes its fresh service configuration.
/// Credentials are referenced by path and never serialized into the registry.
///
/// # Errors
/// Rejects unsupported kinds, unavailable binaries, existing config files and I/O failures.
pub fn prepare_native_launch(
    kind: ServiceType,
    workspace: &Path,
    config: &serde_json::Value,
    env_file: Option<PathBuf>,
    readiness_url: String,
) -> Result<LocalLaunchSpec> {
    let (variable, name) = match kind {
        ServiceType::ChunkKv => ("CROWDB_CHUNK_KV_SERVER_BIN", "crowdb-chunk-kv-server"),
        ServiceType::AccessServer => ("CROWDB_ACCESS_SERVER_BIN", "crowdb-access-server"),
        _ => return Err(Error::Config("Unsupported native service kind".into())),
    };
    let binary = binary(variable, name)?;
    let program = super::stage_server_binary(&binary, workspace)?;
    let config_path = workspace.join("service.toml");
    let value = toml::Value::try_from(config).map_err(|error| Error::Config(error.to_string()))?;
    let body = toml::to_string_pretty(&value).map_err(|error| Error::Config(error.to_string()))?;
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&config_path)?;
    file.write_all(body.as_bytes())?;
    file.sync_all()?;
    let log_dir = workspace.join("log");
    std::fs::create_dir_all(&log_dir)?;
    let mut args = vec!["--config".into(), config_path.to_string_lossy().into_owned()];
    let mut env = BTreeMap::new();
    if kind == ServiceType::ChunkKv {
        args.extend([
            "--log-dir".into(),
            log_dir.to_string_lossy().into_owned(),
            "--log".into(),
        ]);
    } else {
        env.insert(
            "CROWDB_ACCESS_LOG_DIR".into(),
            log_dir.to_string_lossy().into_owned(),
        );
    }
    Ok(LocalLaunchSpec {
        program: program.to_string_lossy().into_owned(),
        args,
        workdir: workspace.to_string_lossy().into_owned(),
        env,
        env_file: env_file.map(|path| path.to_string_lossy().into_owned()),
        readiness_url: Some(readiness_url),
    })
}

fn binary(variable: &str, name: &str) -> Result<PathBuf> {
    if let Some(value) = std::env::var_os(variable) {
        return Ok(PathBuf::from(value));
    }
    if let Ok(executable) = std::env::current_exe() {
        if let Some(directory) = executable.parent() {
            for parent in directory.ancestors().take(3) {
                let candidate = parent.join(name);
                if candidate.is_file() {
                    return Ok(candidate);
                }
            }
        }
    }
    super::find_in_path(std::ffi::OsStr::new(name)).ok_or_else(|| Error::NotFound {
        kind: "binary".into(),
        id: format!("{name} (set {variable})"),
    })
}
