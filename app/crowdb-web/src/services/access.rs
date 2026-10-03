// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::state::AppState;
use crowdb_console_shared::config::ServiceType;

pub(crate) fn origin(state: &AppState, protocol: &str) -> Option<String> {
    let name = match protocol {
        "s3" => "CROWDB_S3_PUBLIC_URI",
        "iceberg" => "CROWDB_ICEBERG_PUBLIC_URI",
        _ => return None,
    };
    let config = state.config.read().unwrap();
    config
        .servers
        .iter()
        .filter(|service| service.service_type == ServiceType::AccessServer)
        .find_map(|service| config.local_launches.get(&service.id)?.env.get(name).cloned())
}

pub(crate) fn reader(state: &AppState, target: Option<&str>) -> Result<Option<String>, String> {
    if let Some(token) = state.iceberg_read_token.as_deref() {
        return Ok(Some(token.to_owned()));
    }
    if target.is_none() || origin(state, "iceberg").as_deref() != target {
        return Ok(None);
    }
    let credentials = crowdb_monitor::ServerCredentials::load_existing(state.runtime_root.as_ref())
        .map_err(|error| error.to_string())?;
    Ok(credentials
        .server_env()
        .lines()
        .find_map(|line| line.strip_prefix("CROWDB_ICEBERG_READ_TOKEN=").map(str::to_owned)))
}
