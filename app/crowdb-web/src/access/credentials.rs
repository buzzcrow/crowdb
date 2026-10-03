// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;

use crate::state::AppState;

fn root(state: &AppState) -> PathBuf {
    std::env::var_os("CROWDB_CONSOLE_CREDENTIAL_ROOT")
        .map_or_else(|| state.runtime_root.as_ref().clone(), PathBuf::from)
}

fn value(body: &str, name: &str) -> Option<String> {
    body.lines().find_map(|line| {
        let (key, value) = line.split_once('=')?;
        (key == name).then(|| value.to_owned())
    })
}

pub(super) fn writer(state: &AppState) -> Result<String, String> {
    if let Ok(token) = std::env::var("CROWDB_ICEBERG_WRITE_TOKEN") {
        return Ok(token);
    }
    let credentials = crowdb_monitor::ServerCredentials::load_existing(&root(state))
        .map_err(|_| "Cluster Catalog writer is not configured on the Console server")?;
    value(&credentials.server_env(), "CROWDB_ICEBERG_WRITE_TOKEN")
        .ok_or_else(|| "Cluster Catalog writer is missing".into())
}

pub(super) struct S3Credentials {
    pub access: String,
    pub secret: String,
    pub region: String,
    pub session: Option<String>,
}

pub(super) fn s3(state: &AppState, target: &str) -> Result<S3Credentials, String> {
    if let (Ok(access), Ok(secret)) = (
        std::env::var("AWS_ACCESS_KEY_ID"),
        std::env::var("AWS_SECRET_ACCESS_KEY"),
    ) {
        return Ok(S3Credentials {
            access,
            secret,
            region: std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".into()),
            session: std::env::var("AWS_SESSION_TOKEN").ok(),
        });
    }
    let body = crowdb_monitor::show_client_credentials(&root(state))
        .map_err(|_| "Cluster S3 credentials are not configured on the Console server")?;
    let get = |name| value(&body, name).ok_or_else(|| "Cluster S3 credentials are incomplete".to_owned());
    if get("AWS_ENDPOINT_URL")?.trim_end_matches('/') != target
        && crate::services::access::origin(state, "s3").as_deref() != Some(target)
    {
        return Err("Cluster S3 credential endpoint does not match the configured service".into());
    }
    Ok(S3Credentials {
        access: get("AWS_ACCESS_KEY_ID")?,
        secret: get("AWS_SECRET_ACCESS_KEY")?,
        region: get("AWS_DEFAULT_REGION")?,
        session: None,
    })
}

pub(super) fn manager(state: &AppState) -> Result<String, String> {
    if let Ok(token) = std::env::var("CROWDB_ICEBERG_MANAGE_TOKEN") {
        return Ok(token);
    }
    let credentials = crowdb_monitor::ServerCredentials::load_existing(&root(state))
        .map_err(|_| "Cluster management credentials are not configured on the Console server")?;
    value(&credentials.server_env(), "CROWDB_ICEBERG_MANAGE_TOKEN")
        .ok_or_else(|| "Cluster management credential is missing".into())
}
