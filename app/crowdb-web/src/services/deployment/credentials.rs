// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{path::Path, time::Duration};

use crowdb_console_shared::config::LocalLaunchSpec;
use crowdb_monitor::{ClientCredentials, ServerCredentials};

use crate::{error::err_502, services::Failure};

/// Provision before Access starts so its initial credential snapshot includes Console.
pub(super) async fn prepare(root: &Path, spec: &LocalLaunchSpec, seeds: &[String]) -> Result<(), Failure> {
    if root
        .join("secrets/client.env")
        .try_exists()
        .map_err(|_| err_502("Cannot inspect Console credentials"))?
    {
        crowdb_monitor::show_client_credentials(root)
            .map_err(|_| err_502("Existing Console credentials are invalid"))?;
        return Ok(());
    }
    let server =
        ServerCredentials::load_existing(root).map_err(|_| err_502("Cluster credentials are unavailable"))?;
    let output = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(&spec.program)
            .args(["s3", "ensure-user", "console-root"])
            .env("CROWDB_MANAGEMENT_SEEDS", seeds.join(","))
            .env("CROWDB_S3_MASTER_KEY", server.s3_master_key())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| err_502("Console S3 credential initialization timed out"))?
    .map_err(|_| err_502("Console S3 credential initialization could not start"))?;
    if !output.status.success() || output.stdout.len() > 4096 {
        return Err(err_502("Console S3 credential initialization failed"));
    }
    let output = std::str::from_utf8(&output.stdout)
        .map_err(|_| err_502("Console S3 credential output is invalid"))?;
    let get = |name: &str| -> Result<String, Failure> {
        let mut values = output.lines().filter_map(|line| line.strip_prefix(name));
        let value = values
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| err_502("Console S3 credential output is incomplete"))?;
        if values.next().is_some() {
            return Err(err_502("Console S3 credential output is ambiguous"));
        }
        Ok(value.to_owned())
    };
    server
        .persist_client(&ClientCredentials {
            s3_endpoint: spec.env["CROWDB_S3_PUBLIC_URI"].clone(),
            iceberg_endpoint: spec.env["CROWDB_ICEBERG_PUBLIC_URI"].clone(),
            region: "us-east-1".into(),
            access_key_id: get("AWS_ACCESS_KEY_ID=")?,
            secret_access_key: get("AWS_SECRET_ACCESS_KEY=")?,
        })
        .map_err(|_| err_502("Could not persist Console S3 credentials"))
}
