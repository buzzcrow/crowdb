// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resolve selected-cluster credentials before any S3 workload request.

use crate::Cli;
use crowdb_console_shared::{
    error::{Error, Result},
    ops::s3::{S3Credentials, S3HttpClient},
};
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize)]
struct AccessCredentials {
    s3: ClientCredentials,
}

#[derive(Deserialize)]
struct ClientCredentials {
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    region: String,
}

pub(crate) async fn client(cli: &Cli) -> Result<S3HttpClient> {
    let target = cli
        .target
        .as_ref()
        .ok_or_else(|| invalid("select --cluster or --management-endpoint before an S3 operation"))?;
    let origin = target
        .management_endpoints
        .first()
        .ok_or_else(|| invalid("selected cluster has no management endpoint"))?;
    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(3))
        .build()
        .map_err(|_| invalid("cannot construct credential client"))?;
    let credentials = tokio::time::timeout(Duration::from_secs(3), async {
        let mut reply = http
            .get(format!("{origin}/api/access/credentials"))
            .send()
            .await
            .map_err(|_| invalid("selected cluster credentials are unavailable"))?
            .error_for_status()
            .map_err(|_| invalid("selected cluster rejected credential access"))?;
        let mut bytes = Vec::new();
        while let Some(chunk) = reply
            .chunk()
            .await
            .map_err(|_| invalid("cannot read selected cluster credentials"))?
        {
            if bytes.len() + chunk.len() > 32768 {
                return Err(invalid("selected credentials exceed their size bound"));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice::<AccessCredentials>(&bytes)
            .map_err(|_| invalid("selected cluster S3 endpoint or credentials are incomplete"))
    })
    .await
    .map_err(|_| invalid("selected cluster credential observation timed out"))??
    .s3;
    S3HttpClient::signed(
        credentials.endpoint,
        S3Credentials {
            access: credentials.access_key_id,
            secret: credentials.secret_access_key,
            region: credentials.region,
            session: None,
        },
    )
}

fn invalid(message: &str) -> Error {
    Error::Config(message.into())
}
