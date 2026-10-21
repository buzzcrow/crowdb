// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{read_private, CredentialError, CLIENT_FILE};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};

pub struct ClientCredentials {
    pub s3_endpoint: String,
    pub iceberg_endpoint: String,
    pub dataset_endpoint: Option<String>,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl ClientCredentials {
    pub(super) fn validate(&self) -> Result<(), CredentialError> {
        crowdb_protocol::mgmt::node::deployment::PublicAccessUris {
            s3: Some(self.s3_endpoint.clone()),
            iceberg: Some(self.iceberg_endpoint.clone()),
            dataset: self.dataset_endpoint.clone(),
        }
        .validate()
        .map_err(CredentialError::Invalid)?;
        if ![&self.region, &self.access_key_id, &self.secret_access_key]
            .iter()
            .all(|value| {
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-._~:/".contains(&byte))
            })
        {
            return Err(CredentialError::Invalid("client credential value is invalid"));
        }
        Ok(())
    }

    pub(super) fn env(&self, writer_token: &str, dataset_token: &str) -> String {
        let mut body = format!(
            "AWS_ENDPOINT_URL={}\nAWS_DEFAULT_REGION={}\nAWS_ACCESS_KEY_ID={}\nAWS_SECRET_ACCESS_KEY={}\nICEBERG_URI={}\nICEBERG_TOKEN={}\n",
            self.s3_endpoint,
            self.region,
            self.access_key_id,
            self.secret_access_key,
            self.iceberg_endpoint,
            writer_token,
        );
        if let Some(endpoint) = &self.dataset_endpoint {
            use std::fmt::Write as _;
            writeln!(body, "DATASET_URI={endpoint}\nDATASET_TOKEN={dataset_token}").expect("String write");
        }
        body
    }
}

/// # Errors
/// Rejects missing, symlinked, malformed, or publicly readable client files.
pub fn show_client_credentials(data_root: &Path) -> Result<String, CredentialError> {
    let directory = data_root.join("secrets");
    let metadata = fs::symlink_metadata(&directory)?;
    if !metadata.file_type().is_dir() || metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(CredentialError::Invalid("secrets directory must have mode 0700"));
    }
    let body = read_private(&directory.join(CLIENT_FILE))?;
    validate_client_env(&body)?;
    Ok(body)
}

fn validate_client_env(body: &str) -> Result<(), CredentialError> {
    let mut values = body.lines();
    let endpoint = take_value(&mut values, "AWS_ENDPOINT_URL")?;
    let region = take_value(&mut values, "AWS_DEFAULT_REGION")?;
    let access_key = take_value(&mut values, "AWS_ACCESS_KEY_ID")?;
    let secret_key = take_value(&mut values, "AWS_SECRET_ACCESS_KEY")?;
    let iceberg_uri = take_value(&mut values, "ICEBERG_URI")?;
    let writer_token = take_value(&mut values, "ICEBERG_TOKEN")?;
    let dataset_endpoint = if let Some(line) = values.next() {
        let endpoint = line
            .strip_prefix("DATASET_URI=")
            .ok_or(CredentialError::Invalid("Dataset URI is missing"))?;
        let token = take_value(&mut values, "DATASET_TOKEN")?;
        validate_token(token)?;
        Some(endpoint.to_owned())
    } else {
        None
    };
    if values.next().is_some() {
        return Err(CredentialError::Invalid(
            "client credentials contain extra values",
        ));
    }
    ClientCredentials {
        s3_endpoint: endpoint.into(),
        iceberg_endpoint: iceberg_uri.into(),
        dataset_endpoint,
        region: region.into(),
        access_key_id: access_key.into(),
        secret_access_key: secret_key.into(),
    }
    .validate()?;
    validate_token(writer_token)
}

fn validate_token(token: &str) -> Result<(), CredentialError> {
    if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CredentialError::Invalid("client bearer token is invalid"));
    }
    Ok(())
}

fn take_value<'a>(lines: &mut impl Iterator<Item = &'a str>, key: &str) -> Result<&'a str, CredentialError> {
    lines
        .next()
        .and_then(|line| line.strip_prefix(key))
        .and_then(|line| line.strip_prefix('='))
        .ok_or(CredentialError::Invalid("credential file is incomplete"))
}
