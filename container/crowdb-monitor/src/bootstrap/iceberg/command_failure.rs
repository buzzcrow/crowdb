// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::IcebergBootstrapError;
use crate::ServerCredentials;
use std::process::Output;

pub(super) fn describe(
    arguments: &[&str],
    output: &Output,
    credentials: &ServerCredentials,
) -> IcebergBootstrapError {
    let mut diagnostic = String::from_utf8_lossy(&output.stderr).into_owned();
    // Redact before truncation so a boundary cannot expose part of a credential.
    for line in credentials.server_env().lines() {
        if let Some((_, value)) = line.split_once('=') {
            if !value.is_empty() {
                diagnostic = diagnostic.replace(value, "<redacted>");
            }
        }
    }
    let diagnostic: String = diagnostic
        .chars()
        .take(1024)
        .map(|character| if character.is_control() { ' ' } else { character })
        .collect();
    IcebergBootstrapError::CommandExit {
        action: arguments.first().copied().unwrap_or("unknown").to_owned(),
        status: output.status.to_string(),
        diagnostic: if diagnostic.trim().is_empty() {
            "no stderr was produced".into()
        } else {
            diagnostic
        },
    }
}
