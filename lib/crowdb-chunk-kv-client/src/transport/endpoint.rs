// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{ClientError, Result};

pub(super) fn normalize_endpoint(endpoint: &str) -> String {
    let endpoint = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    endpoint.replacen("0.0.0.0:", "127.0.0.1:", 1)
}

pub(super) fn parse_endpoint(endpoint: &str) -> Result<(String, i32)> {
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| ClientError::Transport(format!("invalid owner endpoint: {endpoint}")))?;
    let port = port
        .parse::<i32>()
        .map_err(|_| ClientError::Transport(format!("invalid owner endpoint port: {endpoint}")))?;
    if host.is_empty() || !(1..=u16::MAX.into()).contains(&port) {
        return Err(ClientError::Transport(format!(
            "invalid owner endpoint: {endpoint}"
        )));
    }
    Ok((host.into(), port))
}
