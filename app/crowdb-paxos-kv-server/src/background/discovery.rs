// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable connection hints for processes launched before Group 0 exists.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crowdb_protocol::mgmt::Group0DiscoveryRequest;
use tokio::io::AsyncWriteExt;

const FILE_NAME: &str = "group0-discovery.json";
static NEXT_WRITE: AtomicU64 = AtomicU64::new(0);

pub(crate) fn validate(request: &Group0DiscoveryRequest) -> io::Result<()> {
    if request.management_seeds.is_empty() || request.management_seeds.len() > 16 {
        return Err(io::Error::other(
            "one to sixteen Group 0 management seeds are required",
        ));
    }
    for seed in &request.management_seeds {
        let uri: axum::http::Uri = seed.parse().map_err(io::Error::other)?;
        if uri.scheme_str() != Some("http")
            || uri.host().map_or(true, str::is_empty)
            || uri
                .authority()
                .map_or(true, |authority| authority.as_str().contains('@'))
            || uri.path() != "/"
            || uri.query().is_some()
        {
            return Err(io::Error::other(
                "management seed must be an unauthenticated HTTP origin",
            ));
        }
    }
    Ok(())
}

pub(crate) async fn load(root: &Path) -> io::Result<Option<Vec<String>>> {
    let body = match tokio::fs::read(root.join(FILE_NAME)).await {
        Ok(body) => body,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let request: Group0DiscoveryRequest = serde_json::from_slice(&body)?;
    validate(&request)?;
    Ok(Some(request.management_seeds))
}

pub(crate) async fn save(root: &Path, request: &Group0DiscoveryRequest) -> io::Result<()> {
    validate(request)?;
    tokio::fs::create_dir_all(root).await?;
    let temporary = root.join(format!(
        ".group0-discovery-{}-{}.tmp",
        std::process::id(),
        NEXT_WRITE.fetch_add(1, Ordering::Relaxed)
    ));
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .await?;
        file.write_all(&serde_json::to_vec(request)?).await?;
        file.sync_all().await?;
        tokio::fs::rename(&temporary, root.join(FILE_NAME)).await?;
        tokio::fs::File::open(root).await?.sync_all().await
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}
