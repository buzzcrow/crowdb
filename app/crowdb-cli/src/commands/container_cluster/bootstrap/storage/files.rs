// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resume monitor-owned sparse file admission before starting `DiskIO`.

use super::{json, Api, Duration, Error, Instant, Result, State};

pub(super) async fn prepare(api: &Api, state: &State, node: usize) -> Result<()> {
    // Derive the operation from the durable cluster identity so an interrupted
    // start resubmits the same intent rather than creating another medium.
    let suffix = u64::from_str_radix(&state.id[20..], 16)
        .map_err(|error| Error::Config(error.to_string()))?
        ^ u64::try_from(node).map_err(|error| Error::Config(error.to_string()))?;
    let operation = format!(
        "{}-{}-{}-{}-{suffix:012x}",
        &state.id[..8],
        &state.id[8..12],
        &state.id[12..16],
        &state.id[16..20],
    );
    let path = format!("/api/nodes/{node}/disk-files");
    let mut observed = api
        .post(
            &path,
            json!({
                "operation_id": operation,
                "disk_group_id": node,
                "capacity_bytes": 16u64 * 1024 * 1024 * 1024,
                "sparse": true,
            }),
        )
        .await?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match observed["phase"].as_str() {
            Some("waiting-for-diskio" | "registered") => return Ok(()),
            Some("failed") => {
                return Err(Error::Config(format!(
                    "container file disk admission failed: {observed}"
                )));
            }
            _ => {}
        }
        if Instant::now() >= deadline {
            return Err(Error::Config(format!(
                "container file disk admission is not published: {observed}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        observed = api.get(&format!("{path}/{operation}")).await?;
    }
}
