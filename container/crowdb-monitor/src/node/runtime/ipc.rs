// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::Result;
use crowdb_protocol::mgmt::node::NodeControl;
use serde_json::Value;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

pub(super) async fn read_request(stream: &mut UnixStream) -> Result<NodeControl> {
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), stream.take(65537).read_to_end(&mut bytes)).await??;
    if bytes.len() > 65536 {
        return Err("control request exceeds limit".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

/// Send one bounded request to the same-user monitor control socket.
///
/// # Errors
/// Propagates control rejection, transport failure and malformed replies.
pub async fn control_node(socket: &Path, request: &NodeControl) -> Result<Value> {
    let mut stream = UnixStream::connect(socket).await?;
    let bytes = serde_json::to_vec(request)?;
    if bytes.len() > 65536 {
        return Err("control request exceeds limit".into());
    }
    stream.write_all(&bytes).await?;
    stream.shutdown().await?;
    let mut bytes = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(45),
        stream.take(65537).read_to_end(&mut bytes),
    )
    .await??;
    if bytes.len() > 65536 {
        return Err("control response exceeds limit".into());
    }
    let reply: Value = serde_json::from_slice(&bytes)?;
    if reply["ok"] != true {
        return Err(reply["error"]
            .as_str()
            .unwrap_or("monitor rejected control")
            .to_owned()
            .into());
    }
    Ok(reply["value"].clone())
}
