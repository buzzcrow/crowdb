// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use serde_json::{json, Value};

use super::endpoint::normalize_endpoint;
use super::rpc::RpcGroup;
use super::ChunkKvRpcTransport;
use crate::{ClientError, Result};

impl ChunkKvRpcTransport {
    /// Owned class settings, native transport/error counters and pool counts.
    ///
    /// # Errors
    /// Returns a native snapshot or JSON encoding failure.
    pub fn transport_status_json(&self) -> Result<Vec<u8>> {
        let counters = self.rpc.counters();
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "metadata": group_status(&self.metadata)?,
            "aggregate": group_status(&self.aggregate)?,
            "rpc_counters": {
                "submit_fail": counters.submit_fail, "submit_retry": counters.submit_retry,
                "resp_missed": counters.resp_missed, "reaped": counters.reaped,
            },
        }))
        .map_err(|error| ClientError::Transport(error.to_string()))
    }

    /// Snapshot the first existing connection in each endpoint class group.
    /// This diagnostic never opens a connection or changes the pool cursor.
    ///
    /// # Errors
    /// Returns a native snapshot or JSON encoding failure.
    pub fn connection_status_json(&self, endpoint: &str) -> Result<Vec<u8>> {
        let endpoint = normalize_endpoint(endpoint);
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "metadata": connection_status(&self.metadata, &endpoint)?,
            "aggregate": connection_status(&self.aggregate, &endpoint)?,
        }))
        .map_err(|error| ClientError::Transport(error.to_string()))
    }
}

fn group_status(group: &RpcGroup) -> Result<Value> {
    let native = group
        .server
        .transport_status_json()
        .map_err(|error| ClientError::Transport(error.to_string()))?;
    let native: Value =
        serde_json::from_slice(&native).map_err(|error| ClientError::Transport(error.to_string()))?;
    Ok(
        json!({"data_capacity": group.data_capacity, "pool_size": group.connections.pool_size(),
        "endpoint_count": group.connections.len(), "connection_count": group.connections.connection_count(),
        "transport": native}),
    )
}

fn connection_status(group: &RpcGroup, endpoint: &str) -> Result<Value> {
    let Some(connection) = group.connections.get_first(endpoint) else {
        return Ok(Value::Null);
    };
    let native = connection
        .transport_status_json()
        .map_err(|error| ClientError::Transport(error.to_string()))?;
    let mut native: Value =
        serde_json::from_slice(&native).map_err(|error| ClientError::Transport(error.to_string()))?;
    native["pool_generation"] = json!(connection.generation());
    Ok(native)
}
