// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Status exchanges run outside traffic windows on an explicit TCP connection.

use std::time::Duration;

use crowdb_rpc_ffi::{Buffer, RdmaConfig, RpcClient, RpcServer};
use serde_json::Value;

use super::{workload::build_control, RpcArgs};

pub(super) async fn remote_status(args: &RpcArgs) -> Result<Value, String> {
    let server = RpcServer::with_engines(None, 1, 1);
    server
        .configure_rdma(&RdmaConfig {
            allow_tcp_fallback: false,
            ..RdmaConfig::default()
        })
        .map_err(|e| format!("TCP control configuration: {e}"))?;
    server
        .listen("127.0.0.1", 0)
        .map_err(|e| format!("TCP control listen: {e}"))?;
    server.start();
    let client = RpcClient::new();
    client.set_completion_pool_size(1024);
    client.start_reaper(2_000_000_000, 10_000_000);
    let result = query(args, &server, &client).await;
    client.stop_reaper();
    server.stop();
    result
}

async fn query(args: &RpcArgs, server: &RpcServer, client: &RpcClient) -> Result<Value, String> {
    let conn = server
        .connect(&args.server_host, i32::from(args.server_port))
        .map_err(|e| format!("TCP control connect: {e}"))?;
    client.attach(&conn);
    let future = client
        .call(server, &conn, 1, Buffer::from_vec(build_control(1)), None, 101)
        .map_err(|e| format!("status submit: {e}"))?;
    let response = tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .map_err(|e| format!("status deadline: {e}"))?
        .map_err(|e| format!("status response: {e}"))?;
    let data = response.data.ok_or("status response has no data")?;
    let status: Value = serde_json::from_slice(data.as_slice()).map_err(|e| format!("status JSON: {e}"))?;
    if status["schema_version"] != 1 {
        return Err("unsupported server status schema".into());
    }
    Ok(status)
}

pub(super) fn local_status(server: &RpcServer) -> Result<Value, String> {
    let bytes = server
        .transport_status_json()
        .map_err(|e| format!("local status: {e}"))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("local status JSON: {e}"))
}
