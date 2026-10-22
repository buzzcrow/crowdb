// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Session ownership, admission and coroutine/Tokio scheduling.

use std::sync::Arc;
use std::time::Instant;

use crowdb_protocol::fb::{ConnectionPingResponse, ConnectionPingResponseArgs};
use crowdb_rpc_ffi::co_bench::{co_spawn, CoBenchHandler, CoStats};
use crowdb_rpc_ffi::{Connection, RdmaConfig, RpcClient, RpcServer};
use flatbuffers::FlatBufferBuilder;
use serde_json::Value;

use crate::args::BenchMode;
use super::workload::{build_control, Count, EchoWorkload};
use super::RpcArgs;

pub(super) struct Session {
    pub server: Arc<RpcServer>,
    pub client: Arc<RpcClient>,
    pub conns: Arc<Vec<Connection>>,
}

impl Session {
    pub fn new(args: &RpcArgs, config: &RdmaConfig) -> Result<Self, String> {
        let server = Arc::new(RpcServer::with_engines(None, args.io_engines, args.io_workers));
        server
            .configure_rdma(config)
            .map_err(|e| format!("workload configuration: {e}"))?;
        server.set_tcp_nodelay(!args.enable_nagle);
        server.set_send_queue_capacity(args.queues.send_queue_capacity);
        server
            .listen("127.0.0.1", 0)
            .map_err(|e| format!("workload listen: {e}"))?;
        server.start();
        let mut conns = Vec::new();
        for _ in 0..args.connection_count {
            conns.push(
                server
                    .connect(&args.server_host, i32::from(args.server_port))
                    .map_err(|e| format!("workload connect: {e}"))?,
            );
        }
        let client = Arc::new(RpcClient::new());
        client.set_completion_pool_size(args.loader_count.next_power_of_two());
        for conn in &conns {
            client.attach(conn);
        }
        client.start_reaper(args.request_timeout_secs * 1_000_000_000, 10_000_000);
        Ok(Self {
            server,
            client,
            conns: Arc::new(conns),
        })
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.client.stop_reaper();
        self.server.stop();
    }
}

pub(super) fn validate_wire_bound(args: &RpcArgs, connections: &Value, rdma: bool) -> Result<(), String> {
    if !rdma {
        return Ok(());
    }
    let mut builder = FlatBufferBuilder::new();
    let response = ConnectionPingResponse::create(
        &mut builder,
        &ConnectionPingResponseArgs {
            id: u64::MAX,
            rpc_create_nano: u64::MAX,
            response_create_nano: u64::MAX,
            ..ConnectionPingResponseArgs::default()
        },
    );
    builder.finish(response, None);
    let control_bytes = builder.finished_data().len().max(build_control(u64::MAX).len());
    let frame_bytes = 14 + control_bytes + args.payload_bytes;
    for connection in connections.as_array().ok_or("missing connection statuses")? {
        let peer = connection["info"]["peer_receive_slot_capacity"]
            .as_u64()
            .ok_or("missing peer receive admission")?;
        let bound = peer
            .min(u64::from(args.queues.wire_slot_capacity))
            .min(u64::from(args.queues.aggregation_max_bytes));
        if frame_bytes as u64 > bound {
            return Err(format!(
                "frame_bytes={frame_bytes} exceeds negotiated wire bound={bound} (header/control included)"
            ));
        }
    }
    Ok(())
}

pub(super) async fn drive(
    args: &RpcArgs,
    session: &Session,
    handler: Arc<EchoWorkload>,
) -> Result<CoStats, String> {
    let client = session.client.clone();
    let server = session.server.clone();
    let conns = session.conns.clone();
    let loaders = args.loader_count;
    if args.mode == BenchMode::Coroutine {
        tokio::task::spawn_blocking(move || co_spawn(&client, &server, &conns, loaders, 100, handler))
            .await
            .map_err(|e| format!("coroutine join: {e}"))
    } else {
        drive_tokio(session, loaders, handler).await
    }
}

async fn drive_tokio(session: &Session, loaders: u32, handler: Arc<EchoWorkload>) -> Result<CoStats, String> {
    let mut tasks = Vec::new();
    for loader in 0..loaders {
        let client = session.client.clone();
        let server = session.server.clone();
        let conns = session.conns.clone();
        let workload = handler.clone();
        tasks.push(tokio::spawn(async move {
            let mut id = u64::from(loader);
            let mut rejects = 0;
            while let Some((control, data)) = workload.build_request(id) {
                let started = Instant::now();
                if let Ok(future) = client.call(
                    &server,
                    &conns[loader as usize % conns.len()],
                    id,
                    control,
                    Some(data),
                    100,
                ) {
                    let response = future.await;
                    let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    match response {
                        Ok(reply) => {
                            workload.on_response_data(
                                id,
                                Ok(()),
                                nanos,
                                reply.control.as_ref().map_or(&[], |b| b.as_slice()),
                                reply.data.as_ref().map_or(&[], |b| b.as_slice()),
                            );
                        }
                        Err(error) => {
                            workload.on_response(id, Err(error), nanos);
                        }
                    }
                } else {
                    rejects += 1;
                    tokio::task::yield_now().await;
                }
                id += u64::from(loaders.next_power_of_two());
            }
            rejects
        }));
    }
    let mut rejects = 0;
    for task in tasks {
        rejects += task.await.map_err(|e| format!("Tokio loader join: {e}"))?;
    }
    let counts = handler.totals();
    Ok(CoStats {
        total_ops: counts[Count::Responses as usize],
        total_errors: rejects + counts[Count::ResponseErrors as usize],
        ..CoStats::default()
    })
}
