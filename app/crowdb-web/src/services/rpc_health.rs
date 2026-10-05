// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crowdb_protocol::fb::{
    ConnectionPingRequest, ConnectionPingRequestArgs, ConnectionPingResponse, FBMsgType,
};
use crowdb_rpc_ffi::{Buffer, ConnectionPoolIndex, RpcClient, RpcServer};

pub(crate) struct RpcHealth {
    client: RpcClient,
    connections: ConnectionPoolIndex,
    server: RpcServer,
    next_id: AtomicU64,
}

impl RpcHealth {
    pub(crate) fn new() -> Self {
        let server = RpcServer::with_engines(None, 1, 1);
        server.start();
        let client = RpcClient::new();
        client.set_completion_pool_size(1024);
        client.start_reaper(1_000_000_000, 100_000_000);
        Self {
            client,
            server,
            connections: ConnectionPoolIndex::new(1, Some(1000)),
            next_id: AtomicU64::new(1),
        }
    }

    pub(crate) async fn probe(&self, endpoint: &str) -> bool {
        let origin = if endpoint.contains("://") {
            endpoint.to_owned()
        } else {
            format!("tcp://{endpoint}")
        };
        let Ok(url) = reqwest::Url::parse(&origin) else {
            return false;
        };
        let (Some(host), Some(port)) = (url.host_str(), url.port()) else {
            return false;
        };
        let connection = self.connections.get_or_try_install(endpoint, || {
            let connection = self.server.connect(host, i32::from(port))?;
            self.client.attach(&connection);
            Ok::<_, crowdb_rpc_ffi::RpcError>(connection)
        });
        let Ok(connection) = connection else {
            return false;
        };
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let request = ConnectionPingRequest::create(
            &mut builder,
            &ConnectionPingRequestArgs {
                id,
                rpc_create_nano: 0,
            },
        );
        builder.finish(request, None);
        let call = self.client.call(
            &self.server,
            &connection,
            id,
            Buffer::from_bytes(builder.finished_data()),
            None,
            FBMsgType::EConnectionPingRequest.0 as u16,
        );
        let ok = match call {
            Ok(call) => match tokio::time::timeout(Duration::from_millis(1200), call).await {
                Ok(Ok(response)) => response.control.as_ref().is_some_and(|control| {
                    flatbuffers::root::<ConnectionPingResponse>(control.bytes())
                        .is_ok_and(|reply| reply.id() == id)
                }),
                _ => false,
            },
            Err(_) => false,
        };
        if !ok {
            self.connections.invalidate(endpoint, connection.generation());
        }
        ok
    }
}
