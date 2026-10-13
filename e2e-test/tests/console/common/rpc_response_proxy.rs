// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Forward real RPC traffic, discarding one armed response after the server
/// has executed its request. Reconnected traffic proceeds normally.
pub struct TestResponseProxy {
    pub endpoint: String,
    pub management_endpoint: String,
    pub armed: Arc<AtomicBool>,
    pub dropped: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
    management_server: tokio::task::JoinHandle<()>,
}

impl TestResponseProxy {
    pub async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let advertised = endpoint.clone();
        let management = axum::Router::new().route(
            "/topology",
            axum::routing::get(move || {
                let endpoint = advertised.clone();
                async move {
                    axum::Json(crowdb_protocol::mgmt::TopologyResponse {
                        stores: vec![crowdb_protocol::mgmt::StoreStatus {
                            store_id: 0,
                            listen_addr: Some(endpoint),
                            groups: vec![crowdb_protocol::mgmt::GroupStatus {
                                group_id: 0,
                                local_replica_id: 1,
                                leader_id: 1,
                                local_replica: crowdb_protocol::mgmt::ReplicaStatus {
                                    id: 1,
                                    ..Default::default()
                                },
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                    })
                }
            }),
        );
        let management_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let management_endpoint = format!("http://{}", management_listener.local_addr().unwrap());
        let management_server = tokio::spawn(async move {
            axum::serve(management_listener, management).await.unwrap();
        });
        let armed = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicUsize::new(0));
        let arm = armed.clone();
        let drop_count = dropped.clone();
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (downstream, _) = accepted.unwrap();
                        let upstream = upstream.clone();
                        let arm = arm.clone();
                        let drop_count = drop_count.clone();
                        connections.spawn(async move {
                            forward(downstream, &upstream, arm, drop_count).await;
                        });
                    }
                    Some(_) = connections.join_next() => {}
                }
            }
        });
        Self {
            endpoint,
            management_endpoint,
            armed,
            dropped,
            server,
            management_server,
        }
    }
}

impl Drop for TestResponseProxy {
    fn drop(&mut self) {
        self.server.abort();
        self.management_server.abort();
    }
}

async fn forward(downstream: TcpStream, upstream: &str, armed: Arc<AtomicBool>, dropped: Arc<AtomicUsize>) {
    let upstream = TcpStream::connect(upstream).await.unwrap();
    let (mut input, mut output) = downstream.into_split();
    let (mut responses, mut requests) = upstream.into_split();
    let send = tokio::io::copy(&mut input, &mut requests);
    let receive = async {
        let mut buffer = [0; 8192];
        loop {
            let Ok(count) = responses.read(&mut buffer).await else {
                return;
            };
            if count == 0 {
                return;
            }
            if armed.swap(false, Ordering::SeqCst) {
                dropped.fetch_add(1, Ordering::SeqCst);
                return;
            }
            if output.write_all(&buffer[..count]).await.is_err() {
                return;
            }
        }
    };
    tokio::select! {
        _ = send => {}
        () = receive => {}
    }
}
