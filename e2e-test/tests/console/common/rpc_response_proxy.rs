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
    pub forwarded_handshakes: Arc<AtomicUsize>,
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
        let forwarded_handshakes = Arc::new(AtomicUsize::new(0));
        let arm = armed.clone();
        let drop_count = dropped.clone();
        let handshakes = forwarded_handshakes.clone();
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (downstream, _) = accepted.unwrap();
                        let upstream = upstream.clone();
                        let arm = arm.clone();
                        let drop_count = drop_count.clone();
                        let handshakes = handshakes.clone();
                        connections.spawn(async move {
                            forward(downstream, &upstream, arm, drop_count, handshakes).await;
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
            forwarded_handshakes,
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

async fn forward(
    downstream: TcpStream,
    upstream: &str,
    armed: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
    handshakes: Arc<AtomicUsize>,
) {
    let upstream = TcpStream::connect(upstream).await.unwrap();
    let (mut input, mut output) = downstream.into_split();
    let (mut responses, mut requests) = upstream.into_split();
    let send = tokio::io::copy(&mut input, &mut requests);
    let receive = async {
        let mut header = [0; 14];
        loop {
            if responses.read_exact(&mut header).await.is_err() {
                return;
            }
            assert_eq!(u16::from_le_bytes(header[..2].try_into().unwrap()), 0xca70);
            let message_type = u16::from_le_bytes(header[2..4].try_into().unwrap());
            let control_bytes = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
            let data_bytes = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
            let offset = usize::from(header[12]);
            assert!(offset >= header.len());
            let mut body = vec![0; offset - header.len() + control_bytes + data_bytes];
            if responses.read_exact(&mut body).await.is_err() {
                return;
            }
            // Capability advertisements precede application replies and do not
            // establish that the conditional write has completed.
            let handshake = message_type == 0xfffe;
            if !handshake && armed.swap(false, Ordering::SeqCst) {
                dropped.fetch_add(1, Ordering::SeqCst);
                return;
            }
            if output.write_all(&header).await.is_err() || output.write_all(&body).await.is_err() {
                return;
            }
            if handshake {
                handshakes.fetch_add(1, Ordering::SeqCst);
            }
        }
    };
    tokio::select! {
        _ = send => {}
        () = receive => {}
    }
}
