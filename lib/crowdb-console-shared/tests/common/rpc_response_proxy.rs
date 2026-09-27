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
    pub armed: Arc<AtomicBool>,
    pub dropped: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}

impl TestResponseProxy {
    pub async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
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
            armed,
            dropped,
            server,
        }
    }
}

impl Drop for TestResponseProxy {
    fn drop(&mut self) {
        self.server.abort();
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
