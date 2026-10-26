// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    task::JoinHandle,
};

pub struct TestPeer {
    pub endpoint: String,
    pub connections: Arc<AtomicUsize>,
    pub requests: mpsc::Receiver<String>,
    pub release: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl TestPeer {
    pub async fn start(body: &'static str, gated: bool) -> Self {
        Self::start_statuses(body, gated, vec![200]).await
    }
    pub async fn start_statuses(body: &'static str, gated: bool, statuses: Vec<u16>) -> Self {
        Self::start_responses(statuses.into_iter().map(|status| (status, body)).collect(), gated).await
    }
    pub async fn start_responses(responses: Vec<(u16, &'static str)>, gated: bool) -> Self {
        assert!(!responses.is_empty());
        let responses = Arc::new(responses);
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&connections);
        let (tx, requests) = mpsc::channel(16);
        let (release, gate) = watch::channel(!gated);
        let task = tokio::spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=> {
                        let (socket,_)=accepted.unwrap(); counter.fetch_add(1,Ordering::AcqRel);
                        clients.spawn(serve(socket,tx.clone(),gate.clone(),responses.clone(),calls.clone()));
                    },
                    Some(_)=clients.join_next(),if !clients.is_empty()=>{},
                }
            }
        });
        Self {
            endpoint,
            connections,
            requests,
            release,
            task,
        }
    }
}
impl Drop for TestPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(
    mut socket: TcpStream,
    tx: mpsc::Sender<String>,
    mut gate: watch::Receiver<bool>,
    responses: Arc<Vec<(u16, &'static str)>>,
    calls: Arc<AtomicUsize>,
) {
    loop {
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            let Ok(byte) = socket.read_u8().await else { return };
            header.push(byte);
            assert!(header.len() <= 4096);
        }
        let header = String::from_utf8(header).unwrap();
        let length = header
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length: ")
                    .map(|value| value.parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut data = vec![0; length];
        socket.read_exact(&mut data).await.unwrap();
        tx.send(format!("{header}{}", String::from_utf8_lossy(&data)))
            .await
            .unwrap();
        let index = calls.fetch_add(1, Ordering::AcqRel);
        let (status, body) = responses[index.min(responses.len() - 1)];
        while !*gate.borrow() {
            if gate.changed().await.is_err() {
                return;
            }
        }
        let response = format!(
            "HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        if socket.write_all(response.as_bytes()).await.is_err() {
            return;
        }
    }
}
