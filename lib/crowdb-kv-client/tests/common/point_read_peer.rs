// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::{
    fb::FBMsgType,
    kv_client_fb::{
        FBKvClientRetCode, FBKvGetRequest, FBKvResponse, FBKvResponseArgs, FBKvScanItem, FBKvScanItemArgs,
        FBKvScanRequest, FBKvScanResponse, FBKvScanResponseArgs,
    },
};
use flatbuffers::FlatBufferBuilder;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Notify},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy)]
pub enum TestPointReply {
    Ready,
    Rejected,
    Held,
    ClosedOnce,
}

#[derive(Default)]
pub struct TestPointState {
    pub bulk_started: Notify,
    pub release_bulk: Notify,
    pub accepted: AtomicUsize,
    pub scans: AtomicUsize,
    pub gets: AtomicUsize,
}

pub struct TestPointReadPeer {
    pub endpoint: String,
    pub state: Arc<TestPointState>,
    worker: JoinHandle<()>,
}

struct Reply {
    bytes: Vec<u8>,
    hold: bool,
}

impl TestPointReadPeer {
    pub async fn start(reply: TestPointReply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let state = Arc::new(TestPointState::default());
        let worker = tokio::spawn({
            let state = state.clone();
            async move {
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        value=listener.accept()=>{
                            let(stream,_)=value.unwrap();
                            state.accepted.fetch_add(1,Ordering::Release);
                            connections.spawn(serve(stream,state.clone(),reply));
                        },
                        result=connections.join_next(),if !connections.is_empty()=>{result.unwrap().unwrap();}
                    }
                }
            }
        });
        Self {
            endpoint,
            state,
            worker,
        }
    }
}

impl Drop for TestPointReadPeer {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

async fn serve(stream: TcpStream, state: Arc<TestPointState>, reply: TestPointReply) {
    let (reader, writer) = tokio::io::split(stream);
    let (tx, rx) = mpsc::channel(8);
    tokio::join!(
        read_requests(reader, tx, state.clone(), reply),
        write_replies(writer, rx, state)
    );
}

async fn read_requests(
    mut reader: ReadHalf<TcpStream>,
    tx: mpsc::Sender<Reply>,
    state: Arc<TestPointState>,
    reply: TestPointReply,
) {
    loop {
        let mut header = [0; 14];
        if reader.read_exact(&mut header).await.is_err() {
            return;
        }
        assert_eq!(u16::from_le_bytes(header[..2].try_into().unwrap()), 0xca70);
        let kind = u16::from_le_bytes(header[2..4].try_into().unwrap());
        let length = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        assert!(length <= 4096);
        assert_eq!(&header[8..12], &[0; 4]);
        let mut control = vec![0; length];
        reader.read_exact(&mut control).await.unwrap();
        let response = if kind == 0xfffe {
            assert_eq!(control.len(), 160);
            let mut capabilities = vec![0; 160];
            capabilities[..4].copy_from_slice(&[0x44, 0x52, 4, 0]);
            capabilities[4..12].copy_from_slice(&1_u64.to_le_bytes());
            frame(kind, &capabilities, false)
        } else if kind == FBMsgType::EKvScanRequest.0 as u16 {
            let id = flatbuffers::root::<FBKvScanRequest>(&control).unwrap().id();
            let hold = state.scans.fetch_add(1, Ordering::AcqRel) == 0;
            scan_reply(id, hold)
        } else {
            assert_eq!(kind, FBMsgType::EKvGetRequest.0 as u16);
            let id = flatbuffers::root::<FBKvGetRequest>(&control).unwrap().id();
            let index = state.gets.fetch_add(1, Ordering::AcqRel);
            if matches!(reply, TestPointReply::ClosedOnce) && index == 0 {
                return;
            }
            if matches!(reply, TestPointReply::Held) {
                continue;
            }
            point_reply(id, reply)
        };
        if tx.send(response).await.is_err() {
            return;
        }
    }
}

async fn write_replies(
    mut writer: WriteHalf<TcpStream>,
    mut rx: mpsc::Receiver<Reply>,
    state: Arc<TestPointState>,
) {
    while let Some(reply) = rx.recv().await {
        if reply.hold {
            if writer.write_all(&reply.bytes[..15]).await.is_err() {
                return;
            }
            state.bulk_started.notify_one();
            state.release_bulk.notified().await;
            if writer.write_all(&reply.bytes[15..]).await.is_err() {
                return;
            }
        } else if writer.write_all(&reply.bytes).await.is_err() {
            return;
        }
    }
}

fn frame(kind: u16, control: &[u8], hold: bool) -> Reply {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&0xca70_u16.to_le_bytes());
    bytes.extend_from_slice(&kind.to_le_bytes());
    bytes.extend_from_slice(&u32::try_from(control.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&[14, 0]);
    bytes.extend_from_slice(control);
    Reply { bytes, hold }
}

fn scan_reply(id: u64, hold: bool) -> Reply {
    let mut builder = FlatBufferBuilder::new();
    let key = builder.create_vector(b"/bulk/value");
    let value = builder.create_vector(&vec![7_u8; 65536]);
    let item = FBKvScanItem::create(
        &mut builder,
        &FBKvScanItemArgs {
            key: Some(key),
            value: Some(value),
            ..Default::default()
        },
    );
    let items = builder.create_vector(&[item]);
    let response = FBKvScanResponse::create(
        &mut builder,
        &FBKvScanResponseArgs {
            id,
            version: 1,
            ok: true,
            items: Some(items),
            ..Default::default()
        },
    );
    builder.finish(response, None);
    frame(FBMsgType::EKvScanResponse.0 as u16, builder.finished_data(), hold)
}

fn point_reply(id: u64, reply: TestPointReply) -> Reply {
    let mut builder = FlatBufferBuilder::new();
    let response = FBKvResponse::create(
        &mut builder,
        &FBKvResponseArgs {
            id,
            version: 1,
            ok: matches!(reply, TestPointReply::Ready | TestPointReply::ClosedOnce),
            not_found: matches!(reply, TestPointReply::Ready | TestPointReply::ClosedOnce),
            ret_code: if matches!(reply, TestPointReply::Rejected) {
                FBKvClientRetCode::Unavailable
            } else {
                FBKvClientRetCode::Success
            },
            ..Default::default()
        },
    );
    builder.finish(response, None);
    frame(FBMsgType::EKvResponse.0 as u16, builder.finished_data(), false)
}
