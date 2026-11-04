// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig, TestDiskRoute};
use crowdb_protocol::diskio_fb::{
    FBDiskIoRetCode, FBDiskReadRequest, FBDiskReadResponse, FBDiskReadResponseArgs,
};
use flatbuffers::FlatBufferBuilder;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy)]
pub enum TestProbeReply {
    Ready,
    Rejected,
    Held,
}

#[derive(Default)]
pub struct TestReadState {
    pub bulk_started: Notify,
    pub release_bulk: Notify,
    pub release_probe: Notify,
    pub headers: AtomicUsize,
}

pub struct TestReadinessPeer {
    pub state: Arc<TestReadState>,
    address: String,
    worker: JoinHandle<()>,
}

impl TestReadinessPeer {
    pub async fn start(reply: TestProbeReply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let state = Arc::new(TestReadState::default());
        let worker = tokio::spawn({
            let state = state.clone();
            async move {
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        value=listener.accept()=>{let(stream,_)=value.unwrap();connections.spawn(serve(stream,state.clone(),reply));},
                        value=connections.join_next(),if !connections.is_empty()=>{value.unwrap().unwrap();}
                    }
                }
            }
        });
        Self {
            state,
            address,
            worker,
        }
    }

    pub fn client(&self) -> Arc<DiskioClient> {
        Arc::new(
            DiskioClient::connect_for_tests(
                vec![TestDiskRoute {
                    disk_id: DiskId::new(1, 2),
                    rack_id: 1,
                    node_id: 1,
                    disk_group_id: 1,
                    instance_id: 1,
                    endpoint: self.address.clone(),
                }],
                DiskioClientConfig {
                    normal_connections_per_endpoint: 1,
                    priority_connections_per_endpoint: 1,
                    retry_attempts: 1,
                    ..Default::default()
                },
            )
            .unwrap(),
        )
    }
}

impl Drop for TestReadinessPeer {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

async fn serve(mut stream: TcpStream, state: Arc<TestReadState>, reply: TestProbeReply) {
    loop {
        let mut header = [0; 14];
        if stream.read_exact(&mut header).await.is_err() {
            return;
        }
        assert_eq!(u16::from_le_bytes(header[..2].try_into().unwrap()), 0xca70);
        let kind = u16::from_le_bytes(header[2..4].try_into().unwrap());
        let length = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        assert!(length <= 4096);
        assert_eq!(&header[8..12], &[0; 4]);
        let mut control = vec![0; length];
        stream.read_exact(&mut control).await.unwrap();
        if kind == 0xfffe {
            assert_eq!(control.len(), 160);
            let mut capabilities = vec![0; 160];
            capabilities[..4].copy_from_slice(&[0x44, 0x52, 4, 0]);
            capabilities[4..12].copy_from_slice(&1_u64.to_le_bytes());
            header[13] = 0;
            if stream.write_all(&header).await.is_err() {
                return;
            }
            if stream.write_all(&capabilities).await.is_err() {
                return;
            }
            continue;
        }
        let request = flatbuffers::root::<FBDiskReadRequest>(&control).unwrap();
        let id = request.id();
        let size = request.size() as usize;
        assert!(matches!(size, 4096 | 65536));
        let bulk = size == 65536;
        if !bulk {
            state.headers.fetch_add(1, Ordering::Release);
            if matches!(reply, TestProbeReply::Held) {
                state.release_probe.notified().await;
            }
        }
        let code = if !bulk && matches!(reply, TestProbeReply::Rejected) {
            FBDiskIoRetCode::DiskNotExist
        } else {
            FBDiskIoRetCode::Success
        };
        let mut builder = FlatBufferBuilder::new();
        let value = FBDiskReadResponse::create(
            &mut builder,
            &FBDiskReadResponseArgs {
                id,
                ret_code: code,
                ..Default::default()
            },
        );
        builder.finish(value, None);
        let control = builder.finished_data();
        let bytes = if code == FBDiskIoRetCode::Success { size } else { 0 };
        let mut response = Vec::with_capacity(14 + control.len());
        response.extend_from_slice(&0xca70_u16.to_le_bytes());
        response.extend_from_slice(&(kind + 1).to_le_bytes());
        response.extend_from_slice(&u32::try_from(control.len()).unwrap().to_le_bytes());
        response.extend_from_slice(&u32::try_from(bytes).unwrap().to_le_bytes());
        response.extend_from_slice(&[14, 0]);
        response.extend_from_slice(control);
        if stream.write_all(&response).await.is_err() {
            return;
        }
        if bulk {
            if stream.write_all(&[7]).await.is_err() {
                return;
            }
            state.bulk_started.notify_one();
            state.release_bulk.notified().await;
            if stream.write_all(&vec![7; bytes - 1]).await.is_err() {
                return;
            }
        } else if bytes != 0 && stream.write_all(&vec![9; bytes]).await.is_err() {
            return;
        }
    }
}
