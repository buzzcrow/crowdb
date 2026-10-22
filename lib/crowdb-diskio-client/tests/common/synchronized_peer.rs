// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig, TestDiskRoute};
use crowdb_protocol::diskio_fb::{
    FBDiskFsyncRequest, FBDiskFsyncResponse, FBDiskFsyncResponseArgs, FBDiskIoRetCode, FBDiskWriteRequest,
    FBDiskWriteResponse, FBDiskWriteResponseArgs,
};
use crowdb_protocol::fb::FBMsgType;
use flatbuffers::FlatBufferBuilder;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};

#[derive(Default)]
pub struct TestCounts {
    pub writes: AtomicUsize,
    pub sync_requests: AtomicUsize,
    pub fsyncs: AtomicUsize,
    pub bytes: AtomicUsize,
}

pub struct TestDiskioPeer {
    pub counts: Arc<TestCounts>,
    pub gate: Arc<Notify>,
    address: String,
    worker: JoinHandle<()>,
}

impl TestDiskioPeer {
    pub async fn start(modern: bool, delayed: bool, code: FBDiskIoRetCode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let counts = Arc::new(TestCounts::default());
        let gate = Arc::new(Notify::new());
        let worker = tokio::spawn({
            let counts = Arc::clone(&counts);
            let gate = Arc::clone(&gate);
            async move {
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let (stream, _) = accepted.unwrap();
                            connections.spawn(serve(stream, Arc::clone(&counts),
                                Arc::clone(&gate), modern, delayed, code));
                        }
                        done = connections.join_next(), if !connections.is_empty() => {
                            done.unwrap().unwrap();
                        }
                    }
                }
            }
        });
        Self {
            counts,
            gate,
            address,
            worker,
        }
    }

    pub fn client(&self) -> DiskioClient {
        DiskioClient::connect_for_tests(
            vec![self.route(DiskId::new(1, 2))],
            DiskioClientConfig {
                retry_attempts: 1,
                ..Default::default()
            },
        )
        .unwrap()
    }

    pub fn route(&self, disk_id: DiskId) -> TestDiskRoute {
        TestDiskRoute {
            disk_id,
            rack_id: 1,
            node_id: disk_id.high,
            disk_group_id: 1,
            instance_id: disk_id.high,
            endpoint: self.address.clone(),
        }
    }

    pub async fn wait_for_write(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut tick = tokio::time::interval(Duration::from_millis(1));
            while self.counts.writes.load(Ordering::Acquire) == 0 {
                tick.tick().await;
            }
        })
        .await
        .unwrap();
    }
}

impl Drop for TestDiskioPeer {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

async fn serve(
    mut stream: TcpStream,
    counts: Arc<TestCounts>,
    gate: Arc<Notify>,
    modern: bool,
    delayed: bool,
    code: FBDiskIoRetCode,
) {
    loop {
        let mut header = [0; 14];
        if stream.read_exact(&mut header).await.is_err() {
            return;
        }
        assert_eq!(u16::from_le_bytes(header[..2].try_into().unwrap()), 0xca70);
        let kind = u16::from_le_bytes(header[2..4].try_into().unwrap());
        let length = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let data_length = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        assert_eq!(header[12], 14);
        assert!(length <= 4096 && data_length <= 1024 * 1024);
        let mut control = vec![0; length];
        let mut data = vec![0; data_length];
        stream.read_exact(&mut control).await.unwrap();
        stream.read_exact(&mut data).await.unwrap();
        let (id, sync) = if kind == FBMsgType::EDiskWriteRequest.0 as u16 {
            let request = flatbuffers::root::<FBDiskWriteRequest>(&control).unwrap();
            assert_eq!(data.len(), request.size() as usize);
            assert!(data.iter().all(|byte| *byte == 7));
            assert_eq!(request.ordering_zone_offset(), 0);
            let offset = counts.bytes.fetch_add(data.len(), Ordering::AcqRel);
            assert_eq!(request.zone_offset(), offset as u64);
            counts.writes.fetch_add(1, Ordering::Release);
            if request.sync() {
                counts.sync_requests.fetch_add(1, Ordering::Relaxed);
            }
            (request.id(), request.sync())
        } else {
            assert_eq!(kind, FBMsgType::EDiskFsyncRequest.0 as u16);
            counts.fsyncs.fetch_add(1, Ordering::Relaxed);
            (
                flatbuffers::root::<FBDiskFsyncRequest>(&control).unwrap().id(),
                false,
            )
        };
        if sync && delayed {
            gate.notified().await;
        }
        let mut builder = FlatBufferBuilder::new();
        if kind == FBMsgType::EDiskWriteRequest.0 as u16 {
            let reply = FBDiskWriteResponse::create(
                &mut builder,
                &FBDiskWriteResponseArgs {
                    id,
                    ret_code: code,
                    sync_completed: modern && sync && code == FBDiskIoRetCode::Success,
                    ..Default::default()
                },
            );
            builder.finish(reply, None);
        } else {
            let reply = FBDiskFsyncResponse::create(
                &mut builder,
                &FBDiskFsyncResponseArgs {
                    id,
                    ..Default::default()
                },
            );
            builder.finish(reply, None);
        }
        let reply = builder.finished_data();
        let mut response = Vec::with_capacity(14 + reply.len());
        response.extend_from_slice(&0xca70_u16.to_le_bytes());
        response.extend_from_slice(&(kind + 1).to_le_bytes());
        response.extend_from_slice(&u32::try_from(reply.len()).unwrap().to_le_bytes());
        response.extend_from_slice(&0_u32.to_le_bytes());
        response.extend_from_slice(&[14, 0]);
        response.extend_from_slice(reply);
        if stream.write_all(&response).await.is_err() {
            return;
        }
    }
}
