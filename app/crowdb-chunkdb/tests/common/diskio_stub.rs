// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Volatile protocol stub for pinned placement/task proofs. Packaged Diskio
//! discovery, bytes and durability are verified by the container acceptance.

use crowdb_paxos_kv_client::ServiceRegistryClient;
use crowdb_protocol::diskio_fb::{
    FBDiskFsyncRequest, FBDiskFsyncResponse, FBDiskFsyncResponseArgs, FBDiskIoRetCode, FBDiskReadRequest,
    FBDiskReadResponse, FBDiskReadResponseArgs, FBDiskWriteRequest, FBDiskWriteResponse,
    FBDiskWriteResponseArgs,
};
use crowdb_protocol::fb::{
    ConnectionPingRequest, ConnectionPingResponse, ConnectionPingResponseArgs, FBMsgType,
};
use flatbuffers::FlatBufferBuilder;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

type Blocks = Arc<Mutex<HashMap<([u8; 16], u32, u64), Vec<u8>>>>;

pub struct TestDiskio {
    worker: JoinHandle<()>,
    pub control: Arc<TestDiskioControl>,
}

#[derive(Default)]
pub struct TestDiskioControl {
    pub reject_drains: AtomicBool,
    pub drain_calls: AtomicUsize,
    pub read_calls: AtomicUsize,
}

impl TestDiskio {
    pub async fn start(
        service: &ServiceRegistryClient,
        instance: u64,
        rack: u64,
        node: u64,
        group: u64,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let blocks = Blocks::default();
        let control = Arc::new(TestDiskioControl::default());
        let serving_control = Arc::clone(&control);
        let worker = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        connections.spawn(serve(stream, Arc::clone(&blocks), Arc::clone(&serving_control)));
                    }
                    finished = connections.join_next(), if !connections.is_empty() => {
                        finished.unwrap().unwrap();
                    }
                }
            }
        });
        service
            .heartbeat_diskio_at(instance, &address.to_string(), rack, node, &[group], &[])
            .await
            .unwrap();
        Self { worker, control }
    }
}

impl Drop for TestDiskio {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

async fn serve(mut stream: TcpStream, blocks: Blocks, control: Arc<TestDiskioControl>) {
    loop {
        let mut header = [0; 14];
        if stream.read_exact(&mut header).await.is_err() {
            return;
        }
        assert_eq!(u16::from_le_bytes(header[..2].try_into().unwrap()), 0xca70);
        assert_eq!(header[12], 14);
        let kind = u16::from_le_bytes(header[2..4].try_into().unwrap());
        let ctrl_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let data_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        assert!(ctrl_len <= 4096 && data_len <= 2 * 1024 * 1024);
        let mut ctrl = vec![0; ctrl_len];
        let mut data = vec![0; data_len];
        if stream.read_exact(&mut ctrl).await.is_err() || stream.read_exact(&mut data).await.is_err() {
            return;
        }
        let (reply, payload) = response(kind, &ctrl, data, &blocks, &control);
        let mut outgoing = vec![];
        outgoing.extend_from_slice(&0xca70_u16.to_le_bytes());
        let reply_kind = if kind == 0xfffe { kind } else { kind + 1 };
        outgoing.extend_from_slice(&reply_kind.to_le_bytes());
        outgoing.extend_from_slice(&u32::try_from(reply.len()).unwrap().to_le_bytes());
        outgoing.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
        outgoing.extend_from_slice(&[14, 0]);
        outgoing.extend_from_slice(&reply);
        outgoing.extend_from_slice(&payload);
        if stream.write_all(&outgoing).await.is_err() {
            return;
        }
    }
}

fn response(
    kind: u16,
    ctrl: &[u8],
    data: Vec<u8>,
    blocks: &Blocks,
    control: &TestDiskioControl,
) -> (Vec<u8>, Vec<u8>) {
    if kind == 0xfffe {
        assert_eq!(ctrl.len(), 160);
        assert_eq!(&ctrl[..4], &[0x44, 0x52, 4, 0]);
        assert!(data.is_empty());
        let mut capability = vec![0; 160];
        capability[..4].copy_from_slice(&[0x44, 0x52, 4, 0]);
        capability[4..12].copy_from_slice(&1_u64.to_le_bytes());
        return (capability, Vec::new());
    }
    let mut builder = FlatBufferBuilder::new();
    let mut payload = vec![];
    if kind == FBMsgType::EDiskWriteRequest.0 as u16 {
        let request = flatbuffers::root::<FBDiskWriteRequest>(ctrl).unwrap();
        assert_eq!(data.len(), request.size() as usize);
        let mut sync_completed = request.sync();
        if data.is_empty() && request.sync() {
            control.drain_calls.fetch_add(1, Ordering::Relaxed);
            sync_completed = !control.reject_drains.load(Ordering::Relaxed);
        } else {
            blocks.lock().unwrap().insert(
                (
                    request.disk_id().unwrap().0,
                    request.zone_index(),
                    request.zone_offset(),
                ),
                data,
            );
        }
        let reply = FBDiskWriteResponse::create(
            &mut builder,
            &FBDiskWriteResponseArgs {
                id: request.id(),
                sync_completed,
                ..Default::default()
            },
        );
        builder.finish(reply, None);
    } else if kind == FBMsgType::EDiskReadRequest.0 as u16 {
        control.read_calls.fetch_add(1, Ordering::Relaxed);
        let request = flatbuffers::root::<FBDiskReadRequest>(ctrl).unwrap();
        let now_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let age = checked_read_age(
            request.read_create_time_ms(),
            request.fence_probe(),
            request.size(),
            now_ms,
        );
        let (ret_code, request_age_checked) = match age {
            Ok(checked) => {
                if !request.fence_probe() {
                    payload = read_blocks(blocks, &request);
                }
                (FBDiskIoRetCode::Success, checked)
            }
            Err(error) => (error, false),
        };
        let reply = FBDiskReadResponse::create(
            &mut builder,
            &FBDiskReadResponseArgs {
                id: request.id(),
                ret_code,
                request_age_checked,
                ..Default::default()
            },
        );
        builder.finish(reply, None);
    } else if kind == FBMsgType::EDiskFsyncRequest.0 as u16 {
        let request = flatbuffers::root::<FBDiskFsyncRequest>(ctrl).unwrap();
        let reply = FBDiskFsyncResponse::create(
            &mut builder,
            &FBDiskFsyncResponseArgs {
                id: request.id(),
                ..Default::default()
            },
        );
        builder.finish(reply, None);
    } else {
        assert_eq!(kind, FBMsgType::EConnectionPingRequest.0 as u16);
        let request = flatbuffers::root::<ConnectionPingRequest>(ctrl).unwrap();
        let reply = ConnectionPingResponse::create(
            &mut builder,
            &ConnectionPingResponseArgs {
                id: request.id(),
                ..Default::default()
            },
        );
        builder.finish(reply, None);
    }
    (builder.finished_data().to_vec(), payload)
}

fn read_blocks(blocks: &Blocks, request: &FBDiskReadRequest<'_>) -> Vec<u8> {
    let mut bytes = vec![0; request.size() as usize];
    let start = request.zone_offset();
    let end = start + u64::from(request.size());
    for ((disk, zone, offset), data) in blocks.lock().unwrap().iter() {
        if *disk != request.disk_id().unwrap().0 || *zone != request.zone_index() {
            continue;
        }
        let begin = start.max(*offset);
        let finish = end.min(*offset + u64::try_from(data.len()).unwrap());
        if begin < finish {
            let target = usize::try_from(begin - start).unwrap();
            let source = usize::try_from(begin - offset).unwrap();
            let length = usize::try_from(finish - begin).unwrap();
            bytes[target..target + length].copy_from_slice(&data[source..source + length]);
        }
    }
    bytes
}

// Mirror the production DiskIO admission age ceiling for synchronous fixture reads.
pub fn checked_read_age(
    created_ms: u64,
    probe: bool,
    size: u32,
    now_ms: u64,
) -> Result<bool, FBDiskIoRetCode> {
    use crowdb_protocol::timing::{MAX_WRITE_CLOCK_SKEW_MS, MAX_WRITE_REQUEST_AGE_MS};
    if probe && (created_ms == 0 || size != 0) {
        return Err(FBDiskIoRetCode::OldRequest);
    }
    if created_ms == 0 {
        return Ok(false);
    }
    let deadline = created_ms.checked_add(MAX_WRITE_REQUEST_AGE_MS + MAX_WRITE_CLOCK_SKEW_MS);
    if created_ms.saturating_sub(now_ms) > MAX_WRITE_CLOCK_SKEW_MS
        || deadline.map_or(true, |deadline| deadline <= now_ms)
    {
        return Err(FBDiskIoRetCode::OldRequest);
    }
    Ok(true)
}
