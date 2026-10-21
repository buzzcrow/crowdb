// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Volatile protocol stub for pinned placement/task proofs. Packaged Diskio
//! discovery, bytes and durability are verified by the container acceptance.

use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::diskio_fb::{
    FBDiskFsyncRequest, FBDiskFsyncResponse, FBDiskFsyncResponseArgs, FBDiskReadRequest, FBDiskReadResponse,
    FBDiskReadResponseArgs, FBDiskWriteRequest, FBDiskWriteResponse, FBDiskWriteResponseArgs,
};
use crowdb_protocol::fb::{
    ConnectionPingRequest, ConnectionPingResponse, ConnectionPingResponseArgs, FBMsgType,
};
use flatbuffers::FlatBufferBuilder;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

type Blocks = Arc<Mutex<HashMap<([u8; 16], u32, u64), Vec<u8>>>>;

pub struct TestDiskio {
    worker: JoinHandle<()>,
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
        let worker = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        connections.spawn(serve(stream, Arc::clone(&blocks)));
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
        Self { worker }
    }
}

impl Drop for TestDiskio {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

async fn serve(mut stream: TcpStream, blocks: Blocks) {
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
        let (reply, payload) = response(kind, &ctrl, data, &blocks);
        let mut outgoing = vec![];
        outgoing.extend_from_slice(&0xca70_u16.to_le_bytes());
        outgoing.extend_from_slice(&(kind + 1).to_le_bytes());
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

fn response(kind: u16, ctrl: &[u8], data: Vec<u8>, blocks: &Blocks) -> (Vec<u8>, Vec<u8>) {
    let mut builder = FlatBufferBuilder::new();
    let mut payload = vec![];
    if kind == FBMsgType::EDiskWriteRequest.0 as u16 {
        let request = flatbuffers::root::<FBDiskWriteRequest>(ctrl).unwrap();
        assert_eq!(data.len(), request.size() as usize);
        blocks.lock().unwrap().insert(
            (
                request.disk_id().unwrap().0,
                request.zone_index(),
                request.zone_offset(),
            ),
            data,
        );
        let reply = FBDiskWriteResponse::create(
            &mut builder,
            &FBDiskWriteResponseArgs {
                id: request.id(),
                ..Default::default()
            },
        );
        builder.finish(reply, None);
    } else if kind == FBMsgType::EDiskReadRequest.0 as u16 {
        let request = flatbuffers::root::<FBDiskReadRequest>(ctrl).unwrap();
        payload = read_blocks(blocks, &request);
        let reply = FBDiskReadResponse::create(
            &mut builder,
            &FBDiskReadResponseArgs {
                id: request.id(),
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
