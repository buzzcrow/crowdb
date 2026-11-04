// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::{
    chunk_slot::{
        ChunkServiceIncarnation, ChunkSlotAuthority, ChunkSlotBinding, ChunkSlotBootstrap, ChunkStorageGroup,
    },
    fb::FBMsgType,
    key::{
        ChunkServiceAuthorityKey, ChunkServiceSlotsKey, ChunkSlotMapHeadKey, ChunkStorageSlotsKey, TextKey,
    },
    kv_client_fb::{
        FBKvBatchWriteRequest, FBKvClientRetCode, FBKvGetRequest, FBKvResponse, FBKvResponseArgs,
        FBKvScanItem, FBKvScanItemArgs, FBKvScanRequest, FBKvScanResponse, FBKvScanResponseArgs,
    },
};
use flatbuffers::FlatBufferBuilder;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

pub struct TestPublicationPeer {
    pub endpoint: String,
    pub submissions: Arc<AtomicUsize>,
    pub committed_generation: Arc<AtomicU64>,
    worker: JoinHandle<()>,
}

struct Request {
    kind: u16,
    control: Vec<u8>,
    response: oneshot::Sender<Vec<u8>>,
}

struct Records {
    values: BTreeMap<Vec<u8>, (u64, Vec<u8>)>,
    revision: u64,
    moving_head_reads: usize,
    submissions: Arc<AtomicUsize>,
    committed_generation: Arc<AtomicU64>,
}

impl TestPublicationPeer {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let submissions = Arc::new(AtomicUsize::new(0));
        let committed_generation = Arc::new(AtomicU64::new(2));
        let mut records = Records::new(submissions.clone(), committed_generation.clone());
        let (tx, mut rx) = mpsc::channel::<Request>(16);
        let worker = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{
                        let (stream,_)=accepted.unwrap();
                        connections.spawn(serve(stream,tx.clone()));
                    }
                    Some(request)=rx.recv()=>{
                        let response=records.reply(request.kind,&request.control);
                        let _=request.response.send(response);
                    }
                    completed=connections.join_next(),if !connections.is_empty()=>{
                        completed.unwrap().unwrap();
                    }
                }
            }
        });
        Self {
            endpoint,
            submissions,
            committed_generation,
            worker,
        }
    }
}

impl Drop for TestPublicationPeer {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

impl Records {
    fn new(submissions: Arc<AtomicUsize>, committed_generation: Arc<AtomicU64>) -> Self {
        let bootstrap = ChunkSlotBootstrap {
            service_instances: vec![1, 2],
            storage_groups: vec![ChunkStorageGroup {
                store_id: 0,
                group_id: 1,
            }],
        };
        let service = bootstrap.service_map().unwrap();
        let storage = bootstrap.storage_map().unwrap();
        let mut values = BTreeMap::new();
        let mut head = service.head().clone();
        head.generation = 2;
        for binding in service.bindings() {
            let mut binding = binding.clone();
            binding.generation = 2;
            values.insert(
                ChunkServiceSlotsKey {
                    instance_id: binding.owner,
                }
                .to_path()
                .into_bytes(),
                (100, serde_json::to_vec(&binding).unwrap()),
            );
            let authority = ChunkSlotAuthority::new(
                binding.owner,
                ChunkServiceIncarnation::try_from([1; 16]).unwrap(),
                1,
            )
            .unwrap();
            let binding = ChunkSlotBinding {
                generation: 2,
                owner: authority,
                slots: binding.slots,
            };
            values.insert(
                ChunkServiceAuthorityKey { authority }.to_path().into_bytes(),
                (100, serde_json::to_vec(&binding).unwrap()),
            );
        }
        for key in [ChunkSlotMapHeadKey::Service, ChunkSlotMapHeadKey::Authority] {
            values.insert(
                key.to_path().into_bytes(),
                (100, serde_json::to_vec(&head).unwrap()),
            );
        }
        for binding in storage.bindings() {
            values.insert(
                ChunkStorageSlotsKey {
                    store_id: binding.owner.store_id,
                    group_id: binding.owner.group_id,
                }
                .to_path()
                .into_bytes(),
                (100, serde_json::to_vec(binding).unwrap()),
            );
        }
        values.insert(
            ChunkSlotMapHeadKey::Storage.to_path().into_bytes(),
            (100, serde_json::to_vec(storage.head()).unwrap()),
        );
        Self {
            values,
            revision: 100,
            moving_head_reads: 0,
            submissions,
            committed_generation,
        }
    }

    fn reply(&mut self, kind: u16, control: &[u8]) -> Vec<u8> {
        if kind == FBMsgType::EKvBatchWriteRequest.0 as u16 {
            return self.batch(control);
        }
        if kind == FBMsgType::EKvGetRequest.0 as u16 {
            let request = flatbuffers::root::<FBKvGetRequest>(control).unwrap();
            assert_eq!(request.group_id(), 0);
            let key = request.key().unwrap().bytes();
            if key == ChunkSlotMapHeadKey::Service.to_path().as_bytes() && self.moving_head_reads > 0 {
                // A competing cohort reservation can rewrite an unchanged service head.
                self.moving_head_reads -= 1;
                self.revision += 1;
                self.values.get_mut(key).unwrap().0 = self.revision;
            }
            let found = self.values.get(key);
            return point(
                request.id(),
                true,
                self.revision,
                found,
                FBKvClientRetCode::Success,
            );
        }
        assert_eq!(kind, FBMsgType::EKvScanRequest.0 as u16);
        let request = flatbuffers::root::<FBKvScanRequest>(control).unwrap();
        assert_eq!(request.group_id(), 0);
        let cutoff = if request.scan_cutoff() == 0 {
            self.revision
        } else {
            request.scan_cutoff()
        };
        let prefix = request.prefix().unwrap().bytes();
        let after = request.start_after().map_or(&[][..], |key| key.bytes());
        let rows: Vec<_> = self
            .values
            .iter()
            .filter(|(key, (revision, _))| {
                key.starts_with(prefix) && key.as_slice() > after && *revision <= cutoff
            })
            .collect();
        let mut builder = FlatBufferBuilder::new();
        let items: Vec<_> = rows
            .into_iter()
            .map(|(key, (revision, value))| {
                let key = builder.create_vector(key);
                let value = builder.create_vector(value);
                FBKvScanItem::create(
                    &mut builder,
                    &FBKvScanItemArgs {
                        key: Some(key),
                        value: Some(value),
                        commit_slot: *revision,
                    },
                )
            })
            .collect();
        let items = builder.create_vector(&items);
        let response = FBKvScanResponse::create(
            &mut builder,
            &FBKvScanResponseArgs {
                id: request.id(),
                version: 1,
                ok: true,
                items: Some(items),
                read_slot: self.revision,
                scan_cutoff: cutoff,
                ..Default::default()
            },
        );
        builder.finish(response, None);
        frame(FBMsgType::EKvScanResponse.0 as u16, builder.finished_data())
    }

    fn batch(&mut self, control: &[u8]) -> Vec<u8> {
        let request = flatbuffers::root::<FBKvBatchWriteRequest>(control).unwrap();
        assert_eq!(request.group_id(), 0);
        let precondition = request.precondition().unwrap();
        let key = precondition.key().unwrap().bytes();
        assert_eq!(key, ChunkSlotMapHeadKey::Service.to_path().as_bytes());
        assert_eq!(precondition.expected_revision(), self.values[key].0);
        self.revision += 1;
        for item in request.items().unwrap() {
            let key = item.key().unwrap().bytes().to_vec();
            if item.is_delete() {
                self.values.remove(&key);
            } else {
                self.values
                    .insert(key, (self.revision, item.value().unwrap().bytes().to_vec()));
            }
        }
        let first = self.submissions.fetch_add(1, Ordering::AcqRel) == 0;
        let head = &self.values[ChunkSlotMapHeadKey::Service.to_path().as_bytes()].1;
        self.committed_generation.store(
            serde_json::from_slice::<serde_json::Value>(head).unwrap()["generation"]
                .as_u64()
                .unwrap(),
            Ordering::Release,
        );
        if first {
            // The mutation committed, but its caller received no confirmed outcome.
            // Nine head changes exhaust read_service's four consistent-read attempts.
            self.moving_head_reads = 9;
        }
        point(
            request.id(),
            !first,
            self.revision,
            None,
            if first {
                FBKvClientRetCode::OutcomeUnknown
            } else {
                FBKvClientRetCode::Success
            },
        )
    }
}

fn point(
    id: u64,
    ok: bool,
    read_slot: u64,
    found: Option<&(u64, Vec<u8>)>,
    code: FBKvClientRetCode,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let value = found.map(|(_, value)| builder.create_vector(value));
    let response = FBKvResponse::create(
        &mut builder,
        &FBKvResponseArgs {
            id,
            version: 1,
            ok,
            revision: found.map_or(read_slot, |(revision, _)| *revision),
            value,
            not_found: ok && found.is_none(),
            ret_code: code,
            read_slot,
            ..Default::default()
        },
    );
    builder.finish(response, None);
    frame(FBMsgType::EKvResponse.0 as u16, builder.finished_data())
}

fn frame(kind: u16, control: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(control.len() + 14);
    bytes.extend_from_slice(&0xca70_u16.to_le_bytes());
    bytes.extend_from_slice(&kind.to_le_bytes());
    bytes.extend_from_slice(&u32::try_from(control.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&[14, 0]);
    bytes.extend_from_slice(control);
    bytes
}

async fn serve(mut stream: TcpStream, tx: mpsc::Sender<Request>) {
    loop {
        let mut header = [0; 14];
        if stream.read_exact(&mut header).await.is_err() {
            return;
        }
        assert_eq!(u16::from_le_bytes(header[..2].try_into().unwrap()), 0xca70);
        assert_eq!(&header[8..12], &[0; 4]);
        let kind = u16::from_le_bytes(header[2..4].try_into().unwrap());
        let length = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        assert!(length <= 65536);
        let mut control = vec![0; length];
        stream.read_exact(&mut control).await.unwrap();
        let reply = if kind == 0xfffe {
            let mut capabilities = vec![0; 160];
            capabilities[..4].copy_from_slice(&[0x44, 0x52, 4, 0]);
            capabilities[4..12].copy_from_slice(&1_u64.to_le_bytes());
            frame(kind, &capabilities)
        } else {
            let (response, rx) = oneshot::channel();
            if tx
                .send(Request {
                    kind,
                    control,
                    response,
                })
                .await
                .is_err()
            {
                return;
            }
            let Ok(response) = rx.await else {
                return;
            };
            response
        };
        if stream.write_all(&reply).await.is_err() {
            return;
        }
    }
}
