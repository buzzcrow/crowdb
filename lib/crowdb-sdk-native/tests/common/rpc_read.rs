// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig, TestDiskRoute};
use crowdb_protocol::diskio_fb::{
    FBDiskIoRetCode, FBDiskReadRequest, FBDiskReadResponse, FBDiskReadResponseArgs,
};
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{Buffer, RdmaConfig, RpcServer};
use flatbuffers::FlatBufferBuilder;

use super::storage::TestReadStorage;

pub struct TestRpcReadServer {
    pub server: Arc<RpcServer>,
    pub replies: Arc<AtomicUsize>,
    address: String,
    config: RdmaConfig,
}

impl TestRpcReadServer {
    pub fn start(backend: &TestReadStorage, rdma: bool) -> Self {
        let address = if rdma {
            std::env::var("CROWDB_RDMA_E2E_LISTEN_ADDR").unwrap()
        } else {
            "127.0.0.1".into()
        };
        let config = RdmaConfig {
            enabled: rdma,
            allow_tcp_fallback: !rdma,
            listen_enabled: rdma,
            source_address: if rdma { address.clone() } else { String::new() },
            device: if rdma {
                std::env::var("CROWDB_RDMA_DEVICE").unwrap_or_default()
            } else {
                String::new()
            },
            sq_depth: 4,
            rq_depth: 4,
            posted_receive_target: 4,
            cq_depth: 16,
            ..RdmaConfig::default()
        }
        .with_control_data_capacity(65_536)
        .unwrap();
        let server = Arc::new(RpcServer::new(None));
        server.configure_rdma(&config).unwrap();
        server.listen(&address, 0).unwrap();
        let replies = Arc::new(AtomicUsize::new(0));
        let count = replies.clone();
        let data = backend.data[0].clone();
        server.register_owned_handler(
            u16::try_from(FBMsgType::EDiskReadRequest.0).unwrap(),
            move |request| {
                let read = flatbuffers::root::<FBDiskReadRequest<'_>>(request.control()).unwrap();
                assert_eq!(read.zone_offset(), 0);
                assert_eq!(read.size() as usize, data.len());
                let mut builder = FlatBufferBuilder::new();
                let response = FBDiskReadResponse::create(
                    &mut builder,
                    &FBDiskReadResponseArgs {
                        id: request.request_id,
                        rpc_create_nano: 0,
                        ret_code: FBDiskIoRetCode::Success,
                    },
                );
                builder.finish(response, None);
                request
                    .reply(
                        Buffer::from_bytes(builder.finished_data()),
                        Some(Buffer::from_vec(data.to_vec())),
                        u16::try_from(FBMsgType::EDiskReadResponse.0).unwrap(),
                    )
                    .unwrap();
                count.fetch_add(1, Ordering::Release);
            },
        );
        server.start();
        Self {
            server,
            replies,
            address,
            config,
        }
    }

    pub fn client(&self) -> Arc<DiskioClient> {
        Arc::new(
            DiskioClient::connect_for_tests(
                vec![TestDiskRoute {
                    disk_id: DiskId::new(1, 1),
                    rack_id: 1,
                    node_id: 1,
                    disk_group_id: 1,
                    instance_id: 1,
                    endpoint: format!("{}:{}", self.address, self.server.port()),
                }],
                DiskioClientConfig {
                    rdma: Some(RdmaConfig {
                        listen_enabled: false,
                        ..self.config.clone()
                    }),
                    normal_connections_per_endpoint: 1,
                    priority_connections_per_endpoint: 1,
                    rpc_workers: 1,
                    retry_attempts: 1,
                    ..DiskioClientConfig::default()
                },
            )
            .unwrap(),
        )
    }
}

impl Drop for TestRpcReadServer {
    fn drop(&mut self) {
        self.server.stop();
    }
}
