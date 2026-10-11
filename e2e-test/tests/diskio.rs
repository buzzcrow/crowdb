// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod concurrency;
mod durability;
mod errors;
mod faults;
mod group0;
mod hardware;
mod io;
mod io_round;
mod pattern;
mod semantic;
mod semantic_views;
use crowdb_diskio_client::TestWireDiskioClient;
use crowdb_rpc_ffi::{Connection, RpcServer};
use std::sync::Arc;

fn connect() -> (Arc<RpcServer>, Connection, Arc<TestWireDiskioClient>) {
    let addresses: Vec<String> = serde_json::from_str(&std::env::var("CROWDB_E2E_RPC").unwrap()).unwrap();
    connect_address(addresses.last().unwrap())
}

fn connect_at(index: usize) -> (Arc<RpcServer>, Connection, Arc<TestWireDiskioClient>) {
    let addresses: Vec<String> = serde_json::from_str(&std::env::var("CROWDB_E2E_RPC").unwrap()).unwrap();
    connect_address(&addresses[index])
}

fn connect_address(address: &str) -> (Arc<RpcServer>, Connection, Arc<TestWireDiskioClient>) {
    let server = Arc::new(RpcServer::new(None));
    server.start();
    let connection = server.connect(address, 7200).expect("packaged Diskio endpoint");
    let client = Arc::new(TestWireDiskioClient::new());
    client.attach(&connection);
    (server, connection, client)
}

async fn wait_for_disks(client: &TestWireDiskioClient, server: &RpcServer, connection: &Connection) {
    wait_for_disk(client, server, connection, false).await;
}

async fn wait_for_disk(
    client: &TestWireDiskioClient,
    server: &RpcServer,
    connection: &Connection,
    fault: bool,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let response = client
            .fsync(server, connection, crowdb_diskio_client::DiskId::new(0, 1))
            .unwrap();
        match TestWireDiskioClient::await_fsync_response(response).await {
            Ok(crowdb_diskio_client::DiskIoRetCode::Success) => return,
            Err(crowdb_diskio_client::TestWireDiskioError::IoError(
                crowdb_diskio_client::DiskIoRetCode::DiskNotExist,
            )) => {}
            Err(crowdb_diskio_client::TestWireDiskioError::IoError(
                crowdb_diskio_client::DiskIoRetCode::IoError,
            )) if fault => return,
            other => panic!("packaged Diskio readiness failed: {other:?}"),
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Diskio never discovered its disks"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

fn registry() -> crowdb_kv_client::ServiceRegistryClient {
    crowdb_kv_client::ServiceRegistryClient::new(crate::common::hardware::kv_client())
}
fn hardware_client() -> crowdb_kv_client::HardwareClient {
    crowdb_kv_client::HardwareClient::new(crate::common::hardware::kv_client())
}
