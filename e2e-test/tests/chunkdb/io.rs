// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig, Durability, SegmentTarget};
use crowdb_paxos_kv_client::{HardwareClient, ServiceRegistryClient};
use std::time::{Duration, Instant};

mod readiness;

pub(super) async fn client() -> DiskioClient {
    DiskioClient::connect_with_clients(
        ServiceRegistryClient::new(crate::common::hardware::kv_client()),
        HardwareClient::new(crate::common::hardware::kv_client()),
        DiskioClientConfig::default(),
    )
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires the owned storage fixture before its Diskio services start"]
async fn unavailable_before_services_start() {
    assert!(crowdb_chunkdb::conversion::io::ConversionDiskIo::connect(
        &ServiceRegistryClient::new(crate::common::hardware::kv_client()),
        &HardwareClient::new(crate::common::hardware::kv_client()),
    )
    .await
    .is_err());
}

#[tokio::test]
#[ignore = "requires the owned storage container fixture"]
async fn ready() {
    let artifacts = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let started_at_ms = std::fs::read_to_string(artifacts.join("storage-started-at-ms"))
        .unwrap()
        .parse()
        .unwrap();
    let service = ServiceRegistryClient::new(crate::common::hardware::kv_client());
    let hardware = HardwareClient::new(crate::common::hardware::kv_client());
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match readiness::probe(&service, &hardware, started_at_ms).await {
            Ok(()) => return,
            Err(error) => assert!(Instant::now() < deadline, "storage discovery failed: {error}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore = "requires the owned storage container fixture"]
async fn all_failure_domains_route_exact_segment_bytes() {
    let client = client().await;
    assert_eq!(client.status().disks, 18);
    for group in 1000..1006 {
        for disk in 1..=3 {
            let target = SegmentTarget::new(DiskId::new(0, group * 10 + disk), 0, 0, 1, 1024 * 1024).unwrap();
            let payload = Bytes::from(format!("rack={group},disk={disk}:container-segment-bytes"));
            client
                .write(
                    target,
                    17,
                    payload.clone(),
                    Durability::Fsync,
                    client.normal_options(),
                )
                .await
                .unwrap();
            let actual = client
                .read(
                    target,
                    17,
                    u32::try_from(payload.len()).unwrap(),
                    client.normal_options(),
                )
                .await
                .unwrap();
            assert_eq!(actual, payload);
        }
    }
}
