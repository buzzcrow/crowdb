// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

pub(crate) mod signed_file;
use crowdb_access_iceberg::catalog::{Capabilities, ManagementPrivilege};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};
use crowdb_access_iceberg::storage;
use crowdb_chunk_client::{ChunkReadPolicy, SmallWritePolicy};

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

#[tokio::test]
#[ignore = "requires the owned release Iceberg storage"]
async fn initialize() {
    let ips = crate::common::management::addresses(12);
    let seeds = ips[..3]
        .iter()
        .map(|ip| crate::common::management::origin(ip))
        .collect();
    let small = SmallWritePolicy {
        conversion_enabled: false,
        mirror_copies: 2,
        ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
    };
    let (repository, _, chunks) = storage::connect(seeds, ChunkReadPolicy::default(), small, 2, 2)
        .await
        .unwrap();
    for (action, epoch, capabilities) in [
        (ManagementAction::Initialize, 0, None),
        (
            ManagementAction::Activate,
            1,
            Some(Capabilities::from_bits(0x3fff).unwrap()),
        ),
    ] {
        repository
            .execute(
                ManagementRequest {
                    identity: RequestIdentity {
                        operation: OperationId::random(),
                        issued_ms: now_ms(),
                    },
                    principal: "manager".into(),
                    action,
                    expected_epoch: epoch,
                    display_name: "protected-http".into(),
                    confirmation: None,
                    capabilities,
                },
                ManagementPrivilege::Manage,
                now_ms(),
            )
            .await
            .unwrap();
    }
    chunks.shutdown_small_writes().await.unwrap();
}
