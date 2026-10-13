// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

async fn recover(service: &str, signal: &str) {
    super::witness_with_write(true, true).await;
    let observed = crate::common::control::recover_service(service, signal).await;
    assert_ne!(observed["before"]["pid"], observed["after"]["pid"]);
    assert!(
        observed["after"]["generation"].as_u64().unwrap()
            > observed["before"]["generation"].as_u64().unwrap()
    );
    super::witness_with_write(true, false).await;
}

macro_rules! service_cases {
    ($service:literal, $crash:ident, $hang:ident) => {
        #[tokio::test]
        #[ignore = "requires the owned complete release container"]
        async fn $crash() {
            recover($service, "KILL").await;
        }
        #[tokio::test]
        #[ignore = "requires the owned complete release container"]
        async fn $hang() {
            recover($service, "STOP").await;
        }
    };
}

service_cases!("kv", kv_crash, kv_hang);
service_cases!("diskdb", diskdb_crash, diskdb_hang);
service_cases!("diskio", diskio_crash, diskio_hang);
service_cases!("chunkdb", chunkdb_crash, chunkdb_hang);
service_cases!("chunk-kv", chunk_kv_crash, chunk_kv_hang);
service_cases!("access", access_crash, access_hang);
service_cases!("web", web_crash, web_hang);
