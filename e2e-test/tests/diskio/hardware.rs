// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[tokio::test]
#[ignore = "requires the owned container fixture"]
async fn seed() {
    let hardware = crowdb_kv_client::HardwareClient::new(crate::common::hardware::kv_client());
    crate::common::hardware::seed_hardware(&hardware, &crate::common::hardware::standard_disk_ids_4()).await;
}
