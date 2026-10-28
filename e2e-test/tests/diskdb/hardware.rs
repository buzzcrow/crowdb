// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

pub(super) use crate::common::hardware::{
    make_disk_id, standard_disk_ids_3, CAPACITY_UNITS, DG_ID, UNIT_SIZE_BYTES, ZONE_COUNT,
};

#[tokio::test]
#[ignore = "requires the owned container fixture"]
async fn seed() {
    let hardware = crowdb_paxos_kv_client::HardwareClient::new(super::kv_client());
    crate::common::hardware::seed_hardware(&hardware, &standard_disk_ids_3()).await;
}
