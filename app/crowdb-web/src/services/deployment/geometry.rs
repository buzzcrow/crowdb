// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::HardwareClient;
use crowdb_protocol::chunk_allocation_geometry::uniform_allocation_unit;
use crowdb_protocol::common::HwStatus;

use crate::error::{err_409, err_502};
use crate::state::AppState;

pub(super) async fn validate(state: &AppState) -> Result<(), super::Failure> {
    let hardware = HardwareClient::from_shared(state.kv_client().await);
    let disks = hardware
        .list_all_disks()
        .await
        .map_err(|error| err_502(error.to_string()))?;
    uniform_allocation_unit(
        disks
            .iter()
            .filter(|disk| disk.value.status == HwStatus::Up as i32)
            .map(|disk| disk.value.unit_size_bytes),
    )
    .map_err(|error| err_409(error.to_string()))?;
    Ok(())
}
