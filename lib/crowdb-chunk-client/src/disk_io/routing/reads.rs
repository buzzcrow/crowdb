// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Preserve semantic routing and refresh behavior for both transport lanes.

use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_rpc_ffi::OwnedPayload;

use super::{map_read_error, DiskioError, IoError, Result, RoutedDiskWriter, TrafficLane};

impl RoutedDiskWriter {
    pub(super) async fn read_payload_in_lane(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        segment_offset: u64,
        length: u32,
        lane: TrafficLane,
    ) -> Result<OwnedPayload> {
        let target = Self::target(seg, unit_bytes).map_err(|error| IoError::ReadFailed(error.to_string()))?;
        let options = || {
            let mut options = self.client.normal_options();
            options.lane = lane;
            options
        };
        let result = self
            .client
            .read_payload(target, segment_offset, length, options())
            .await;
        match result {
            Err(DiskioError::TransportUnavailable(_) | DiskioError::TopologyUnavailable(_)) => {
                // A restarted DiskIO can advertise a new endpoint while the
                // retained generation still points at its old connection.
                // Reads are safe to retry once after an authoritative refresh.
                self.client.refresh().await.map_err(map_read_error)?;
                self.client
                    .read_payload(target, segment_offset, length, options())
                    .await
                    .map_err(map_read_error)
            }
            result => result.map_err(map_read_error),
        }
    }
}
