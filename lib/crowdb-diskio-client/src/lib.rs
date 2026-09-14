// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Routed semantic client for CROWDB `DiskIO`.

mod address;
mod client;
mod error;
mod semantic;
mod status;
mod topology;

pub use address::{DiskId, SegmentTarget};
pub use error::{DiskioError, DiskioResult};
pub use semantic::{
    DiskioClient, DiskioClientConfig, Durability, NativeDiskIoRoutes, OperationOptions, TrafficLane,
};
pub use status::DiskioStatus;

#[cfg(feature = "test-util")]
pub use semantic::TestDiskRoute;

#[cfg(feature = "test-util")]
pub use topology::{validate_topology_for_tests, TestTopologyDisk, TestTopologyInstance};

#[cfg(feature = "test-util")]
pub use client::{
    DiskIoRetCode, WireClient as TestWireDiskioClient, WireError as TestWireDiskioError,
    WireWriteTarget as TestWireWriteTarget,
};
