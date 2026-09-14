// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Hardware hierarchy, space-usage aggregation, and system metadata facade.

pub mod hierarchy;
pub mod space_usage;
pub mod sysmd;

pub use hierarchy::{
    DiskCapacityEntry, DiskGroupCapacityEntry, DiskRecord, HardwareCapacitySummary, HardwareClient,
    NodeCapacityEntry, RackCapacityEntry,
};
pub use space_usage::{ClusterUsage, NodeUsage, RackUsage, SpaceUsageClient};
pub use sysmd::CrowdbSysmdClient;
