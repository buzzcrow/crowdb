// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Standalone control and serving surface for chunk-backed KV partitions.

pub mod balance;
pub mod catalog;
pub mod lease;
pub mod monitor;
pub mod scan;
pub mod server;
pub mod transfer;

pub use balance::{
    choose_split, choose_transfer, desired_partition_count, BalanceConfig, OwnerLoad, PartitionLoad,
    SplitProposal, TransferProposal,
};
pub use catalog::{CatalogError, CatalogPublisher, CatalogStore, HeadWriteOutcome, MemoryCatalogStore};
pub use lease::{classify_instance, replacement_may_activate, AuthorityError, ServingAuthority};
pub use monitor::{
    DomainMonitorDriver, DomainMonitorRegistry, MonitorDescriptorStore, MonitorError, MonitorTick,
    PreparedMonitor,
};
pub use scan::{validate_and_clip_scan, ClippedScan, ScanValidationError};
pub use server::ChunkKvService;
pub use transfer::{TransferAction, TransferStateMachine};
