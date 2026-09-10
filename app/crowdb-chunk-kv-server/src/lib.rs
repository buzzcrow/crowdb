// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Standalone control and serving surface for chunk-backed KV partitions.

pub mod catalog;
pub mod lease;
pub mod monitor;
pub mod server;

pub use catalog::{CatalogError, CatalogPublisher, CatalogStore, HeadWriteOutcome, MemoryCatalogStore};
pub use lease::{classify_instance, replacement_may_activate, AuthorityError, ServingAuthority};
pub use monitor::{
    DomainMonitorDriver, DomainMonitorRegistry, MonitorDescriptorStore, MonitorError, MonitorTick,
    PreparedMonitor,
};
pub use server::ChunkKvService;
