// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Standalone control and serving surface for chunk-backed KV partitions.

pub mod catalog;
pub mod config;
pub mod control_store;
pub mod management;
pub mod metrics;
pub mod rpc;
pub mod server;
pub mod serving;
pub mod storage;

pub use catalog::{
    validate_and_clip_scan, ChunkKvRangeCatalogCutover, ChunkKvRangeCatalogError,
    ChunkKvRangeCatalogPublisher, ChunkKvRangeCatalogStore, ClippedScan, HeadWriteOutcome,
    MemoryChunkKvRangeCatalogStore, ScanValidationError,
};
pub use config::{BootstrapPartitionConfig, ChunkKvServerConfig, ConfigError, StorageConfig};
pub use control_store::{Group0ControlStore, Group0Kv, Group0KvError, VersionedValue};
pub use management::{management_router, ManagementState};
pub use metrics::{ServerMetrics, ServerMetricsSnapshot};
pub use rpc::ChunkKvRpcService;
pub use server::{
    ChunkKvRangeCatalogReconcileError, ChunkKvService, HostedPartitionHealth, ServerHealth, ServerLifecycle,
};
pub use serving::{
    choose_split, choose_transfer, classify_instance, desired_partition_count, replacement_may_activate,
    AuthorityError, BalanceConfig, DomainMonitorDriver, DomainMonitorRegistry, MonitorDescriptorStore,
    MonitorError, MonitorTick, OwnerLoad, PartitionLoad, PreparedMonitor, ServingAuthority, SplitAction,
    SplitProposal, SplitStateMachine, TransferAction, TransferProposal, TransferStateMachine,
    TransitionExecutor, TransitionProcessor, TransitionStorage,
};
pub use storage::{ChunkKvStorage, StorageRuntimeError};
