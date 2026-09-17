// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Domain serving: grants, leases, monitoring, partition transfer, and
//! rebalance planning.

pub mod balance;
pub mod lease;
pub mod monitor;
pub mod split;
pub mod transfer;
mod transition_runtime;
mod worker;

pub use balance::{
    choose_split, choose_transfer, desired_partition_count, BalanceConfig, OwnerLoad, PartitionLoad,
    SplitProposal, TransferProposal,
};
pub use lease::{classify_instance, replacement_may_activate, AuthorityError, ServingAuthority};
pub use monitor::{
    DomainMonitorDriver, DomainMonitorRegistry, MonitorDescriptorStore, MonitorError, MonitorTick,
    PreparedMonitor,
};
pub use split::{SplitAction, SplitStateMachine};
pub use transfer::{TransferAction, TransferStateMachine};
pub use transition_runtime::TransitionProcessor;
pub use worker::{PreparedLocalSplit, TransitionExecutor, TransitionStorage};
