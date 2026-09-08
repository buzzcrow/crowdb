// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Persistent chunk task domain, storage, scanning, and execution.

pub mod executor;
pub mod manager;
pub mod scanner;
pub mod store;

pub use executor::{TaskExecutor, TaskHandler, TaskOutcome, TaskRegistryError};
pub use manager::{TaskAdmission, TaskClaim, TaskManager, TaskManagerError};
pub use scanner::{TaskScanSummary, TaskScanner};
pub use store::{TaskStore, TaskStoreError};
