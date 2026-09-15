// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Background scanner — periodic consistency check that detects
//! live-state drift (ghost allocations), catches record corruption
//! early, and gives operators visibility into cluster health during
//! uptime.

pub mod ghost;
pub mod integrity;
pub mod leak;
pub mod owner;
pub mod task;

pub use ghost::{GhostBlock, GhostDirection, GhostScanResult};
pub use integrity::{IntegrityFinding, IntegrityScanResult};
pub use leak::LeakScanResult;
pub use owner::{BusyBlockOwnerScanner, SegmentOwnerQuery, ZonePacer};
pub use task::{FallbackReason, ScanState, ScanSummary, ScannerTask};
