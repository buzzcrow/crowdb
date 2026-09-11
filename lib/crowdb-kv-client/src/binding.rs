// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Key → service-instance binding framework and strategies.

pub mod chunkdb_strategy;
pub mod framework;
pub mod range;

pub use chunkdb_strategy::{compute_sub_range_assignment, ChunkdbRangeStrategy, DEFAULT_SUB_RANGE_COUNT};
pub use framework::{BindingMonitor, BindingStrategy, MonitorTickResult};
pub use range::{ChunkdbRangeBinding, RangeBindingClient, RangeRouteError, RouteWithFallback};
