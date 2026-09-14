// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Multi-scan and batch composition for [`crate::ChunkKvClient`].

mod batch;
mod ordered;

pub use batch::{BatchItem, ComposedItemError, MultiGetItemResult};
pub use ordered::{MultiScanContinuation, MultiScanPage, MultiScanRequest};
