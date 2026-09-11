// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Catalog store and scan validation.

mod scan;
mod store;

pub use scan::{validate_and_clip_scan, ClippedScan, ScanValidationError};
pub use store::{CatalogError, CatalogPublisher, CatalogStore, HeadWriteOutcome, MemoryCatalogStore};
