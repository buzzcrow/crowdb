// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable metadata projections, separate from payload and publication records.

mod index;
mod key;
mod record;

pub use record::{FieldMetadata, FieldStorageKind, InspectionState, SnapshotSummary};
