// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable metadata projections, separate from payload and publication records.

mod index;
mod key;
mod legacy;
mod preparation;
mod record;
mod rows;
mod status;

pub use record::{FieldMetadata, FieldStorageKind, InspectionState, SnapshotSummary};

pub use preparation::{InspectionError, InspectionPreparation, PreparationLimits, PreparationState};

pub(crate) mod work;

mod pages;
pub use pages::InspectionPage;

mod content;
mod evaluate;

mod exact;
mod materialize;
mod matrix;
mod read;
pub(crate) use read::PreparedInspectionRead;
mod uploads;
pub use matrix::InspectionSample;

pub(crate) mod cursor;

mod split_evaluation;

mod split_rules;

mod reclaim;

mod retirement;
pub(crate) use retirement::RETIRED_RECORD;
