// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable named Split sets, scoped to one data Snapshot.

pub(crate) mod codec;
pub(crate) mod definition;
mod draft;
pub mod key;
mod record;

pub use definition::{
    CoveragePolicy, MembershipOrigin, OverlapPolicy, PartitionWeight, SplitDefinition, SplitName,
    SplitPolicy, SplitSource,
};
pub use draft::{DraftSplit, SplitDraftRecord};
pub use record::{
    MembershipPage, MembershipPageBinding, NamedSplitRecord, SplitHead, SplitRevisionRecord,
    MAX_MEMBERSHIP_PAGE_ITEMS, MAX_SPLIT_RECORD_BYTES,
};

#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
pub enum SplitError {
    #[error("split name must contain between 1 and 255 UTF-8 bytes")]
    InvalidName,
    #[error("split record is malformed or has an unsupported version")]
    InvalidRecord,
    #[error("split metadata exceeds its configured bound")]
    Limit,
    #[error("split definition is invalid")]
    InvalidDefinition,
}
