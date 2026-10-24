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

mod partition;
mod policy;
pub use partition::{partition_bucket, SamplePartition};
pub use policy::SplitValidation;

mod lifecycle;
mod state;
pub use lifecycle::SplitLifecycleError;

mod cleanup;
mod snapshot_cleanup;
pub use snapshot_cleanup::SnapshotSplitCleanup;
mod evaluation;
mod work;
pub use cleanup::SplitScratchCleanup;
pub(crate) use cleanup::SEALED as RETIRED_RECORD;
pub use evaluation::SplitPreview;

mod preparation;
pub use preparation::SplitPreparation;

mod head;
pub use head::{SplitHeadUpdate, SplitReference};

mod publication;

mod recovery;

mod read;
pub use read::SplitRevisionSummary;

mod inheritance;

mod identity;
mod initial;
pub(crate) use identity::candidate_revision;
