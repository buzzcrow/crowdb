// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Server-owned child composition drafts and revision-bound evaluation state.

mod record;
pub use record::{CompositionRecord, CompositionSource, DraftProgress};

mod begin;
mod edit;
mod evaluate_step;
mod evaluation;
pub use evaluation::CompositionPreview;
mod input;
mod key;
mod prepare;
mod state;
pub use input::{CompositionDraft, CompositionEdit, CompositionInput, SampleChange};

#[derive(Debug, thiserror::Error)]
pub enum CompositionError {
    #[error(transparent)]
    Authority(#[from] crate::AuthorityError),
    #[error(transparent)]
    Store(#[from] crate::StoreError),
    #[error(transparent)]
    Record(#[from] crate::split::SplitError),
    #[error(transparent)]
    Split(#[from] crate::split::SplitLifecycleError),
    #[error(transparent)]
    Inspection(#[from] crate::inspection::InspectionError),
    #[error("composition was not found")]
    NotFound,
    #[error("composition precondition changed")]
    Conflict,
    #[error("composition metadata is corrupt")]
    Corrupt,
    #[error("composition exceeds its work limit")]
    Limit,
    #[error("composition is frozen")]
    Frozen,
}
