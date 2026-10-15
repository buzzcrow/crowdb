// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Server-owned child composition drafts and revision-bound evaluation state.

mod record;
pub use record::{CompositionRecord, CompositionSource, DraftProgress};

mod begin;
mod candidates;
mod cleanup;
pub use candidates::CompositionCandidateCleanup;
pub use cleanup::CompositionScratchCleanup;
mod edit;
mod edit_cleanup;
pub use edit_cleanup::CompositionEditCleanup;
mod evaluate_step;
mod evaluation;
pub use evaluation::CompositionPreview;
mod input;
mod key;
mod prepare;
mod publication;
mod state;
mod uploads;
pub use input::{CompositionDraft, CompositionEdit, CompositionInput, CompositionPreparation, SampleChange};
pub use uploads::{CompositionUploadCleanup, CompositionUploadStatus};

#[derive(Debug, thiserror::Error)]
pub enum CompositionError {
    #[error(transparent)]
    Upload(#[from] crate::UploadError),
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
