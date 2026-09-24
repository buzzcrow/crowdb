//! Bounded candidate evaluation and checks, separate from file proofs and publication.

mod evaluator;
mod files;
mod journal;
mod operation;
mod preparation;
mod provenance;
mod request;
mod requirement;
mod transition;
mod update;

pub use evaluator::{evaluate_metadata_updates, EvaluatedMetadata, EvaluationError, EvaluationLimits};
pub use files::{
    CandidateAuxiliaryLimits, CandidateAuxiliarySummary, CandidateFileSource, CandidateSnapshotLimits,
    CandidateSnapshotSummary,
};
pub use journal::TableCommitJournal;
pub use operation::{TableCommitOperation, TableCommitOutcome, TableCommitPhase};
pub use preparation::{evaluate_durable_commit, CommitPreparationError, CommitPreparationLimits};
pub use provenance::{PriorManifestLimits, PriorManifestSource};
pub use request::{CommitRequest, CommitRequestLimits, CommitTableIdentifier};
pub use requirement::{validate_requirements, RequirementError, RequirementLimits, TableRequirement};
pub use transition::{validate_metadata_transition, TransitionLimits};
pub use update::{MetadataObject, SnapshotRefType, TableUpdate};
