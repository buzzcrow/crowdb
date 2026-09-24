//! Bounded candidate evaluation and checks, separate from file proofs and publication.

mod create;
mod evaluator;
mod files;
mod journal;
mod operation;
mod preparation;
mod proof;
mod provenance;
mod publication;
mod request;
mod requirement;
mod transition;
mod update;

pub use create::{
    evaluate_table_creation, CreateTableRequest, InitialTableMetadata, TableCreateJournal,
    TableCreateOperation, TableCreatePhase,
};
pub use create::{TableCreationRequest, TableCreator};
pub use evaluator::{
    evaluate_metadata_updates, evaluate_table_create_commit, EvaluatedMetadata, EvaluationError,
    EvaluationLimits,
};
pub use files::{
    CandidateAuxiliaryLimits, CandidateAuxiliarySummary, CandidateFileSource, CandidateSnapshotLimits,
    CandidateSnapshotSummary,
};
pub use journal::TableCommitJournal;
pub use operation::{TableCommitOperation, TableCommitOutcome, TableCommitPhase};
pub use preparation::{evaluate_durable_commit, CommitPreparationError, CommitPreparationLimits};
pub use proof::{prepare_table_commit, CommitProofError, CommitProofLimits, PreparedTableCommit};
pub use provenance::{PriorManifestLimits, PriorManifestSource};
pub use publication::{recover_table_commit, CommitPublicationError};
pub use request::{CommitRequest, CommitRequestLimits, CommitTableIdentifier};
pub use requirement::{validate_requirements, RequirementError, RequirementLimits, TableRequirement};
pub use transition::{validate_metadata_transition, TransitionLimits};
pub use update::{MetadataObject, SnapshotRefType, TableUpdate};
