//! Durable, bounded reclamation after reachability and retention proof.

mod admission;
mod budget;
mod candidate;
mod claim;
mod discovery;
mod fence;
mod limits;
mod mark;
mod node;
mod page;
mod proof;
mod reachability;
mod repository;
mod retirement;
mod storage;
mod task;
mod tree;
mod worker;

pub use budget::{BudgetedGcBlocks, BudgetedGcStore, GcIoBudget};
pub use candidate::{CandidatePhase, GcCandidate};
pub use limits::GcLimits;
pub use mark::GcMarkError;
pub use node::GcNode;
pub use page::GcPage;
pub use proof::GcProofState;
pub use reachability::{
    avro_links, metadata_links, AvroMarkCursor, AvroMarkLimits, AvroMarkPage, ReachableFile, ReachableKind,
};
pub use repository::GcRepository;
pub use storage::{GcScan, GcStore, GcSystemScan};
pub use task::{GcPhase, GcStalledReason, GcTask, GcTaskKind};
pub use tree::{ReclaimFrame, ReclaimStep, TreeReclaimCursor};
pub use worker::{GcWorkError, GcWorker, GcWorkerStatus};
