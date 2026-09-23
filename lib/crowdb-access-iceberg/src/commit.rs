//! Bounded candidate checks; publication and ordered update evaluation are separate.

mod requirement;
mod transition;

pub use requirement::{validate_requirements, RequirementError, RequirementLimits, TableRequirement};
pub use transition::{validate_metadata_transition, TransitionLimits};
