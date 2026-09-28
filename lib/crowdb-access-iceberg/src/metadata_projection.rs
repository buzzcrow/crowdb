//! Disposable, generation-bound metadata children with canonical streaming fallback.

mod model;
mod repository;
mod validated;

use model::ProjectionIdentity;
pub use model::{MAX_PROJECTION_BYTES, PROJECTION_PAGE_BYTES, PROJECTION_VERSION};
pub use repository::{MetadataRead, ProjectionStore};
