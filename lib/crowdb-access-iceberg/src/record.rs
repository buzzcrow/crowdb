//! Bounded, versioned `FlatBuffer` storage records, separate from REST models.

mod authority;
mod envelope;
mod root;

pub use envelope::{StorageRecord, MAX_RECORD_BYTES};
