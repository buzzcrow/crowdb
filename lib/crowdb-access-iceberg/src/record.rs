//! Bounded, versioned `FlatBuffer` storage records, separate from REST models.

mod authority;
mod envelope;
mod file;
mod management;
mod multipart;
mod namespace;
mod namespace_operation;
mod payload;
mod retry;
mod root;

pub use envelope::{StorageRecord, MAX_RECORD_BYTES};
