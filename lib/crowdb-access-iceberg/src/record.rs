//! Bounded, versioned `FlatBuffer` storage records, separate from REST models.

mod authority;
mod envelope;
mod file;
mod management;
mod multipart;
mod multipart_admission;
mod namespace;
mod namespace_operation;
mod payload;
mod retry;
mod root;
mod table;

pub use envelope::{StorageRecord, MAX_RECORD_BYTES};
