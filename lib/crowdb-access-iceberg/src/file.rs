//! Native immutable file identity and storage, independent of general S3 metadata.

mod location;

pub use location::{FileLocation, TableLocation, MAX_OBJECT_KEY_BYTES};
