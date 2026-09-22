//! Native immutable file identity and storage, independent of general S3 metadata.

mod content;
mod key;
mod location;
mod record;
mod repository;

pub use content::{ChunkRoot, FileContent, InlineCodec, MAX_COMPRESSION_INPUT_BYTES, MAX_INLINE_BYTES};
pub use key::{file_key, location_key};
pub(crate) use location::validate_relative_key;
pub use location::{FileLocation, TableLocation, MAX_OBJECT_KEY_BYTES};
pub use record::{ContentFormat, FileKind, FileMapping, FileRecord, FormatHint};
pub use repository::FileRepository;
