//! Native immutable file identity and storage, independent of general S3 metadata.

mod blocks;
mod content;
mod credentials;
mod directory;
mod format;
mod json;
mod key;
mod location;
mod range;
mod reader;
mod record;
mod repository;
mod writer;

pub use blocks::{FileBlockStore, FileIoError, NativeFileBlocks, MAX_FILE_BLOCK_BYTES};
pub use content::{ChunkRoot, FileContent, InlineCodec, MAX_COMPRESSION_INPUT_BYTES, MAX_INLINE_BYTES};
pub use credentials::{
    FileCredentials, FileGrant, FileGrantError, FileGrantIssuer, FileOperation, FileOperations,
};
pub use directory::{ChunkDirectory, ChunkEntry, FileIdentity, MAX_DIRECTORY_ENTRIES};
pub use format::{
    probe_orc_footer, probe_parquet_footer, probe_puffin_footer, FormatProbeError, OrcFooter, PuffinFooter,
};
pub use json::{JsonSealError, JsonSealer};
pub use key::{file_key, location_key};
pub(crate) use location::validate_relative_key;
pub use location::{FileLocation, TableLocation, MAX_OBJECT_KEY_BYTES};
pub use range::{resolve_range, ByteRange, RangeError};
pub use reader::{FileReader, MAX_READ_FRAME_BYTES};
pub use record::{ContentFormat, FileKind, FileMapping, FileRecord, FormatHint};
pub use repository::FileRepository;
pub use writer::{FileTree, FileTreeWriter};
