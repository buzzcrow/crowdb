//! Native immutable file identity and storage, independent of general S3 metadata.

mod assembly;
mod avro;
mod blocks;
mod content;
mod credentials;
mod digest;
mod directory;
mod format;
mod json;
mod key;
mod location;
mod multipart;
mod multipart_admission;
mod multipart_credits;
mod multipart_list;
mod multipart_recovery;
mod multipart_repository;
mod multipart_selection;
mod puffin;
mod range;
mod reader;
mod record;
mod repository;
mod writer;

pub use assembly::{AssemblyPart, AssemblyProgress, FileAssembly, PartFingerprint};
pub use avro::{
    AvroBlock, AvroBlocks, AvroCodec, AvroContainerError, AvroDatumLimits, AvroDecodedBlock, AvroLimits,
    AvroRecords, AvroSchema,
};
pub use blocks::{FileBlockStore, FileIoError, NativeFileBlocks, MAX_FILE_BLOCK_BYTES};
pub use content::{ChunkRoot, FileContent, InlineCodec, MAX_COMPRESSION_INPUT_BYTES, MAX_INLINE_BYTES};
pub use credentials::{
    FileCredentials, FileGrant, FileGrantError, FileGrantIssuer, FileOperation, FileOperations,
};
pub use digest::FileDigest;
pub use directory::{ChunkDirectory, ChunkEntry, FileIdentity, MAX_DIRECTORY_ENTRIES};
pub use format::{
    probe_orc_footer, probe_parquet_footer, probe_puffin_footer, FormatProbeError, OrcFooter, PuffinFooter,
};
pub use json::{JsonSealError, JsonSealer};
pub use key::{file_key, location_key};
pub(crate) use location::validate_relative_key;
pub use location::{FileLocation, TableLocation, MAX_OBJECT_KEY_BYTES};
pub use multipart::{
    MultipartCompletion, MultipartLimits, MultipartPart, MultipartPartMutation, MultipartPhase,
    MultipartSession,
};
pub use multipart_admission::{
    MultipartAdmissionLimits, MultipartAdmissionRecord, MultipartCredit, MultipartCreditAction,
    MultipartCreditMutation,
};
pub use multipart_credits::MultipartAdmission;
pub use multipart_list::{MultipartLister, MultipartPartPage, MultipartPartScan, MultipartPartStore};
pub use multipart_recovery::{
    MultipartRecovery, MultipartRecoveryPage, MultipartRecoveryScan, MultipartRecoveryStore,
};
pub use multipart_repository::{MultipartRepository, MultipartWorkError};
pub use multipart_selection::{MultipartSelection, SelectedPart};
pub use puffin::{read_puffin_metadata, PuffinBlob, PuffinMetadata, PuffinMetadataError};
pub use range::{resolve_range, ByteRange, RangeError};
pub use reader::{FileReader, MAX_READ_FRAME_BYTES};
pub use record::{ContentFormat, FileKind, FileMapping, FileRecord, FormatHint};
pub use repository::FileRepository;
pub use writer::{FileTree, FileTreeWriter, FileWriterCheckpoint};
