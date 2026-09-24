//! Native immutable file identity and storage, independent of general S3 metadata.

mod assembly;
mod avro;
mod blocks;
mod content;
mod credentials;
mod deletion_vector;
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
mod parquet;
pub use parquet::ParquetPageLimits;
pub(crate) use parquet::{ParquetColumnReader, ParquetColumnValue};
mod puffin;
mod range;
mod reader;
mod record;
mod repository;
mod seal;
mod writer;

pub use assembly::{AssemblyPart, AssemblyProgress, FileAssembly, PartFingerprint};
pub use avro::{
    AvroBlock, AvroBlocks, AvroCodec, AvroContainerError, AvroDatumLimits, AvroDecodedBlock, AvroFieldPath,
    AvroIntList, AvroLimits, AvroMetricMap, AvroMetricValue, AvroProjectedRecords, AvroProjection,
    AvroRecordArray, AvroRecords, AvroScalar, AvroScalarType, AvroSchema, AvroTuple, AvroTupleField,
};
pub use blocks::{
    FileBlockStore, FileIoError, NativeFileBlocks, MAX_FILE_BLOCK_BYTES, NATIVE_FILE_BLOCK_BYTES,
};
pub use content::{ChunkRoot, FileContent, InlineCodec, MAX_COMPRESSION_INPUT_BYTES, MAX_INLINE_BYTES};
pub use credentials::{
    FileCredentials, FileGrant, FileGrantError, FileGrantIssuer, FileOperation, FileOperations,
};
pub use deletion_vector::{
    read_deletion_vector_positions, validate_deletion_vector, DeletionVectorError, DeletionVectorLimits,
    DeletionVectorPositions, DeletionVectorReference, DeletionVectorStats,
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
#[cfg(feature = "test-util")]
pub use parquet::{
    read_parquet_integer_column_for_tests, read_parquet_nullable_integer_column_for_tests,
    read_parquet_scalar_column_for_tests,
};
pub use parquet::{
    read_parquet_metadata, ParquetColumnChunk, ParquetLogicalType, ParquetMetadata, ParquetMetadataError,
    ParquetMetadataLimits, ParquetRowGroup, ParquetSchemaElement, ParquetTimeUnit,
};
pub use puffin::{read_puffin_metadata, PuffinBlob, PuffinMetadata, PuffinMetadataError};
pub use range::{resolve_range, ByteRange, RangeError};
pub use reader::{FileReader, MAX_READ_FRAME_BYTES};
pub use record::{ContentFormat, FileKind, FileMapping, FileRecord, FormatHint};
pub use repository::FileRepository;
pub use seal::{FileSealError, FileSealer};
pub use writer::{FileTree, FileTreeWriter, FileWriterCheckpoint};
