//! Dataset authority primitives shared by native and HTTP access.
//!
//! The crate deliberately keeps the core identity and publication model
//! independent from the Access Server listener. Physical payload IO is added by
//! the authority and chunk adapters, not by the HTTP process entrypoint.

mod adapter;
mod authority;
mod chunk_store;
mod client;
pub mod composition;
mod cursor;
mod delivery;
mod error;
mod identity;
pub mod inspection;
mod key;
mod manifest;
mod namespace;
mod payload;
#[cfg(feature = "test-util")]
pub use payload::collect_payload_for_tests;
mod planner;
mod publication;
mod record;
mod retention;
mod selection;
mod shuffle;
pub mod split;
mod store;
mod surface;
mod transport;
mod upload;
mod wire;

pub use payload::{
    ChunkPayloadStorage, PayloadDurability, PayloadStorage, PayloadStream, UploadedChunkReader,
};
pub use upload::{
    DatasetUploads, SnapshotUploadCleanup, UploadError, UploadExpiryCursor, UploadExpiryStep, UploadPart,
    UploadReclaimError, UploadRecord, UploadState, MAX_UPLOAD_PART_BYTES,
};
pub use upload::{UploadReadDescriptor, VerifiedUploadPart};

pub use cursor::ReadCursor;
pub use delivery::DeliveryWindow;
pub use error::DatasetError;
pub use identity::{DatasetId, OperationId, SnapshotId, SplitRevisionId};
pub use key::manifest_partition;
pub use manifest::{
    FieldDefinition, FieldLocator, FieldRecord, ManifestPartition, ManifestRecord, SampleRecord, SchemaRecord,
};
pub use namespace::{DatasetIdentity, NamespacePath, DEFAULT_NAMESPACE};
pub use planner::{Ordering, ReadFailure, ReadLimits, ReadPlan, RetryPolicy, Selection, MAX_BATCH_SIZE};
pub use publication::{
    CommitAttempt, CommitGeneration, CommitTarget, PublicationState, SnapshotPublication, SuccessfulCommit,
};
pub use shuffle::{
    GroupShuffle, SampleShuffle, ShuffleSpec, DEFAULT_MAX_SHUFFLE_BYTES, DEFAULT_MAX_SHUFFLE_OBJECTS,
};
pub use surface::ReadSurface;
pub use transport::{DatasetReadService, ReadTransportError};
pub use wire::{
    DatasetManifestResponse, DatasetPublishRequest, DatasetPublishResponse, DatasetReadRequest,
    DatasetReadResponse, DatasetReclaimResponse, DatasetRetentionResponse, DatasetScanRequest,
    DatasetScanResponse, DatasetSnapshotRequest, DatasetSnapshotResponse, DatasetSnapshotsResponse,
};

pub use adapter::{BoundedPrefetch, WorkerPartition};
pub use authority::{
    AuthorityError, DatasetAuthority, DatasetHead, MetadataCleanupProgress, SampleView, SnapshotLifecycle,
    SnapshotReclaimError, SnapshotReclaimPhase, SnapshotReclaimStatus,
};
pub use chunk_store::{
    unavailable_chunk_reader, ChunkKvDatasetStore, ChunkReadError, ChunkReader, ReadCancellation,
};
pub use client::{DatasetClientError, DatasetDirectClient, DatasetHttpClient};
pub use record::{
    ActiveReadLease, DatasetRecord, HeadRecord, ManifestBinding, ReclaimProgress, ReclaimState,
    SnapshotRecord,
};
pub use retention::{RetentionPlan, RetentionPlanner};
pub use store::{
    CasOutcome, DatasetStore, MetadataPage, MetadataScan, MetadataScanBudget, StoreError, StoredValue,
};

mod read_query;
pub use read_query::{
    DatasetReads, ReadBinding, ReadPage, ReadPartition, ReadShuffle, ReadSpec, ReadSplit, ResolvedReadPage,
};
