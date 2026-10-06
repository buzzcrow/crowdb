//! Dataset authority primitives shared by native and HTTP access.
//!
//! The crate deliberately keeps the core identity and publication model
//! independent from the Access Server listener. Physical payload IO is added by
//! the authority and chunk adapters, not by the HTTP process entrypoint.

mod adapter;
mod authority;
mod chunk_store;
mod client;
mod cursor;
mod delivery;
mod error;
mod identity;
mod key;
mod lease;
mod manifest;
mod namespace;
mod planner;
mod publication;
mod record;
mod retention;
mod shuffle;
mod store;
mod surface;
mod transport;
mod wire;

pub use cursor::ReadCursor;
pub use delivery::DeliveryWindow;
pub use error::DatasetError;
pub use identity::{DatasetId, OperationId, SnapshotId};
pub use key::manifest_partition;
pub use lease::{ReadLease, DEFAULT_READ_LEASE_SECONDS};
pub use manifest::{
    FieldDefinition, FieldLocator, FieldRecord, ManifestPartition, ManifestRecord, SampleRecord, SchemaRecord,
};
pub use namespace::{DatasetIdentity, NamespacePath, DEFAULT_NAMESPACE};
pub use planner::{Ordering, ReadFailure, ReadLimits, ReadPlan, RetryPolicy, Selection, MAX_BATCH_SIZE};
pub use publication::{PublicationState, SnapshotPublication};
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
pub use authority::{AuthorityError, DatasetAuthority, SampleView};
pub use chunk_store::{
    unavailable_chunk_reader, ChunkKvDatasetStore, ChunkReadError, ChunkReader, ReadCancellation,
};
pub use client::{DatasetClientError, DatasetDirectClient, DatasetHttpClient};
pub use record::{
    ActiveReadLease, DatasetRecord, HeadRecord, ManifestBinding, ReclaimProgress, ReclaimState,
    SnapshotRecord,
};
pub use retention::{
    RetentionLease, RetentionLeaseHandle, RetentionLeaseRegistry, RetentionPlan, RetentionPlanner,
};
pub use store::{CasOutcome, DatasetStore, StoreError, StoredValue};
