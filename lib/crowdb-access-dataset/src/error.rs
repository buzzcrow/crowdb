use thiserror::Error;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DatasetError {
    #[error("dataset name is empty or contains a reserved separator")]
    InvalidName,
    #[error("namespace has no segments")]
    EmptyNamespace,
    #[error("dataset must use the default dataset-ns namespace")]
    InvalidDefaultNamespace,
    #[error("namespace segment is invalid")]
    InvalidNamespaceSegment,
    #[error("identity is zero")]
    ZeroIdentity,
    #[error("identity has invalid byte length")]
    IdentityLength,
    #[error("snapshot parent is the snapshot itself")]
    SelfParent,
    #[error("manifest reference is empty")]
    EmptyManifest,
    #[error("manifest record is invalid")]
    InvalidManifest,
    #[error("inline field value exceeds the bounded small-value limit")]
    InlineValueTooLarge,
    #[error("publication state is not valid for this transition")]
    InvalidPublicationState,
    #[error("field payload checksum does not match its locator")]
    ChecksumMismatch,
    #[error("field payload length does not match its locator")]
    FieldLengthMismatch,
    #[error("requested sample is not a member of the snapshot")]
    SampleNotFound,
    #[error("payload was truncated")]
    PayloadTruncated,
    #[error("payload read failed transiently")]
    PayloadTransient,
    #[error("payload was not found")]
    PayloadNotFound,
    #[error("manifest partition size must be non-zero")]
    InvalidPartitionSize,
    #[error("read plan exceeds the configured delivery limits")]
    ReadLimitExceeded,
    #[error("dataset read was cancelled")]
    ReadCancelled,
    #[error("the requested grouping key has no supported index")]
    UnsupportedGrouping,
    #[error("read cursor does not match the requested snapshot or plan")]
    CursorMismatch,
    #[error("shuffle output does not match the selected sample set")]
    ShuffleIntegrity,
}
