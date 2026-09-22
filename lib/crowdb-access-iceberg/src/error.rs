//! Fail-closed validation errors shared by Iceberg storage codecs.

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ValidationError {
    #[error("identity must be 16 bytes and must not be all zero")]
    Identity,
    #[error("unsupported Iceberg key version {0}")]
    KeyVersion(u8),
    #[error("malformed Iceberg key")]
    Key,
    #[error("Iceberg key exceeds its byte limit")]
    KeyTooLarge,
    #[error("invalid or oversized text field")]
    Text,
    #[error("unsupported Iceberg record version {0}")]
    RecordVersion(u16),
    #[error("malformed Iceberg record")]
    Record,
    #[error("Iceberg record exceeds its byte limit")]
    RecordTooLarge,
    #[error("Iceberg identity does not match its storage key")]
    IdentityMismatch,
    #[error("Iceberg generation exhausted")]
    GenerationExhausted,
    #[error("invalid Iceberg capability matrix")]
    Capabilities,
    #[error("invalid Iceberg deadline configuration")]
    Deadline,
    #[error("namespace property removals and updates overlap")]
    PropertyOverlap,
}
