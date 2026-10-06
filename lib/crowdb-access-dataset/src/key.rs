use crate::{DatasetError, DatasetIdentity, OperationId, SnapshotId};

pub const DATASET_PREFIX: &[u8] = b"DS\0D\0";
pub const SNAPSHOT_PREFIX: &[u8] = b"DS\0S\0";
pub const HEAD_PREFIX: &[u8] = b"DS\0H\0";
pub const OPERATION_PREFIX: &[u8] = b"DS\0O\0";
pub const MANIFEST_PREFIX: &[u8] = b"DS\0M\0";
pub const MANIFEST_HEAD_PREFIX: &[u8] = b"DS\0MH\0";
pub const CURSOR_PREFIX: &[u8] = b"DS\0C\0";
pub const RETENTION_PREFIX: &[u8] = b"DS\0R\0";
pub const LEASE_PREFIX: &[u8] = b"DS\0L\0";
pub const RECLAIM_PREFIX: &[u8] = b"DS\0G\0";

pub fn dataset(identity: &DatasetIdentity) -> Result<Vec<u8>, DatasetError> {
    let mut key = DATASET_PREFIX.to_vec();
    key.extend_from_slice(&identity.key()?);
    Ok(key)
}

pub fn snapshot(identity: &DatasetIdentity, id: SnapshotId) -> Result<Vec<u8>, DatasetError> {
    let mut key = SNAPSHOT_PREFIX.to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(id.as_bytes());
    Ok(key)
}

pub fn head(identity: &DatasetIdentity) -> Result<Vec<u8>, DatasetError> {
    let mut key = HEAD_PREFIX.to_vec();
    key.extend_from_slice(&identity.key()?);
    Ok(key)
}

pub fn operation(identity: &DatasetIdentity, operation: OperationId) -> Result<Vec<u8>, DatasetError> {
    let mut key = OPERATION_PREFIX.to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(operation.as_bytes());
    Ok(key)
}

/// Builds the stable key for one snapshot manifest partition.
///
/// # Errors
/// Returns an identity validation error when the Dataset identity is invalid.
pub fn manifest_partition(
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    partition: u32,
) -> Result<Vec<u8>, DatasetError> {
    let mut key = MANIFEST_PREFIX.to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(snapshot.as_bytes());
    key.extend_from_slice(&partition.to_be_bytes());
    Ok(key)
}

/// Builds the key holding the partition count for one snapshot manifest.
pub fn manifest_binding(identity: &DatasetIdentity, snapshot: SnapshotId) -> Result<Vec<u8>, DatasetError> {
    let mut key = MANIFEST_HEAD_PREFIX.to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(snapshot.as_bytes());
    Ok(key)
}

pub fn cursor(identity: &DatasetIdentity, plan_identity: [u8; 16]) -> Result<Vec<u8>, DatasetError> {
    let mut key = CURSOR_PREFIX.to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(&plan_identity);
    Ok(key)
}

pub fn retention(identity: &DatasetIdentity, id: SnapshotId) -> Result<Vec<u8>, DatasetError> {
    let mut key = RETENTION_PREFIX.to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(id.as_bytes());
    Ok(key)
}

pub fn read_lease(identity: &DatasetIdentity, id: SnapshotId) -> Result<Vec<u8>, DatasetError> {
    let mut key = LEASE_PREFIX.to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(id.as_bytes());
    Ok(key)
}

pub fn reclaim_progress(identity: &DatasetIdentity, id: SnapshotId) -> Result<Vec<u8>, DatasetError> {
    let mut key = RECLAIM_PREFIX.to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(id.as_bytes());
    Ok(key)
}
