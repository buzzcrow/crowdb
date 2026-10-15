// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Length-delimited opaque Split names cannot alias another page or revision.

use super::SplitName;
use crate::{DatasetError, DatasetIdentity, SnapshotId, SplitRevisionId};

/// # Errors
/// Rejects invalid Dataset identities.
pub fn head(identity: &DatasetIdentity, snapshot: SnapshotId) -> Result<Vec<u8>, DatasetError> {
    scoped(b"DS\0SH\0", identity, snapshot)
}

/// # Errors
/// Rejects invalid Dataset identities.
pub fn revision(
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    revision: SplitRevisionId,
) -> Result<Vec<u8>, DatasetError> {
    let mut key = scoped(b"DS\0SR\0", identity, snapshot)?;
    key.extend_from_slice(revision.as_bytes());
    Ok(key)
}

/// # Errors
/// Rejects invalid Dataset identities.
pub fn membership_page(
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    revision: SplitRevisionId,
    name: &SplitName,
    page: u32,
) -> Result<Vec<u8>, DatasetError> {
    let mut key = scoped(b"DS\0SM\0", identity, snapshot)?;
    key.extend_from_slice(revision.as_bytes());
    key.push(u8::try_from(name.as_str().len()).map_err(|_| DatasetError::InvalidName)?);
    key.extend_from_slice(name.as_str().as_bytes());
    key.extend_from_slice(&page.to_be_bytes());
    Ok(key)
}

fn scoped(prefix: &[u8], identity: &DatasetIdentity, snapshot: SnapshotId) -> Result<Vec<u8>, DatasetError> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(snapshot.as_bytes());
    Ok(key)
}

/// Durable proof that a complete revision was published.
/// # Errors
/// Rejects invalid Dataset identities.
pub fn committed_revision(
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    revision: SplitRevisionId,
) -> Result<Vec<u8>, DatasetError> {
    let mut key = scoped(b"DS\0SRC\0", identity, snapshot)?;
    key.extend_from_slice(revision.as_bytes());
    Ok(key)
}

/// Small discovery index written before publication and filtered by commit proof.
/// # Errors
/// Rejects invalid Dataset identities.
pub fn revision_summary(
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    revision: SplitRevisionId,
) -> Result<Vec<u8>, DatasetError> {
    let mut key = scoped(b"DS\0SRI\0", identity, snapshot)?;
    key.extend_from_slice(revision.as_bytes());
    Ok(key)
}

pub(crate) fn membership_prefix(
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    revision: SplitRevisionId,
) -> Result<Vec<u8>, DatasetError> {
    let mut key = scoped(b"DS\0SM\0", identity, snapshot)?;
    key.extend_from_slice(revision.as_bytes());
    Ok(key)
}
