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
