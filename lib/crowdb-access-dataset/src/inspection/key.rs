// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{DatasetError, DatasetIdentity, SnapshotId};

pub(super) fn scope(
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    kind: u8,
) -> Result<Vec<u8>, DatasetError> {
    let mut key = b"DS\0I\0".to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(snapshot.as_bytes());
    key.push(kind);
    Ok(key)
}

// Ordered, delimiter-safe binary component. Zero bytes never alias separators.
pub(super) fn component(key: &mut Vec<u8>, bytes: &[u8]) {
    for byte in bytes {
        if *byte == 0 {
            key.extend_from_slice(&[0, 255]);
        } else {
            key.push(*byte);
        }
    }
    key.extend_from_slice(&[0, 0]);
}

pub(super) fn inventory(identity: &DatasetIdentity, snapshot: SnapshotId) -> Result<Vec<u8>, DatasetError> {
    let mut key = b"DS\0II\0".to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(snapshot.as_bytes());
    Ok(key)
}

pub(super) fn child(
    identity: &DatasetIdentity,
    parent: Option<SnapshotId>,
    snapshot: SnapshotId,
) -> Result<Vec<u8>, DatasetError> {
    let mut key = b"DS\0IC\0".to_vec();
    key.extend_from_slice(&identity.key()?);
    key.push(u8::from(parent.is_some()));
    if let Some(parent) = parent {
        key.extend_from_slice(parent.as_bytes());
    }
    key.extend_from_slice(snapshot.as_bytes());
    Ok(key)
}
