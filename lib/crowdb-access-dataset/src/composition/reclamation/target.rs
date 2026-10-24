// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::CompositionError;
use crate::{AuthorityError, DatasetIdentity, NamespacePath, OperationId};

pub(super) fn decode(key: &[u8]) -> Result<(DatasetIdentity, OperationId), CompositionError> {
    let mut bytes = key.strip_prefix(b"DS\0CD\0").ok_or(CompositionError::Corrupt)?;
    let mut namespace = Vec::new();
    while bytes.first() != Some(&255) {
        namespace.push(component(&mut bytes)?);
    }
    bytes = bytes.get(1..).ok_or(CompositionError::Corrupt)?;
    let name = component(&mut bytes)?;
    let operation = OperationId::from_bytes(bytes).map_err(AuthorityError::from)?;
    let identity = DatasetIdentity::new(NamespacePath::new(namespace).map_err(AuthorityError::from)?, name)
        .map_err(AuthorityError::from)?;
    if super::super::key::scope(b"DS\0CD\0", &identity, operation)? != key {
        return Err(CompositionError::Corrupt);
    }
    Ok((identity, operation))
}
fn component(bytes: &mut &[u8]) -> Result<String, CompositionError> {
    let prefix: [u8; 2] = bytes
        .get(..2)
        .ok_or(CompositionError::Corrupt)?
        .try_into()
        .map_err(|_| CompositionError::Corrupt)?;
    let end = 2 + usize::from(u16::from_be_bytes(prefix));
    let value = std::str::from_utf8(bytes.get(2..end).ok_or(CompositionError::Corrupt)?)
        .map_err(|_| CompositionError::Corrupt)?
        .to_owned();
    *bytes = bytes.get(end..).ok_or(CompositionError::Corrupt)?;
    Ok(value)
}
