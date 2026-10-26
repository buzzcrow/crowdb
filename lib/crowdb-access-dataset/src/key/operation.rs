// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{AuthorityError, DatasetIdentity, NamespacePath, OperationId};

pub(crate) fn decode_scoped_operation(
    key: &[u8],
    prefix: &[u8],
) -> Result<(DatasetIdentity, OperationId), AuthorityError> {
    let mut bytes = key.strip_prefix(prefix).ok_or(AuthorityError::Corrupt)?;
    let mut namespace = Vec::new();
    while bytes.first() != Some(&255) {
        namespace.push(component(&mut bytes)?);
    }
    bytes = bytes.get(1..).ok_or(AuthorityError::Corrupt)?;
    let name = component(&mut bytes)?;
    let operation = OperationId::from_bytes(bytes)?;
    let identity = DatasetIdentity::new(NamespacePath::new(namespace)?, name)?;
    let expected = [prefix, &identity.key()?, operation.as_bytes()].concat();
    if expected != key {
        return Err(AuthorityError::Corrupt);
    }
    Ok((identity, operation))
}
fn component(bytes: &mut &[u8]) -> Result<String, AuthorityError> {
    let prefix: [u8; 2] = bytes
        .get(..2)
        .ok_or(AuthorityError::Corrupt)?
        .try_into()
        .map_err(|_| AuthorityError::Corrupt)?;
    let end = 2 + usize::from(u16::from_be_bytes(prefix));
    let value = std::str::from_utf8(bytes.get(2..end).ok_or(AuthorityError::Corrupt)?)
        .map_err(|_| AuthorityError::Corrupt)?
        .to_owned();
    *bytes = bytes.get(end..).ok_or(AuthorityError::Corrupt)?;
    Ok(value)
}
