// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, FieldMetadata};
use crate::{AuthorityError, DatasetIdentity, ManifestRecord};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IndexPosition {
    schema: usize,
    sample: usize,
    sample_written: bool,
    field: usize,
    field_value: bool,
    pub rows: u64,
}

impl IndexPosition {
    pub(super) fn validate(&self, manifest: &ManifestRecord) -> Result<(), AuthorityError> {
        let schema_len = manifest.schema.fields.len();
        if self.schema > schema_len || self.sample > manifest.samples.len() {
            return Err(AuthorityError::Corrupt);
        }
        if self.schema < schema_len
            && (self.sample != 0 || self.sample_written || self.field != 0 || self.field_value)
        {
            return Err(AuthorityError::Corrupt);
        }
        let mut expected = self.schema as u64;
        for sample in manifest.samples.iter().take(self.sample) {
            expected += 1 + 2 * sample.fields.len() as u64;
        }
        if let Some(sample) = manifest.samples.get(self.sample) {
            if self.field > sample.fields.len()
                || (!self.sample_written && (self.field != 0 || self.field_value))
                || (self.field == sample.fields.len() && self.field_value)
            {
                return Err(AuthorityError::Corrupt);
            }
            expected += u64::from(self.sample_written) + 2 * self.field as u64 + u64::from(self.field_value);
        } else if self.sample_written || self.field != 0 || self.field_value {
            return Err(AuthorityError::Corrupt);
        }
        if self.rows != expected {
            return Err(AuthorityError::Corrupt);
        }
        Ok(())
    }
}

pub(super) struct EncodedRow {
    pub key: Vec<u8>,
    pub bytes: Vec<u8>,
}

pub(super) fn next_row(
    identity: &DatasetIdentity,
    manifest: &ManifestRecord,
    position: &mut IndexPosition,
) -> Result<Option<EncodedRow>, AuthorityError> {
    let row = if let Some(field) = manifest.schema.fields.get(position.schema) {
        let mut key = key::scope(identity, manifest.snapshot, b'S')?;
        key.extend_from_slice(
            &u32::try_from(position.schema)
                .map_err(|_| AuthorityError::Corrupt)?
                .to_be_bytes(),
        );
        position.schema += 1;
        encode(key, field)?
    } else {
        loop {
            let Some(sample) = manifest.samples.get(position.sample) else {
                return Ok(None);
            };
            if !position.sample_written {
                let mut key = key::scope(identity, manifest.snapshot, b'M')?;
                key::component(&mut key, &sample.sample_id);
                position.sample_written = true;
                break encode(key, &sample.sample_id)?;
            }
            if let Some(field) = sample.fields.get(position.field) {
                let mut key = key::scope(
                    identity,
                    manifest.snapshot,
                    if position.field_value { b'V' } else { b'F' },
                )?;
                key::component(&mut key, &sample.sample_id);
                key::component(&mut key, field.name.as_bytes());
                let row = if position.field_value {
                    encode(key, &field.value)?
                } else {
                    encode(
                        key,
                        &FieldMetadata::new(&field.name, &field.value, manifest.snapshot),
                    )?
                };
                if position.field_value {
                    position.field += 1;
                }
                position.field_value = !position.field_value;
                break row;
            }
            position.sample += 1;
            position.sample_written = false;
            position.field = 0;
        }
    };
    position.rows = position.rows.checked_add(1).ok_or(AuthorityError::Corrupt)?;
    Ok(Some(row))
}

pub(super) fn encode(key: Vec<u8>, value: &impl serde::Serialize) -> Result<EncodedRow, AuthorityError> {
    let bytes = serde_json::to_vec(value).map_err(|_| AuthorityError::Corrupt)?;
    if key.len() >= 64 * 1024 || bytes.len() > 1024 * 1024 {
        return Err(AuthorityError::Corrupt);
    }
    Ok(EncodedRow { key, bytes })
}
