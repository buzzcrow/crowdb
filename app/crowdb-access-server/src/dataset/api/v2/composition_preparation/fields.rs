// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{
    super::{operation, ApiError},
    query::bytes,
    split_models::decimal,
    uploads::digest,
};
use crowdb_access_dataset::{
    composition::SampleChange, DatasetIdentity, DatasetUploads, FieldLocator, FieldRecord, SampleRecord,
};
use md5::{Digest, Md5};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Change {
    Remove { id: String },
    Upsert { id: String, fields: Vec<Field> },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Field {
    name: String,
    value: Value,
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Value {
    Inline { data: String },
    Upload { id: String, length: String, md5: String },
    Tombstone,
}

impl Change {
    pub(super) fn native(self, identity: &DatasetIdentity) -> Result<SampleChange, ApiError> {
        match self {
            Self::Remove { id } => Ok(SampleChange::Remove(bytes(&id, 16384, false)?)),
            Self::Upsert { id, fields } => {
                if fields.len() > 256 {
                    return Err(ApiError::too_large());
                }
                Ok(SampleChange::Upsert(SampleRecord {
                    sample_id: bytes(&id, 16384, false)?,
                    fields: fields
                        .into_iter()
                        .map(|field| field.native(identity))
                        .collect::<Result<_, _>>()?,
                }))
            }
        }
    }
}
impl Field {
    fn native(self, identity: &DatasetIdentity) -> Result<FieldRecord, ApiError> {
        if self.name.is_empty() || self.name.len() > 1024 {
            return Err(ApiError::invalid());
        }
        let value = match self.value {
            Value::Inline { data } => {
                let value = bytes(&data, 65536, true)?;
                FieldLocator::Inline {
                    md5: Md5::digest(&value).into(),
                    value,
                }
            }
            Value::Tombstone => FieldLocator::Tombstone,
            Value::Upload { id, length, md5 } => FieldLocator::Chunk {
                location: DatasetUploads::location(identity, operation(&id)?)?,
                length: decimal(&length)?,
                md5: digest(&md5)?,
            },
        };
        Ok(FieldRecord {
            name: self.name,
            value,
        })
    }
}
