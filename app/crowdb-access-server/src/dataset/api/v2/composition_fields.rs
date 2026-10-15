// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{operation, ApiError},
    query::bytes,
};
use crowdb_access_dataset::{
    composition, DatasetIdentity, DatasetUploads, FieldLocator, FieldRecord, SampleRecord, UploadState,
};
use md5::{Digest, Md5};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum SampleChange {
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
    Upload { id: String },
    Tombstone,
}
impl SampleChange {
    pub(super) async fn native(
        self,
        identity: &DatasetIdentity,
        uploads: &DatasetUploads,
    ) -> Result<composition::SampleChange, ApiError> {
        match self {
            Self::Remove { id } => Ok(composition::SampleChange::Remove(bytes(&id, 16384, false)?)),
            Self::Upsert { id, fields } => {
                if fields.len() > 256 {
                    return Err(ApiError::too_large());
                }
                let sample_id = bytes(&id, 16384, false)?;
                let mut values = Vec::new();
                for field in fields {
                    values.push(field.native(identity, uploads).await?);
                }
                Ok(composition::SampleChange::Upsert(SampleRecord {
                    sample_id,
                    fields: values,
                }))
            }
        }
    }
}
impl Field {
    async fn native(
        self,
        identity: &DatasetIdentity,
        uploads: &DatasetUploads,
    ) -> Result<FieldRecord, ApiError> {
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
            Value::Upload { id } => {
                let location = DatasetUploads::location(identity, operation(&id)?)?;
                let record = uploads.metadata(&location).await?;
                if !matches!(record.state, UploadState::Complete | UploadState::Claimed) {
                    return Err(ApiError::conflict());
                }
                FieldLocator::Chunk {
                    location,
                    length: record.length,
                    md5: record.md5,
                }
            }
        };
        Ok(FieldRecord {
            name: self.name,
            value,
        })
    }
}
