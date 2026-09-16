// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use flatbuffers::FlatBufferBuilder;

use super::generated::crowdb::access::s_3::{
    FBBucketName, FBBucketNameArgs, FBBucketState, FBObject, FBObjectArgs,
};
use super::{BucketId, TenantId};

const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketNameRecord {
    pub tenant: TenantId,
    pub name: Vec<u8>,
    pub bucket_id: BucketId,
    pub tombstone: bool,
}

/// The complete value stored directly at one tenant/bucket/object key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRecord {
    pub bucket_id: BucketId,
    pub key: Vec<u8>,
    pub logical_length: u64,
    pub checksum: Vec<u8>,
    pub etag: String,
    pub created_at_ms: u64,
    pub modified_at_ms: u64,
    pub content_type: String,
    pub attributes: Vec<u8>,
    pub data_reference: Vec<u8>,
    pub data_length: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataRecordError {
    #[error("invalid S3 metadata flatbuffer")]
    Invalid,
    #[error("unsupported S3 metadata schema version {0}")]
    Version(u16),
    #[error("S3 metadata is missing {0}")]
    Missing(&'static str),
    #[error("S3 bucket ID must be 16 bytes")]
    BucketId,
    #[error("invalid S3 tenant identity")]
    Tenant,
    #[error("S3 metadata field {0} cannot be empty")]
    Empty(&'static str),
    #[error("S3 data reference length does not match logical length")]
    DataLength,
}

impl BucketNameRecord {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let tenant = builder.create_vector(self.tenant.as_bytes());
        let name = builder.create_vector(&self.name);
        let bucket_id = builder.create_vector(self.bucket_id.as_bytes());
        let value = FBBucketName::create(
            &mut builder,
            &FBBucketNameArgs {
                schema_version: SCHEMA_VERSION,
                tenant: Some(tenant),
                name: Some(name),
                bucket_id: Some(bucket_id),
                state: if self.tombstone {
                    FBBucketState::Tombstone
                } else {
                    FBBucketState::Active
                },
            },
        );
        builder.finish(value, None);
        builder.finished_data().to_vec()
    }

    /// Decodes and validates one persisted bucket mapping.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, unknown-version, or incomplete metadata.
    pub fn decode(bytes: &[u8]) -> Result<Self, MetadataRecordError> {
        let value = flatbuffers::root::<FBBucketName<'_>>(bytes).map_err(|_| MetadataRecordError::Invalid)?;
        require_version(value.schema_version())?;
        let tenant = value.tenant().ok_or(MetadataRecordError::Missing("tenant"))?;
        let name = value.name().ok_or(MetadataRecordError::Missing("name"))?;
        Ok(Self {
            tenant: TenantId::new(tenant.bytes().to_vec()).map_err(|_| MetadataRecordError::Tenant)?,
            name: name.bytes().to_vec(),
            bucket_id: bucket_id(value.bucket_id())?,
            tombstone: value.state() == FBBucketState::Tombstone,
        })
    }
}

impl ObjectRecord {
    /// Encodes one complete, validated object record.
    ///
    /// # Errors
    ///
    /// Returns an error when a required field is absent or lengths disagree.
    pub fn encode(&self) -> Result<Vec<u8>, MetadataRecordError> {
        validate_object(self)?;
        let mut builder = FlatBufferBuilder::new();
        let key = builder.create_vector(&self.key);
        let bucket_id = builder.create_vector(self.bucket_id.as_bytes());
        let checksum = builder.create_vector(&self.checksum);
        let etag = builder.create_string(&self.etag);
        let content_type = builder.create_string(&self.content_type);
        let attributes = builder.create_vector(&self.attributes);
        let data_reference = builder.create_vector(&self.data_reference);
        let value = FBObject::create(
            &mut builder,
            &FBObjectArgs {
                schema_version: SCHEMA_VERSION,
                bucket_id: Some(bucket_id),
                key: Some(key),
                logical_length: self.logical_length,
                checksum: Some(checksum),
                etag: Some(etag),
                created_at_ms: self.created_at_ms,
                modified_at_ms: self.modified_at_ms,
                content_type: Some(content_type),
                attributes: Some(attributes),
                data_reference: Some(data_reference),
                data_length: self.data_length,
            },
        );
        builder.finish(value, None);
        Ok(builder.finished_data().to_vec())
    }

    /// Decodes and validates one complete object record.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, unsupported, or inconsistent metadata.
    pub fn decode(bytes: &[u8]) -> Result<Self, MetadataRecordError> {
        let value = flatbuffers::root::<FBObject<'_>>(bytes).map_err(|_| MetadataRecordError::Invalid)?;
        require_version(value.schema_version())?;
        let record = Self {
            bucket_id: bucket_id(value.bucket_id())?,
            key: bytes_field(value.key(), "key")?,
            logical_length: value.logical_length(),
            checksum: bytes_field(value.checksum(), "checksum")?,
            etag: string_field(value.etag(), "etag")?,
            created_at_ms: value.created_at_ms(),
            modified_at_ms: value.modified_at_ms(),
            content_type: string_field(value.content_type(), "content_type")?,
            attributes: bytes_field(value.attributes(), "attributes")?,
            data_reference: bytes_field(value.data_reference(), "data_reference")?,
            data_length: value.data_length(),
        };
        validate_object(&record)?;
        Ok(record)
    }
}

fn require_version(version: u16) -> Result<(), MetadataRecordError> {
    if version != SCHEMA_VERSION {
        return Err(MetadataRecordError::Version(version));
    }
    Ok(())
}

fn validate_object(record: &ObjectRecord) -> Result<(), MetadataRecordError> {
    required("key", &record.key)?;
    required("checksum", &record.checksum)?;
    required("etag", record.etag.as_bytes())?;
    required("content_type", record.content_type.as_bytes())?;
    required("data_reference", &record.data_reference)?;
    if record.logical_length != record.data_length {
        return Err(MetadataRecordError::DataLength);
    }
    Ok(())
}

fn required(name: &'static str, value: &[u8]) -> Result<(), MetadataRecordError> {
    if value.is_empty() {
        return Err(MetadataRecordError::Empty(name));
    }
    Ok(())
}

fn bytes_field(
    value: Option<flatbuffers::Vector<'_, u8>>,
    name: &'static str,
) -> Result<Vec<u8>, MetadataRecordError> {
    value
        .map(|value| value.bytes().to_vec())
        .ok_or(MetadataRecordError::Missing(name))
}

fn string_field(value: Option<&str>, name: &'static str) -> Result<String, MetadataRecordError> {
    value.map(str::to_owned).ok_or(MetadataRecordError::Missing(name))
}

fn bucket_id(value: Option<flatbuffers::Vector<'_, u8>>) -> Result<BucketId, MetadataRecordError> {
    let value = value.ok_or(MetadataRecordError::Missing("bucket_id"))?;
    let bytes: [u8; 16] = value
        .bytes()
        .try_into()
        .map_err(|_| MetadataRecordError::BucketId)?;
    Ok(BucketId::new(bytes))
}
