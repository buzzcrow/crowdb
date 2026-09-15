// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fmt;

const BUCKET_NAME_KIND: u8 = 1;
const OBJECT_KIND: u8 = 2;
const MAX_KEY_BYTES: usize = 1024;

/// A tenant namespace identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TenantId(Vec<u8>);

impl TenantId {
    /// Creates a nonempty tenant identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the identity is empty or exceeds the metadata
    /// component bound.
    pub fn new(value: Vec<u8>) -> Result<Self, MetadataKeyError> {
        validate_component("tenant", &value)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// An immutable bucket identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BucketId([u8; 16]);

impl BucketId {
    #[must_use]
    pub const fn new(value: [u8; 16]) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Builds ordered, binary-safe S3 metadata keys.
pub struct MetadataKey;

impl MetadataKey {
    #[must_use]
    pub fn bucket_name_prefix(tenant: &TenantId) -> Vec<u8> {
        let mut key = namespace_prefix(tenant);
        key.push(BUCKET_NAME_KIND);
        key
    }

    #[must_use]
    pub fn bucket_name_end(tenant: &TenantId) -> Vec<u8> {
        let mut key = namespace_prefix(tenant);
        key.push(BUCKET_NAME_KIND + 1);
        key
    }

    /// # Errors
    ///
    /// Returns an error when the bucket name is not a valid metadata component.
    pub fn bucket_name(tenant: &TenantId, name: &[u8]) -> Result<Vec<u8>, MetadataKeyError> {
        validate_component("bucket name", name)?;
        let mut key = Self::bucket_name_prefix(tenant);
        append_ordered_bytes(&mut key, name);
        Ok(key)
    }

    #[must_use]
    pub fn object_prefix(tenant: &TenantId, bucket: BucketId) -> Vec<u8> {
        let mut key = namespace_prefix(tenant);
        key.extend_from_slice(bucket.as_bytes());
        key.push(OBJECT_KIND);
        key
    }

    /// # Errors
    ///
    /// Returns an error when the object key is not a valid metadata component.
    pub fn object(tenant: &TenantId, bucket: BucketId, object: &[u8]) -> Result<Vec<u8>, MetadataKeyError> {
        validate_component("object key", object)?;
        let mut key = Self::object_prefix(tenant, bucket);
        append_ordered_bytes(&mut key, object);
        Ok(key)
    }

    /// Returns the inclusive lower bound for object keys with `object_prefix`.
    ///
    /// # Errors
    ///
    /// Returns an error when the prefix exceeds the object-key bound.
    pub fn object_key_prefix(
        tenant: &TenantId,
        bucket: BucketId,
        object_prefix: &[u8],
    ) -> Result<Vec<u8>, MetadataKeyError> {
        if object_prefix.len() > MAX_KEY_BYTES {
            return Err(MetadataKeyError::TooLong("object key prefix"));
        }
        let mut key = Self::object_prefix(tenant, bucket);
        append_ordered_bytes_prefix(&mut key, object_prefix);
        Ok(key)
    }

    /// Returns the exclusive upper bound for object keys with `object_prefix`.
    ///
    /// # Errors
    ///
    /// Returns an error when the prefix exceeds the object-key bound.
    pub fn object_key_prefix_end(
        tenant: &TenantId,
        bucket: BucketId,
        object_prefix: &[u8],
    ) -> Result<Vec<u8>, MetadataKeyError> {
        if object_prefix.is_empty() {
            return Ok(Self::object_end(tenant, bucket));
        }
        let mut end = Self::object_key_prefix(tenant, bucket, object_prefix)?;
        increment_lexicographic(&mut end);
        Ok(end)
    }

    /// Returns an inclusive scan cursor that sorts strictly after `object`.
    ///
    /// # Errors
    ///
    /// Returns an error when the object key is invalid.
    pub fn object_after(
        tenant: &TenantId,
        bucket: BucketId,
        object: &[u8],
    ) -> Result<Vec<u8>, MetadataKeyError> {
        let mut key = Self::object(tenant, bucket, object)?;
        key.push(0);
        Ok(key)
    }

    /// Returns the exclusive upper bound for all object keys of one bucket.
    #[must_use]
    pub fn object_end(tenant: &TenantId, bucket: BucketId) -> Vec<u8> {
        let mut end = namespace_prefix(tenant);
        end.extend_from_slice(bucket.as_bytes());
        end.push(OBJECT_KIND + 1);
        end
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataKeyError {
    Empty(&'static str),
    TooLong(&'static str),
}

impl fmt::Display for MetadataKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty(component) => write!(formatter, "{component} cannot be empty"),
            Self::TooLong(component) => write!(formatter, "{component} exceeds {MAX_KEY_BYTES} bytes"),
        }
    }
}

impl std::error::Error for MetadataKeyError {}

fn namespace_prefix(tenant: &TenantId) -> Vec<u8> {
    let mut key = Vec::with_capacity(tenant.as_bytes().len() * 2 + 2 + 17);
    append_ordered_bytes(&mut key, tenant.as_bytes());
    key
}

fn validate_component(name: &'static str, value: &[u8]) -> Result<(), MetadataKeyError> {
    if value.is_empty() {
        return Err(MetadataKeyError::Empty(name));
    }
    if value.len() > MAX_KEY_BYTES {
        return Err(MetadataKeyError::TooLong(name));
    }
    Ok(())
}

fn append_ordered_bytes(output: &mut Vec<u8>, value: &[u8]) {
    append_ordered_bytes_prefix(output, value);
    output.extend_from_slice(&[0, 0]);
}

fn append_ordered_bytes_prefix(output: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        match byte {
            0 => output.extend_from_slice(&[0, 0xff]),
            value => output.push(*value),
        }
    }
}

fn increment_lexicographic(value: &mut Vec<u8>) {
    while let Some(byte) = value.pop() {
        if byte != u8::MAX {
            value.push(byte + 1);
            return;
        }
    }
}
