use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

use crate::{DatasetError, SnapshotId};

pub const MANIFEST_VERSION: u16 = 1;
pub const MAX_INLINE_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManifestRecord {
    pub version: u16,
    pub snapshot: SnapshotId,
    pub schema: SchemaRecord,
    pub samples: Vec<SampleRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManifestPartition {
    pub version: u16,
    pub snapshot: SnapshotId,
    pub partition: u32,
    pub schema: SchemaRecord,
    pub samples: Vec<SampleRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SchemaRecord {
    pub version: u32,
    pub fields: Vec<FieldDefinition>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub name: String,
    pub required: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SampleRecord {
    pub sample_id: Vec<u8>,
    pub fields: Vec<FieldRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldRecord {
    pub name: String,
    pub value: FieldLocator,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FieldLocator {
    Inline {
        value: Vec<u8>,
        md5: [u8; 16],
    },
    Chunk {
        location: Vec<u8>,
        length: u64,
        md5: [u8; 16],
    },
    Tombstone,
}

impl ManifestRecord {
    /// # Errors
    /// Rejects unsupported versions, duplicate fields, and oversized inline values.
    pub fn validate(&self) -> Result<(), DatasetError> {
        if self.version != MANIFEST_VERSION || self.samples.iter().any(|sample| sample.sample_id.is_empty()) {
            return Err(DatasetError::InvalidManifest);
        }
        self.schema.validate()?;
        let schema_names: std::collections::BTreeSet<_> = self
            .schema
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect();
        let mut sample_ids = std::collections::BTreeSet::new();
        for sample in &self.samples {
            if !sample_ids.insert(&sample.sample_id) {
                return Err(DatasetError::InvalidManifest);
            }
            let mut names = std::collections::BTreeSet::new();
            for field in &sample.fields {
                if !names.insert(&field.name)
                    || field.name.is_empty()
                    || !schema_names.contains(field.name.as_str())
                {
                    return Err(DatasetError::InvalidManifest);
                }
                field.value.validate()?;
            }
            for field in &self.schema.fields {
                if field.required && !names.contains(&field.name) {
                    return Err(DatasetError::InvalidManifest);
                }
            }
        }
        Ok(())
    }

    /// Splits a validated manifest into independently addressable partitions.
    /// The schema is repeated so a partition can be decoded without a hot
    /// metadata key.
    ///
    /// # Errors
    /// Returns `InvalidPartitionSize` when `max_samples` is zero, or any
    /// validation error from the source manifest.
    pub fn partition(&self, max_samples: usize) -> Result<Vec<ManifestPartition>, DatasetError> {
        if max_samples == 0 {
            return Err(DatasetError::InvalidPartitionSize);
        }
        self.validate()?;
        self.samples
            .chunks(max_samples)
            .enumerate()
            .map(|(partition, samples)| {
                Ok(ManifestPartition {
                    version: self.version,
                    snapshot: self.snapshot,
                    partition: u32::try_from(partition).map_err(|_| DatasetError::InvalidManifest)?,
                    schema: self.schema.clone(),
                    samples: samples.to_vec(),
                })
            })
            .collect::<Result<Vec<_>, DatasetError>>()
    }
}

impl SchemaRecord {
    fn validate(&self) -> Result<(), DatasetError> {
        let mut names = std::collections::BTreeSet::new();
        for field in &self.fields {
            if field.name.is_empty() || !names.insert(&field.name) {
                return Err(DatasetError::InvalidManifest);
            }
        }
        Ok(())
    }
}

impl FieldLocator {
    fn validate(&self) -> Result<(), DatasetError> {
        match self {
            Self::Inline { value, md5 } => {
                if value.len() > MAX_INLINE_BYTES {
                    return Err(DatasetError::InlineValueTooLarge);
                }
                verify_md5(value, md5)?;
            }
            Self::Chunk { location, length, .. } if location.is_empty() || *length == 0 => {
                return Err(DatasetError::InvalidManifest);
            }
            _ => {}
        }
        Ok(())
    }

    /// Verifies a payload at the IO boundary without interpreting chunk
    /// locations.
    ///
    /// # Errors
    /// Returns a length or MD5 mismatch for a chunk locator, or an MD5 mismatch
    /// for an inline value.
    pub fn verify_payload(&self, payload: &[u8]) -> Result<(), DatasetError> {
        match self {
            Self::Inline { value, md5 } => {
                if payload != value {
                    return Err(DatasetError::ChecksumMismatch);
                }
                verify_md5(payload, md5)
            }
            Self::Chunk { length, md5, .. } => {
                if payload.len() as u64 != *length {
                    return Err(DatasetError::FieldLengthMismatch);
                }
                verify_md5(payload, md5)
            }
            Self::Tombstone => Ok(()),
        }
    }
}

fn verify_md5(payload: &[u8], expected: &[u8; 16]) -> Result<(), DatasetError> {
    let mut digest = Md5::new();
    digest.update(payload);
    if digest.finalize().as_slice() == expected {
        Ok(())
    } else {
        Err(DatasetError::ChecksumMismatch)
    }
}
