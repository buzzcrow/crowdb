// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{error::ApiError, models};
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::{
    DatasetRecord, FieldLocator, FieldRecord, ManifestRecord, PublicationState, SampleRecord, SchemaRecord,
    SnapshotId, SnapshotRecord,
};
use md5::{Digest, Md5};
use std::collections::BTreeMap;

pub(super) fn dataset(value: &DatasetRecord) -> models::DatasetInfo {
    models::DatasetInfo {
        id: value.id.to_string(),
        namespace: value.identity.namespace().to_string(),
        name: value.identity.name().into(),
        latest: value.latest.map(|v| v.to_string()),
        stable: value.stable.map(|v| v.to_string()),
    }
}
pub(super) fn snapshot(value: SnapshotRecord, latest: Option<SnapshotId>) -> models::Snapshot {
    let value = value.publication;
    models::Snapshot {
        id: value.snapshot.to_string(),
        parent: value.parent.map(|v| v.to_string()),
        operation: value.operation.to_string(),
        state: match value.state {
            PublicationState::Prepared => "prepared",
            PublicationState::Published => "published",
            PublicationState::Aborted => "aborted",
        }
        .into(),
        is_latest: latest == Some(value.snapshot),
    }
}
pub(super) fn manifest(
    value: models::Manifest,
    identity: &crowdb_access_dataset::DatasetIdentity,
) -> Result<ManifestRecord, ApiError> {
    if value.schema.len() > 256 || value.samples.is_empty() || value.samples.len() > 1_000_000 {
        return Err(ApiError::invalid());
    }
    let mut fields: Vec<crowdb_access_dataset::FieldDefinition> = value
        .schema
        .into_iter()
        .map(|v| {
            if v.name.is_empty() || v.name.len() > 255 {
                return Err(ApiError::invalid());
            }
            Ok(crowdb_access_dataset::FieldDefinition {
                name: v.name,
                required: v.required,
            })
        })
        .collect::<Result<_, _>>()?;
    let mut samples: Vec<SampleRecord> = value
        .samples
        .into_iter()
        .map(|s| {
            let id = STANDARD.decode(&s.id).map_err(|_| ApiError::invalid())?;
            if id.is_empty() || id.len() > 1024 || STANDARD.encode(&id) != s.id || s.fields.len() > 256 {
                return Err(ApiError::invalid());
            }
            let fields = s
                .fields
                .into_iter()
                .map(|(name, v)| {
                    let value = match v {
                        models::FieldValue::Inline { data } => {
                            if data.len() > 87_384 {
                                return Err(ApiError::too_large());
                            }
                            let value = STANDARD.decode(&data).map_err(|_| ApiError::invalid())?;
                            if STANDARD.encode(&value) != data {
                                return Err(ApiError::invalid());
                            }
                            FieldLocator::Inline {
                                md5: Md5::digest(&value).into(),
                                value,
                            }
                        }
                        models::FieldValue::Tombstone => FieldLocator::Tombstone,
                        models::FieldValue::Payload { upload } => FieldLocator::Chunk {
                            location: crowdb_access_dataset::DatasetUploads::location(
                                identity,
                                super::operation(&upload)?,
                            )?,
                            length: 1,
                            md5: [0; 16],
                        },
                    };
                    Ok(FieldRecord { name, value })
                })
                .collect::<Result<_, _>>()?;
            Ok(SampleRecord {
                sample_id: id,
                fields,
            })
        })
        .collect::<Result<_, _>>()?;
    fields.sort_by(|a, b| a.name.cmp(&b.name));
    samples.sort_by(|a, b| a.sample_id.cmp(&b.sample_id));
    // The candidate identity is assigned by the publication authority. A fixed
    // placeholder makes equivalent JSON requests serialize identically on retry.
    let manifest = ManifestRecord {
        version: 1,
        snapshot: SnapshotId::from_bytes(&[1; 16]).map_err(|_| ApiError::integrity())?,
        schema: SchemaRecord { version: 1, fields },
        samples,
    };
    manifest.validate().map_err(|_| ApiError::invalid())?;
    Ok(manifest)
}
pub(super) fn public_manifest(
    value: ManifestRecord,
    identity: &crowdb_access_dataset::DatasetIdentity,
) -> Result<models::Manifest, ApiError> {
    let samples = value
        .samples
        .into_iter()
        .map(|sample| {
            let fields = sample
                .fields
                .into_iter()
                .map(|field| {
                    let value = match field.value {
                        FieldLocator::Inline { value, .. } => models::FieldValue::Inline {
                            data: STANDARD.encode(value),
                        },
                        FieldLocator::Tombstone => models::FieldValue::Tombstone,
                        FieldLocator::Chunk { location, .. } => models::FieldValue::Payload {
                            upload: crowdb_access_dataset::DatasetUploads::upload_id(identity, &location)?
                                .to_string(),
                        },
                    };
                    Ok((field.name, value))
                })
                .collect::<Result<BTreeMap<_, _>, ApiError>>()?;
            Ok(models::Sample {
                id: STANDARD.encode(sample.sample_id),
                fields,
            })
        })
        .collect::<Result<_, ApiError>>()?;
    Ok(models::Manifest {
        schema: value
            .schema
            .fields
            .into_iter()
            .map(|v| models::FieldDefinition {
                name: v.name,
                required: v.required,
            })
            .collect(),
        samples,
    })
}
