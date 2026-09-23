use std::collections::BTreeMap;

use serde_json::Value;

use super::{ManifestContent, ManifestVersion};

#[derive(Clone, Copy, Debug)]
pub struct ManifestMetadata<'metadata> {
    pub version: ManifestVersion,
    pub content: ManifestContent,
    pub schema_id: Option<i32>,
    pub partition_spec_id: Option<i32>,
    pub schema_json: &'metadata [u8],
    pub partition_spec_json: &'metadata [u8],
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestMetadataError {
    #[error("invalid or incomplete manifest Avro metadata")]
    Field,
}

impl<'metadata> ManifestMetadata<'metadata> {
    /// Reads the manifest writer's version and content from bounded OCF metadata.
    /// Full table-schema and partition-spec membership checks require table context.
    /// # Errors
    /// Rejects missing required keys, wrong version/content and malformed JSON or IDs.
    pub fn parse(metadata: &'metadata BTreeMap<String, Vec<u8>>) -> Result<Self, ManifestMetadataError> {
        let version = match metadata.get("format-version") {
            None => ManifestVersion::V1,
            Some(value) if value == b"1" => ManifestVersion::V1,
            Some(value) if value == b"2" => ManifestVersion::V2,
            Some(value) if value == b"3" => ManifestVersion::V3,
            _ => return Err(ManifestMetadataError::Field),
        };
        let schema_json = required(metadata, "schema")?;
        let partition_spec_json = required(metadata, "partition-spec")?;
        let schema: Value = serde_json::from_slice(schema_json).map_err(|_| ManifestMetadataError::Field)?;
        let schema = schema.as_object().ok_or(ManifestMetadataError::Field)?;
        if schema.get("type").and_then(Value::as_str) != Some("struct")
            || !schema.get("fields").is_some_and(Value::is_array)
        {
            return Err(ManifestMetadataError::Field);
        }
        let partition_spec: Value =
            serde_json::from_slice(partition_spec_json).map_err(|_| ManifestMetadataError::Field)?;
        if !partition_spec.is_array() {
            return Err(ManifestMetadataError::Field);
        }
        let schema_id = optional_id(metadata, "schema-id")?;
        let partition_spec_id = optional_id(metadata, "partition-spec-id")?;
        let content = match (version, metadata.get("content")) {
            (ManifestVersion::V1, None) => ManifestContent::Data,
            (ManifestVersion::V2 | ManifestVersion::V3, Some(value)) if value == b"data" => {
                ManifestContent::Data
            }
            (ManifestVersion::V2 | ManifestVersion::V3, Some(value)) if value == b"deletes" => {
                ManifestContent::Deletes
            }
            _ => return Err(ManifestMetadataError::Field),
        };
        if version != ManifestVersion::V1 && (schema_id.is_none() || partition_spec_id.is_none()) {
            return Err(ManifestMetadataError::Field);
        }
        let json_id = schema
            .get("schema-id")
            .map(|json_id| {
                json_id
                    .as_i64()
                    .and_then(|id| i32::try_from(id).ok())
                    .filter(|id| *id >= 0)
                    .ok_or(ManifestMetadataError::Field)
            })
            .transpose()?;
        if (version != ManifestVersion::V1 && json_id != schema_id)
            || json_id.is_some_and(|id| schema_id.is_some_and(|metadata_id| metadata_id != id))
        {
            return Err(ManifestMetadataError::Field);
        }
        Ok(Self {
            version,
            content,
            schema_id,
            partition_spec_id,
            schema_json,
            partition_spec_json,
        })
    }
}

fn required<'metadata>(
    metadata: &'metadata BTreeMap<String, Vec<u8>>,
    key: &str,
) -> Result<&'metadata [u8], ManifestMetadataError> {
    metadata
        .get(key)
        .filter(|value| !value.is_empty() && value.len() <= 1024 * 1024)
        .map(Vec::as_slice)
        .ok_or(ManifestMetadataError::Field)
}

fn optional_id(
    metadata: &BTreeMap<String, Vec<u8>>,
    key: &str,
) -> Result<Option<i32>, ManifestMetadataError> {
    metadata
        .get(key)
        .map(|value| {
            std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<i32>().ok())
                .filter(|value| *value >= 0)
                .ok_or(ManifestMetadataError::Field)
        })
        .transpose()
}
