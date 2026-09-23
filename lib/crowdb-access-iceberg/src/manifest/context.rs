use std::collections::BTreeMap;

use super::{ManifestMetadata, ManifestVersion};

mod partition;
mod schema;
mod types;

pub use partition::{PartitionField, PartitionTransform};
pub use types::PrimitiveType;

#[derive(Debug, thiserror::Error)]
pub enum ManifestContextError {
    #[error("invalid manifest schema or partition context")]
    Invalid,
    #[error("manifest schema or partition context exceeds resource limits")]
    Bounds,
    #[error("unsupported manifest logical value")]
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchemaDefault {
    Absent,
    NonNull,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaField {
    pub name: String,
    pub parent: Option<i32>,
    pub primitive: Option<PrimitiveType>,
    pub required: bool,
    pub initial_default: SchemaDefault,
    pub required_path: bool,
    pub repeated: bool,
    pub kind: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestContext {
    version: ManifestVersion,
    schema_id: i32,
    spec_id: i32,
    fields: BTreeMap<i32, SchemaField>,
    partitions: Vec<PartitionField>,
    historical_fields: BTreeMap<i32, SchemaField>,
}

impl ManifestContext {
    /// Parses one historical schema/spec, independently of the table's current IDs.
    /// # Errors
    /// Rejects invalid IDs, nesting, transforms and excessive metadata or field counts.
    pub fn parse(
        version: ManifestVersion,
        schema_id: i32,
        spec_id: i32,
        schema: &[u8],
        partition_spec: &[u8],
    ) -> Result<Self, ManifestContextError> {
        if schema_id < 0 || spec_id < 0 {
            return Err(ManifestContextError::Invalid);
        }
        let fields = schema::parse(schema, version, schema_id)?;
        let partitions = partition::parse(partition_spec, version, &fields)?;
        Ok(Self {
            version,
            schema_id,
            spec_id,
            fields,
            partitions,
            historical_fields: BTreeMap::new(),
        })
    }

    /// Binds writer metadata to a trusted historical table schema/spec context.
    /// # Errors
    /// Rejects mismatched IDs or definitions, including nested field and transform changes.
    pub fn validate_metadata(
        &self,
        metadata: ManifestMetadata<'_>,
        list_spec_id: i32,
    ) -> Result<(), ManifestContextError> {
        if metadata.schema_id.is_some_and(|id| id != self.schema_id)
            || metadata.partition_spec_id.is_some_and(|id| id != self.spec_id)
            || list_spec_id != self.spec_id
        {
            return Err(ManifestContextError::Invalid);
        }
        let actual = Self::parse(
            metadata.version,
            self.schema_id,
            self.spec_id,
            metadata.schema_json,
            metadata.partition_spec_json,
        )?;
        if actual.fields != self.fields || actual.partitions != self.partitions {
            return Err(ManifestContextError::Invalid);
        }
        Ok(())
    }

    #[must_use]
    pub fn field(&self, id: i32) -> Option<&SchemaField> {
        self.fields.get(&id)
    }

    pub(crate) fn fields(&self) -> impl Iterator<Item = (&i32, &SchemaField)> {
        self.fields.iter()
    }

    pub(crate) fn version(&self) -> ManifestVersion {
        self.version
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        let fields: usize = self
            .fields
            .values()
            .chain(self.historical_fields.values())
            .map(|field| field_bytes(field) + 128)
            .sum();
        let partitions: usize = self
            .partitions
            .iter()
            .map(|field| {
                std::mem::size_of::<PartitionField>()
                    + field.name.len()
                    + field.sources.len() * 4
                    + match &field.transform {
                        PartitionTransform::Unknown(value) => value.len(),
                        _ => 0,
                    }
                    + match &field.result {
                        Some(PrimitiveType::Geometry(value) | PrimitiveType::Geography(value)) => value.len(),
                        _ => 0,
                    }
            })
            .sum();
        std::mem::size_of::<Self>() + fields + partitions
    }

    /// Adds trusted historical columns retained in metrics after a column was dropped.
    /// # Errors
    /// Rejects excessive history or incompatible type reuse of a dropped field ID.
    pub fn with_schema_history(mut self, history: &[Self]) -> Result<Self, ManifestContextError> {
        if history.len() > 16 {
            return Err(ManifestContextError::Bounds);
        }
        let mut work = 16_384_usize;
        let mut bytes = 1024 * 1024_usize;
        for field in self.historical_fields.values() {
            bytes = bytes
                .checked_sub(field_bytes(field))
                .ok_or(ManifestContextError::Bounds)?;
        }
        for schema in history {
            work = work
                .checked_sub(schema.fields.len())
                .ok_or(ManifestContextError::Bounds)?;
            for (id, field) in &schema.fields {
                if self.fields.contains_key(id) {
                    continue;
                }
                if let Some(prior) = self.historical_fields.get_mut(id) {
                    if prior.parent != field.parent
                        || prior.repeated != field.repeated
                        || prior.kind != field.kind
                    {
                        return Err(ManifestContextError::Invalid);
                    }
                    prior.primitive = merge_type(prior.primitive.as_ref(), field.primitive.as_ref())?;
                } else {
                    if self.historical_fields.len() + self.fields.len() >= 4096 {
                        return Err(ManifestContextError::Bounds);
                    }
                    bytes = bytes
                        .checked_sub(field_bytes(field))
                        .ok_or(ManifestContextError::Bounds)?;
                    self.historical_fields.insert(*id, field.clone());
                }
            }
        }
        Ok(self)
    }

    #[must_use]
    pub fn retained_field(&self, id: i32) -> Option<&SchemaField> {
        self.fields.get(&id).or_else(|| self.historical_fields.get(&id))
    }

    #[must_use]
    pub fn partitions(&self) -> &[PartitionField] {
        &self.partitions
    }

    #[must_use]
    pub fn schema_id(&self) -> i32 {
        self.schema_id
    }

    #[must_use]
    pub fn spec_id(&self) -> i32 {
        self.spec_id
    }
}

fn merge_type(
    first: Option<&PrimitiveType>,
    second: Option<&PrimitiveType>,
) -> Result<Option<PrimitiveType>, ManifestContextError> {
    use PrimitiveType::{Decimal, Double, Float, Int, Long};
    if first == second {
        return Ok(first.cloned());
    }
    Ok(Some(match (first, second) {
        (Some(Int), Some(Long)) | (Some(Long), Some(Int)) => Long,
        (Some(Float), Some(Double)) | (Some(Double), Some(Float)) => Double,
        (
            Some(Decimal {
                precision: first,
                scale,
            }),
            Some(Decimal {
                precision: second,
                scale: other,
            }),
        ) if scale == other => Decimal {
            precision: (*first).max(*second),
            scale: *scale,
        },
        _ => return Err(ManifestContextError::Invalid),
    }))
}

fn field_bytes(field: &SchemaField) -> usize {
    field.name.len()
        + std::mem::size_of::<SchemaField>()
        + match &field.primitive {
            Some(PrimitiveType::Geometry(value) | PrimitiveType::Geography(value)) => value.len(),
            _ => 0,
        }
}

fn json(bytes: &[u8]) -> Result<serde_json::Value, ManifestContextError> {
    if bytes.len() > 1024 * 1024 {
        return Err(ManifestContextError::Bounds);
    }
    serde_json::from_slice(bytes).map_err(|_| ManifestContextError::Invalid)
}

fn id(value: &serde_json::Value) -> Result<i32, ManifestContextError> {
    value
        .as_i64()
        .and_then(|id| i32::try_from(id).ok())
        .filter(|id| *id > 0 && *id <= 2_147_483_447)
        .ok_or(ManifestContextError::Invalid)
}
