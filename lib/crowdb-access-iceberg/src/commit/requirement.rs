use serde::Deserialize;

use crate::table::TableMetadataDocument;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TableRequirement {
    AssertCreate,
    AssertTableUuid {
        uuid: String,
    },
    AssertRefSnapshotId {
        #[serde(rename = "ref")]
        reference: String,
        #[serde(rename = "snapshot-id", deserialize_with = "required_snapshot_id")]
        snapshot_id: Option<i64>,
    },
    AssertLastAssignedFieldId {
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i32,
    },
    AssertCurrentSchemaId {
        #[serde(rename = "current-schema-id")]
        current_schema_id: i32,
    },
    AssertLastAssignedPartitionId {
        #[serde(rename = "last-assigned-partition-id")]
        last_assigned_partition_id: i32,
    },
    AssertDefaultSpecId {
        #[serde(rename = "default-spec-id")]
        default_spec_id: i32,
    },
    AssertDefaultSortOrderId {
        #[serde(rename = "default-sort-order-id")]
        default_sort_order_id: i32,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct RequirementLimits {
    pub count: usize,
    pub text_bytes: usize,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum RequirementError {
    #[error("table requirement resource limit exceeded")]
    Bounds,
    #[error("malformed table requirement at index {0}")]
    Invalid(usize),
    #[error("table requirement failed at index {0}")]
    Failed(usize),
}

/// Evaluates every supported requirement against one immutable selected input.
/// Callers must separately bound wire decoding and fence the same generation at publication.
/// # Errors
/// Distinguishes malformed input and exhausted budgets from deterministic requirement conflicts.
pub fn validate_requirements(
    requirements: &[TableRequirement],
    current: Option<&TableMetadataDocument>,
    limits: RequirementLimits,
) -> Result<(), RequirementError> {
    if limits.count == 0
        || limits.count > 1000
        || requirements.len() > limits.count
        || limits.text_bytes == 0
        || limits.text_bytes > 1024 * 1024
    {
        return Err(RequirementError::Bounds);
    }
    let mut bytes = 0_usize;
    for (index, requirement) in requirements.iter().enumerate() {
        bytes = bytes
            .checked_add(requirement.validate().ok_or(RequirementError::Invalid(index))?)
            .ok_or(RequirementError::Bounds)?;
        if bytes > limits.text_bytes {
            return Err(RequirementError::Bounds);
        }
    }
    for (index, requirement) in requirements.iter().enumerate() {
        if !requirement.matches(current) {
            return Err(RequirementError::Failed(index));
        }
    }
    Ok(())
}

impl TableRequirement {
    fn validate(&self) -> Option<usize> {
        match self {
            Self::AssertCreate => Some(0),
            Self::AssertTableUuid { uuid } => {
                (uuid.len() == 36 && uuid::Uuid::parse_str(uuid).is_ok()).then_some(uuid.len())
            }
            Self::AssertRefSnapshotId { reference, .. } => (!reference.is_empty()).then_some(reference.len()),
            Self::AssertLastAssignedFieldId {
                last_assigned_field_id: id,
            }
            | Self::AssertCurrentSchemaId {
                current_schema_id: id,
            }
            | Self::AssertLastAssignedPartitionId {
                last_assigned_partition_id: id,
            }
            | Self::AssertDefaultSpecId { default_spec_id: id }
            | Self::AssertDefaultSortOrderId {
                default_sort_order_id: id,
            } => (*id >= 0).then_some(0),
        }
    }

    fn matches(&self, current: Option<&TableMetadataDocument>) -> bool {
        if matches!(self, Self::AssertCreate) {
            return current.is_none();
        }
        let Some(current) = current else {
            return false;
        };
        let fields = current.fields();
        match self {
            Self::AssertCreate => false,
            Self::AssertTableUuid { uuid } => {
                uuid::Uuid::parse_str(uuid).ok() == current.selected_head().table_uuid
            }
            Self::AssertRefSnapshotId {
                reference,
                snapshot_id,
            } => {
                let actual = fields
                    .get("refs")
                    .and_then(|refs| refs.get(reference))
                    .and_then(|reference| reference.get("snapshot-id"))
                    .and_then(serde_json::Value::as_i64)
                    .or_else(|| {
                        (reference == "main")
                            .then(|| current.current_snapshot())
                            .flatten()
                    });
                actual == *snapshot_id
            }
            Self::AssertLastAssignedFieldId {
                last_assigned_field_id,
            } => super::transition::number(current, "last-column-id") == i64::from(*last_assigned_field_id),
            Self::AssertCurrentSchemaId { current_schema_id } => {
                let actual = fields
                    .get("current-schema-id")
                    .or_else(|| fields.get("schema").and_then(|schema| schema.get("schema-id")))
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                actual == i64::from(*current_schema_id)
            }
            Self::AssertLastAssignedPartitionId {
                last_assigned_partition_id,
            } => {
                super::transition::number(current, "last-partition-id")
                    == i64::from(*last_assigned_partition_id)
            }
            Self::AssertDefaultSpecId { default_spec_id } => {
                super::transition::number(current, "default-spec-id") == i64::from(*default_spec_id)
            }
            Self::AssertDefaultSortOrderId {
                default_sort_order_id,
            } => {
                super::transition::number(current, "default-sort-order-id")
                    == i64::from(*default_sort_order_id)
            }
        }
    }
}

fn required_snapshot_id<'de, Decoder: serde::Deserializer<'de>>(
    decoder: Decoder,
) -> Result<Option<i64>, Decoder::Error> {
    Option::<i64>::deserialize(decoder)
}
