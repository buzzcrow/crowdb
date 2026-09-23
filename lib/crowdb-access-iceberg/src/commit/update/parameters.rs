use super::{SnapshotRefType, TableUpdate};
use crate::table::TableMetadataError as Error;

impl TableUpdate {
    pub(crate) fn validate_parameters(&self) -> Result<(), Error> {
        match self {
            Self::AssignUuid { uuid } => {
                uuid::Uuid::parse_str(uuid).map_err(|_| Error::Field("uuid"))?;
            }
            Self::UpgradeFormatVersion { format_version } => {
                require((1..=3).contains(format_version), "format-version")?;
            }
            Self::SetCurrentSchema { schema_id } => require(*schema_id >= -1, "schema-id")?,
            Self::SetDefaultSpec { spec_id } => require(*spec_id >= -1, "spec-id")?,
            Self::SetDefaultSortOrder { sort_order_id } => require(*sort_order_id >= -1, "sort-order-id")?,
            Self::SetSnapshotRef {
                ref_name,
                kind,
                min_snapshots_to_keep,
                max_snapshot_age_ms,
                max_ref_age_ms,
                ..
            } => {
                require(!ref_name.is_empty(), "ref-name")?;
                require(ref_name != "main" || *kind == SnapshotRefType::Branch, "type")?;
                require(
                    positive(*min_snapshots_to_keep)
                        && (*kind == SnapshotRefType::Branch || min_snapshots_to_keep.is_none()),
                    "min-snapshots-to-keep",
                )?;
                require(
                    positive(*max_snapshot_age_ms)
                        && (*kind == SnapshotRefType::Branch || max_snapshot_age_ms.is_none()),
                    "max-snapshot-age-ms",
                )?;
                require(positive(*max_ref_age_ms), "max-ref-age-ms")?;
            }
            Self::RemoveSnapshotRef { ref_name } => require(!ref_name.is_empty(), "ref-name")?,
            Self::RemovePartitionSpecs { spec_ids } => {
                require(spec_ids.iter().all(|value| *value >= 0), "spec-ids")?;
            }
            Self::RemoveSchemas { schema_ids } => {
                require(schema_ids.iter().all(|value| *value >= 0), "schema-ids")?;
            }
            Self::RemoveEncryptionKey { key_id } => require(!key_id.is_empty(), "key-id")?,
            _ => {}
        }
        Ok(())
    }
}

fn require(valid: bool, field: &'static str) -> Result<(), Error> {
    if valid {
        Ok(())
    } else {
        Err(Error::Field(field))
    }
}

fn positive<Number: PartialOrd + From<u8>>(value: Option<Number>) -> bool {
    value.map_or(true, |value| value > Number::from(0))
}
