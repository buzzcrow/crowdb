use std::{collections::BTreeMap, sync::Arc};

use super::{charge, projection, rows, value, Error, PartitionStatisticsRowLimits};
use crate::{
    file::{ContentFormat, FileBlockStore, FileRecord, ParquetMetadata, ParquetMetadataError},
    manifest::{EntryStatus, FileContentKind, ManifestContext, ManifestScalarEntry, SnapshotManifestReader},
    table::TableMetadataDocument,
};

mod key;

const COUNTERS: [usize; 8] = [3, 4, 5, 6, 7, 8, 9, 13];

struct Counts {
    expected: [u64; 8],
    observed: [Option<u64>; 8],
    total: Option<u64>,
    seen: bool,
}

pub(super) struct Inventory {
    entries: BTreeMap<Vec<u8>, Counts>,
    fields: BTreeMap<i32, projection::PartitionField>,
    present: Vec<i32>,
    bytes: usize,
    limit: usize,
}

/// Checks statistics counters against a complete selected manifest inventory.
/// The caller supplies a newly opened reader bound to the statistics snapshot.
/// Does not read data rows to recompute counts after equality or ordinary position deletes.
/// # Errors
/// Rejects mismatched counters, missing live partitions, malformed files and exhausted budgets.
pub async fn validate_partition_statistics_inventory(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    metadata: &ParquetMetadata,
    document: &TableMetadataDocument,
    reader: &mut SnapshotManifestReader,
    limits: PartitionStatisticsRowLimits,
    work: &mut usize,
) -> Result<(), crate::manifest::SnapshotValidationError> {
    limits.validate(metadata.rows)?;
    let mut inventory = Inventory::new(metadata, document, limits.buffered_bytes / 2, work)?;
    while let Some(entry) = reader.next_entry().await? {
        charge(work, 1)?;
        if entry.entry.status != EntryStatus::Deleted {
            let (_, context) = reader.current_manifest().ok_or(Error::Schema)?;
            inventory.entry(&entry, context, work)?;
        }
    }
    reader.finish()?;
    let remaining = limits
        .buffered_bytes
        .checked_sub(inventory.bytes)
        .ok_or(Error::from(ParquetMetadataError::Bounds))?;
    rows::validate(
        store,
        record,
        metadata,
        document,
        PartitionStatisticsRowLimits {
            buffered_bytes: remaining,
            ..limits
        },
        work,
        Some(&mut inventory),
    )
    .await?;
    inventory.finish()?;
    Ok(())
}

impl Inventory {
    fn new(
        metadata: &ParquetMetadata,
        document: &TableMetadataDocument,
        limit: usize,
        work: &mut usize,
    ) -> Result<Self, Error> {
        let fields = super::validated_projection(metadata, document, work)?;
        let index = metadata
            .schema
            .iter()
            .position(|field| field.field_id == Some(1))
            .ok_or(Error::Schema)?;
        let present = metadata.schema[index + 1..index + 1 + metadata.schema[index].children]
            .iter()
            .map(|field| field.field_id.ok_or(Error::Schema))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            entries: BTreeMap::new(),
            fields,
            present,
            bytes: 0,
            limit,
        })
    }

    fn entry(
        &mut self,
        entry: &ManifestScalarEntry,
        context: &ManifestContext,
        work: &mut usize,
    ) -> Result<(), Error> {
        let key = key::manifest(entry, context, &self.fields, &self.present, work)?;
        let comparison = self.entries.len().max(1).ilog2() as usize + 1;
        charge(
            work,
            key.len()
                .checked_mul(comparison)
                .ok_or(ParquetMetadataError::Bounds)?,
        )?;
        if !self.entries.contains_key(&key) {
            self.bytes = self
                .bytes
                .checked_add(key.len() + std::mem::size_of::<Counts>() + 128)
                .filter(|bytes| *bytes <= self.limit)
                .ok_or(ParquetMetadataError::Bounds)?;
            self.entries.insert(
                key.clone(),
                Counts {
                    expected: [0; 8],
                    observed: [Some(0); 8],
                    total: Some(0),
                    seen: false,
                },
            );
        }
        let counts = self.entries.get_mut(&key).ok_or(Error::Schema)?;
        let rows = u64::try_from(entry.entry.record_count).map_err(|_| Error::Rows)?;
        let updates = match entry.entry.content {
            FileContentKind::Data => [(0, rows), (1, 1), (2, entry.file.length)],
            FileContentKind::PositionDeletes => [
                (3, rows),
                (
                    if entry.file.format == ContentFormat::Puffin {
                        7
                    } else {
                        4
                    },
                    1,
                ),
                (0, 0),
            ],
            FileContentKind::EqualityDeletes => [(5, rows), (6, 1), (0, 0)],
        };
        for (index, amount) in updates {
            counts.expected[index] = counts.expected[index].checked_add(amount).ok_or(Error::Rows)?;
        }
        Ok(())
    }

    pub(super) fn row(
        &mut self,
        tuple: &BTreeMap<i32, value::Value>,
        values: &[Option<i64>; 14],
        work: &mut usize,
    ) -> Result<(), Error> {
        let spec = i32::try_from(values[2].ok_or(Error::Schema)?).map_err(|_| Error::Schema)?;
        let key = key::row(spec, tuple, work)?;
        let comparison = self.entries.len().max(1).ilog2() as usize + 1;
        charge(
            work,
            key.len()
                .checked_mul(comparison)
                .ok_or(ParquetMetadataError::Bounds)?,
        )?;
        let Some(counts) = self.entries.get_mut(&key) else {
            return if COUNTERS
                .iter()
                .chain([&10])
                .all(|index| values[*index].unwrap_or(0) == 0)
            {
                Ok(())
            } else {
                Err(Error::Rows)
            };
        };
        counts.seen = true;
        for (index, field) in COUNTERS.iter().enumerate() {
            counts.observed[index] = sum(counts.observed[index], values[*field])?;
        }
        counts.total = sum(counts.total, values[10])?;
        Ok(())
    }

    fn finish(self) -> Result<(), Error> {
        for counts in self.entries.values() {
            if !counts.seen
                || counts
                    .observed
                    .iter()
                    .zip(counts.expected)
                    .any(|(observed, expected)| observed.is_some_and(|observed| observed != expected))
            {
                return Err(Error::Rows);
            }
            if counts.expected[4] == 0 && counts.expected[6] == 0 {
                let expected = counts.expected[0]
                    .checked_sub(counts.expected[3])
                    .ok_or(Error::Rows)?;
                if counts.total.is_some_and(|total| total != expected) {
                    return Err(Error::Rows);
                }
            }
        }
        Ok(())
    }
}

fn sum(previous: Option<u64>, value: Option<i64>) -> Result<Option<u64>, Error> {
    match (previous, value) {
        (Some(previous), Some(value)) => Ok(Some(
            previous
                .checked_add(u64::try_from(value).map_err(|_| Error::Rows)?)
                .ok_or(Error::Rows)?,
        )),
        _ => Ok(None),
    }
}
