use std::{collections::BTreeMap, sync::Arc};

use super::{charge, projection, value, Error};
use crate::{
    file::{
        FileBlockStore, FileRecord, ParquetColumnReader, ParquetColumnValue, ParquetMetadata,
        ParquetMetadataError, ParquetPageLimits,
    },
    table::TableMetadataDocument,
};

mod state;

#[derive(Clone, Copy, Debug)]
pub struct PartitionStatisticsRowLimits {
    pub page: ParquetPageLimits,
    pub rows: u64,
    pub buffered_bytes: usize,
}

impl PartitionStatisticsRowLimits {
    pub(super) fn validate(self, rows: u64) -> Result<(), Error> {
        if self.rows == 0
            || self.rows > 1_000_000
            || rows > self.rows
            || self.buffered_bytes == 0
            || self.buffered_bytes > 64 * 1024 * 1024
            || self.page.bytes == 0
            || self.page.bytes > 8 * 1024 * 1024
            || self.page.values == 0
            || self.page.values > 1_048_576
            || self.page.pages == 0
            || self.page.pages > 1_000_000
        {
            return Err(ParquetMetadataError::Bounds.into());
        }
        Ok(())
    }
}

/// Validates canonical statistics pages, typed tuple ordering, spec membership and count consistency.
/// Does not prove that the statistics equal a snapshot's complete manifest inventory.
/// # Errors
/// Rejects malformed values, provable duplicate tuples, invalid counts and exhausted budgets.
pub async fn validate_partition_statistics_rows(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    metadata: &ParquetMetadata,
    document: &TableMetadataDocument,
    limits: PartitionStatisticsRowLimits,
    work: &mut usize,
) -> Result<(), Error> {
    validate(store, record, metadata, document, limits, work, None).await
}

pub(super) async fn validate(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    metadata: &ParquetMetadata,
    document: &TableMetadataDocument,
    limits: PartitionStatisticsRowLimits,
    work: &mut usize,
    mut inventory: Option<&mut super::inventory::Inventory>,
) -> Result<(), Error> {
    limits.validate(metadata.rows)?;
    let projection = super::validated_projection(metadata, document, work)?;
    let partition_index = metadata
        .schema
        .iter()
        .position(|field| field.field_id == Some(1))
        .ok_or(Error::Schema)?;
    let partition_end = partition_index + 1 + metadata.schema[partition_index].children;
    let partition_ids: Vec<_> = metadata.schema[partition_index + 1..partition_end]
        .iter()
        .map(|field| field.field_id.ok_or(Error::Schema))
        .collect::<Result<_, _>>()?;
    let mut state = state::State::new(document, &partition_ids, work)?;
    let mut total_rows = 0_u64;
    for group in &metadata.groups {
        total_rows = total_rows
            .checked_add(group.rows)
            .filter(|rows| *rows <= metadata.rows)
            .ok_or(Error::Rows)?;
        let GroupReader {
            buffered,
            mut readers,
        } = GroupReader::open(&store, record, metadata, group, limits, work)?;
        for _ in 0..group.rows {
            let mut tuple = BTreeMap::new();
            let mut counts = [None; 14];
            let mut bytes = 0_usize;
            for (index, reader) in &mut readers {
                charge(work, 1)?;
                let field = &metadata.schema[*index];
                let id = field.field_id.ok_or(Error::Schema)?;
                let value = reader.next().await?.ok_or(Error::Rows)?;
                if let ParquetColumnValue::Bytes(bytes) = &value {
                    charge(work, bytes.len())?;
                }
                if *index > partition_index && *index < partition_end {
                    let kind = projection
                        .get(&id)
                        .and_then(|field| field.result.as_ref())
                        .ok_or(Error::Schema)?;
                    let value = value::decode(value, field, kind)?;
                    charge(work, value.bytes())?;
                    bytes = bytes
                        .checked_add(value.bytes() + 128)
                        .ok_or(ParquetMetadataError::Bounds)?;
                    if bytes
                        .checked_add(state.retained_bytes)
                        .and_then(|bytes| bytes.checked_add(buffered))
                        .map_or(true, |bytes| bytes > limits.buffered_bytes)
                    {
                        return Err(ParquetMetadataError::Bounds.into());
                    }
                    tuple.insert(id, value);
                } else {
                    let count = match value {
                        ParquetColumnValue::Null => None,
                        ParquetColumnValue::Long(value) => Some(value),
                        _ => return Err(Error::Schema),
                    };
                    *counts
                        .get_mut(usize::try_from(id).map_err(|_| Error::Schema)?)
                        .ok_or(Error::Schema)? = count;
                }
            }
            if let Some(inventory) = inventory.as_deref_mut() {
                inventory.row(&tuple, &counts, work)?;
            }
            state.observe(tuple, &counts, &projection, work)?;
        }
        for (_, reader) in &mut readers {
            charge(work, 1)?;
            if reader.next().await?.is_some() {
                return Err(Error::Rows);
            }
        }
    }
    if total_rows != metadata.rows || metadata.row_groups != metadata.groups.len() {
        return Err(Error::Rows);
    }
    Ok(())
}

struct GroupReader {
    buffered: usize,
    readers: Vec<(usize, ParquetColumnReader)>,
}

impl GroupReader {
    fn open(
        store: &Arc<dyn FileBlockStore>,
        record: &FileRecord,
        metadata: &ParquetMetadata,
        group: &crate::file::ParquetRowGroup,
        limits: PartitionStatisticsRowLimits,
        work: &mut usize,
    ) -> Result<Self, Error> {
        validate_group(metadata, group)?;
        charge(work, group.columns.len())?;
        let buffered = group
            .columns
            .len()
            .checked_mul(4)
            .and_then(|pages| pages.checked_add(8))
            .and_then(|pages| pages.checked_mul(limits.page.bytes))
            .filter(|bytes| *bytes <= limits.buffered_bytes)
            .ok_or(ParquetMetadataError::Bounds)?;
        let mut readers = Vec::new();
        for column in &group.columns {
            let field = metadata.schema.get(column.schema_index).ok_or(Error::Schema)?;
            readers.push((
                column.schema_index,
                ParquetColumnReader::new(store.clone(), record, column, field, limits.page)?,
            ));
        }
        Ok(Self { buffered, readers })
    }
}

fn validate_group(metadata: &ParquetMetadata, group: &crate::file::ParquetRowGroup) -> Result<(), Error> {
    let leaves: Vec<_> = metadata
        .schema
        .iter()
        .enumerate()
        .filter(|(_, field)| field.physical_type.is_some())
        .map(|(index, _)| index)
        .collect();
    if leaves.len() != group.columns.len()
        || group
            .columns
            .iter()
            .zip(leaves)
            .any(|(column, index)| column.schema_index != index || column.values != group.rows)
    {
        return Err(Error::Schema);
    }
    Ok(())
}
