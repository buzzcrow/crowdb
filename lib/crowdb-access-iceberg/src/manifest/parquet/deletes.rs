use std::sync::Arc;

use async_trait::async_trait;

use super::{read_parquet_selection, ParquetSelection, SelectedParquetError as Error};
use crate::file::{
    FileBlockStore, FileLocation, FileRecord, ParquetColumnReader, ParquetColumnValue, ParquetMetadataLimits,
    ParquetPageLimits,
};
use crate::manifest::FileContentKind;

#[async_trait]
pub trait PositionDeleteTargets: Send + Sync {
    /// Returns a canonical row count for a data file applicable in the selected scope.
    /// None means an unselected target; old delete files may refer to removed data files.
    async fn rows(&self, location: &FileLocation) -> Result<Option<u64>, Error>;

    /// Checks a validated applicable position; callbacks must not publish partial progress.
    async fn position(&self, _location: &FileLocation, _position: u64) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PositionDeleteLimits {
    pub metadata: ParquetMetadataLimits,
    pub page: ParquetPageLimits,
    pub rows: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PositionDeleteSummary {
    pub rows: u64,
    pub applicable_rows: u64,
    pub targets: u64,
}

/// Reads both reserved columns from canonical pages and checks every delete pair.
/// Returns only after exact EOF; cancellation discards all local progress. No authority
/// is changed. Target applicability and canonical data row counts belong to the caller.
/// # Errors
/// Rejects bad schema, pages, ordering, foreign paths and out-of-range positions.
pub async fn validate_parquet_position_deletes(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    selection: ParquetSelection<'_>,
    targets: &dyn PositionDeleteTargets,
    limits: PositionDeleteLimits,
) -> Result<PositionDeleteSummary, Error> {
    if selection.entry.entry.content != FileContentKind::PositionDeletes
        || limits.rows == 0
        || u64::try_from(selection.entry.entry.record_count).map_or(true, |rows| rows > limits.rows)
    {
        return Err(Error::Binding);
    }
    let (metadata, schema) =
        read_parquet_selection(store.clone(), record, &selection, limits.metadata).await?;
    let path_index = schema.field_index(2_147_483_546).ok_or(Error::Schema)?;
    let pos_index = schema.field_index(2_147_483_545).ok_or(Error::Schema)?;
    let mut state = State {
        summary: PositionDeleteSummary {
            rows: 0,
            applicable_rows: 0,
            targets: 0,
        },
        previous: None,
        target_rows: None,
        target_location: None,
    };
    for group in &metadata.groups {
        let path = group
            .columns
            .iter()
            .find(|column| column.schema_index == path_index)
            .ok_or(Error::Schema)?;
        let pos = group
            .columns
            .iter()
            .find(|column| column.schema_index == pos_index)
            .ok_or(Error::Schema)?;
        let mut paths = ParquetColumnReader::new(
            store.clone(),
            record,
            path,
            &metadata.schema[path_index],
            limits.page,
        )?;
        let mut positions = ParquetColumnReader::new(
            store.clone(),
            record,
            pos,
            &metadata.schema[pos_index],
            limits.page,
        )?;
        for _ in 0..group.rows {
            let (Some(ParquetColumnValue::Bytes(path)), Some(ParquetColumnValue::Long(pos))) =
                (paths.next().await?, positions.next().await?)
            else {
                return Err(Error::Rows);
            };
            state.observe(&path, pos, &selection, targets).await?;
        }
        if paths.next().await?.is_some() || positions.next().await?.is_some() {
            return Err(Error::Rows);
        }
    }
    if state.summary.rows != metadata.rows {
        return Err(Error::Rows);
    }
    Ok(state.summary)
}

struct State {
    summary: PositionDeleteSummary,
    previous: Option<(String, u64)>,
    target_rows: Option<u64>,
    target_location: Option<FileLocation>,
}

impl State {
    async fn observe(
        &mut self,
        bytes: &[u8],
        position: i64,
        selection: &ParquetSelection<'_>,
        targets: &dyn PositionDeleteTargets,
    ) -> Result<(), Error> {
        let path = std::str::from_utf8(bytes).map_err(|_| Error::Delete)?;
        let position = u64::try_from(position).map_err(|_| Error::Delete)?;
        if self
            .previous
            .as_ref()
            .is_some_and(|(previous, pos)| (previous.as_str(), *pos) > (path, position))
        {
            return Err(Error::Delete);
        }
        if self.previous.as_ref().map(|(previous, _)| previous.as_str()) != Some(path) {
            let location: FileLocation = path.parse().map_err(|_| Error::Delete)?;
            if location.table() != selection.table
                || selection
                    .entry
                    .file
                    .referenced_data_file
                    .as_ref()
                    .is_some_and(|expected| *expected != location)
            {
                return Err(Error::Delete);
            }
            self.target_rows = targets.rows(&location).await?;
            self.target_location = Some(location);
            self.summary.targets += 1;
        }
        if let Some(rows) = self.target_rows {
            if position >= rows {
                return Err(Error::Delete);
            }
            targets
                .position(self.target_location.as_ref().ok_or(Error::Delete)?, position)
                .await?;
            self.summary.applicable_rows += 1;
        }
        self.summary.rows += 1;
        self.previous = Some((path.into(), position));
        Ok(())
    }
}
