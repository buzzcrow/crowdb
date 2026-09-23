use super::{
    EntryStatus, ManifestContext, ManifestEntryError as Error, ManifestEntryProjection, ManifestEntryState,
    ManifestListEntry, ManifestMetadata, ManifestScalarEntry, ManifestVersion,
};
use crate::file::{
    AvroContainerError, AvroDatumLimits, AvroDecodedBlock, AvroLimits, AvroRecords, ContentFormat,
    FileBlockStore, FileKind, FileRecord,
};
use std::sync::Arc;

pub struct ManifestReader {
    reader: AvroRecords,
    context: ManifestContext,
    state: ManifestEntryState,
    version: ManifestVersion,
    list: ManifestListEntry,
    limits: AvroDatumLimits,
    block: Option<AvroDecodedBlock>,
    offset: usize,
    remaining: u64,
    files: [i64; 3],
    rows: [i64; 3],
    failed: bool,
    complete: bool,
    min_sequence: Option<i64>,
    summaries: super::summary::SummaryState,
}

impl ManifestReader {
    pub(super) fn selection(&self) -> (&crate::file::FileLocation, &ManifestContext) {
        (&self.list.location, &self.context)
    }

    /// Opens exactly the file named by a manifest list using trusted historical table context.
    /// # Errors
    /// Rejects file identity/length/kind, header, history and partition-schema mismatches.
    pub async fn open(
        store: Arc<dyn FileBlockStore>,
        record: FileRecord,
        list: ManifestListEntry,
        context: ManifestContext,
        framing: AvroLimits,
        limits: AvroDatumLimits,
        decoded_bytes: usize,
    ) -> Result<Self, Error> {
        if record.location != list.location
            || record.length != list.length
            || record.format != ContentFormat::Avro
        {
            return Err(Error::Field);
        }
        let record = record.bind_kind(FileKind::Manifest).map_err(|_| Error::Field)?;
        if list.min_sequence < 0
            || list.min_sequence > list.sequence
            || list.file_counts.iter().flatten().any(|value| *value < 0)
            || list.row_counts.iter().flatten().any(|value| *value < 0)
        {
            return Err(Error::Field);
        }
        let table = record.location.table();
        let reader = AvroRecords::open(store, record, framing, limits, decoded_bytes).await?;
        let metadata = ManifestMetadata::parse(reader.metadata()).map_err(|_| Error::Field)?;
        context.validate_metadata(metadata, list.partition_spec_id)?;
        let state = ManifestEntryState::from_list(metadata, &list, table)?;
        let version = metadata.version;
        ManifestEntryProjection::with_context(reader.schema(), version, table, &context)?;
        let summaries = super::summary::SummaryState::new(&list, &context)?;
        Ok(Self {
            reader,
            context,
            state,
            version,
            list,
            limits,
            block: None,
            offset: 0,
            remaining: 0,
            files: [0; 3],
            rows: [0; 3],
            failed: false,
            complete: false,
            min_sequence: None,
            summaries,
        })
    }

    #[must_use]
    pub fn next_row_id(&self) -> Option<i64> {
        self.state.next_row_id()
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Pulls one entry, retaining one bounded block and no manifest-sized entry collection.
    /// # Errors
    /// Cancellation or any failure permanently poisons this reader. EOF verifies list totals;
    /// entries yielded before EOF are not proof that the complete manifest is valid.
    pub async fn next_entry(&mut self) -> Result<Option<ManifestScalarEntry>, Error> {
        if self.failed {
            return Err(AvroContainerError::Failed.into());
        }
        if self.complete {
            return Ok(None);
        }
        self.failed = true;
        while self.remaining == 0 {
            self.block = None;
            let Some(block) = self.reader.next().await? else {
                self.check_totals(true)?;
                self.summaries.finish(&self.list, &self.context)?;
                self.complete = true;
                self.failed = false;
                return Ok(None);
            };
            self.remaining = block.records;
            self.offset = 0;
            self.block = Some(block);
        }
        let block = self.block.as_ref().ok_or(Error::Field)?;
        let projection = ManifestEntryProjection::with_context(
            self.reader.schema(),
            self.version,
            self.list.location.table(),
            &self.context,
        )?;
        let mut state = self.state.clone();
        let mut records = projection.records(
            &block.bytes[self.offset..],
            self.remaining,
            self.limits,
            &mut state,
        )?;
        let entry = records.next_entry()?.ok_or(Error::Field)?;
        let length = records.last_record_length();
        self.summaries.observe(
            &self.list,
            &self.context,
            entry.file.partition.as_deref().ok_or(Error::Field)?,
        )?;
        if entry.inherited.data_sequence > self.list.sequence
            || entry.inherited.file_sequence > self.list.sequence
        {
            return Err(Error::Field);
        }
        if entry.entry.status != EntryStatus::Deleted {
            if entry.inherited.data_sequence < self.list.min_sequence {
                return Err(Error::Field);
            }
            self.min_sequence = Some(
                self.min_sequence
                    .map_or(entry.inherited.data_sequence, |sequence| {
                        sequence.min(entry.inherited.data_sequence)
                    }),
            );
        }
        let slot = match entry.entry.status {
            EntryStatus::Added => 0,
            EntryStatus::Existing => 1,
            EntryStatus::Deleted => 2,
        };
        self.files[slot] = self.files[slot].checked_add(1).ok_or(Error::Field)?;
        self.rows[slot] = self.rows[slot]
            .checked_add(entry.entry.record_count)
            .ok_or(Error::Field)?;
        self.check_totals(false)?;
        self.offset += length;
        self.remaining -= 1;
        self.state = state;
        self.failed = false;
        Ok(Some(entry))
    }

    fn check_totals(&self, exact: bool) -> Result<(), Error> {
        if exact
            && self
                .min_sequence
                .is_some_and(|sequence| sequence != self.list.min_sequence)
        {
            return Err(Error::Field);
        }
        for slot in 0..3 {
            for (expected, actual) in [
                (self.list.file_counts[slot].map(i64::from), self.files[slot]),
                (self.list.row_counts[slot], self.rows[slot]),
            ] {
                if expected.is_some_and(|expected| actual > expected || exact && actual != expected) {
                    return Err(Error::Field);
                }
            }
        }
        Ok(())
    }
}
