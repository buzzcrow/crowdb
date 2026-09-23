use std::sync::Arc;

use crate::file::{
    AvroContainerError, AvroDatumLimits, AvroDecodedBlock, AvroLimits, AvroRecords, ContentFormat,
    FileBlockStore, FileKind, FileLocation, FileRecord,
};

use super::{ManifestListEntry, ManifestListError as Error, ManifestListProjection, ManifestVersion};

pub struct ManifestListReader {
    reader: AvroRecords,
    location: FileLocation,
    version: ManifestVersion,
    limits: AvroDatumLimits,
    block: Option<AvroDecodedBlock>,
    offset: usize,
    remaining: u64,
    failed: bool,
    complete: bool,
}

impl ManifestListReader {
    /// The caller supplies the selected snapshot's location and trusted list writer version,
    /// not necessarily the current table version after an upgrade.
    /// An unbound upload remains immutable; this reader validates only its selected use.
    /// # Errors
    /// Rejects a different file, incompatible kind, malformed header or writer schema.
    pub async fn open(
        store: Arc<dyn FileBlockStore>,
        record: FileRecord,
        selected: (FileLocation, ManifestVersion),
        framing: AvroLimits,
        limits: AvroDatumLimits,
        decoded_bytes: usize,
    ) -> Result<Self, Error> {
        let (location, version) = selected;
        if record.location != location || record.format != ContentFormat::Avro {
            return Err(Error::Field);
        }
        let record = record
            .bind_kind(FileKind::ManifestList)
            .map_err(|_| Error::Field)?;
        let reader = AvroRecords::open(store, record, framing, limits, decoded_bytes).await?;
        ManifestListProjection::new(reader.schema(), version, location.table())?;
        Ok(Self {
            reader,
            location,
            version,
            limits,
            block: None,
            offset: 0,
            remaining: 0,
            failed: false,
            complete: false,
        })
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Retains at most one decoded block and returns one selected manifest reference.
    /// # Errors
    /// Any failure or cancelled read poisons the cursor; only EOF verifies the full file.
    pub async fn next_entry(&mut self) -> Result<Option<ManifestListEntry>, Error> {
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
                self.complete = true;
                self.failed = false;
                return Ok(None);
            };
            self.remaining = block.records;
            self.offset = 0;
            self.block = Some(block);
        }
        let block = self.block.as_ref().ok_or(Error::Field)?;
        let projection =
            ManifestListProjection::new(self.reader.schema(), self.version, self.location.table())?;
        let mut records = projection.records(&block.bytes[self.offset..], self.remaining, self.limits)?;
        let entry = records.next_entry()?.ok_or(Error::Field)?;
        self.offset += records.last_record_length();
        self.remaining -= 1;
        self.failed = false;
        Ok(Some(entry))
    }
}
