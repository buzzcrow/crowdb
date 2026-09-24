use serde_json::Value;
use std::sync::Arc;

use super::{file_error, CandidateFileSource};
use crate::{
    file::{
        probe_puffin_footer, read_parquet_metadata, read_puffin_metadata, ContentFormat, FileKind,
        FileReader, FileRecord, ParquetMetadataLimits, PuffinBlob,
    },
    manifest::{
        validate_partition_statistics_inventory, ManifestVersion, PartitionStatisticsRowLimits,
        SnapshotManifestLimits, SnapshotManifestReader, SnapshotValidationError as Error,
    },
};

#[derive(Clone, Copy, Debug)]
pub struct CandidateAuxiliaryLimits {
    pub files: usize,
    pub bytes: u64,
    pub work: usize,
    pub puffin_encoded_bytes: usize,
    pub puffin_decoded_bytes: usize,
    pub parquet: ParquetMetadataLimits,
    pub partition_rows: PartitionStatisticsRowLimits,
    pub manifests: SnapshotManifestLimits,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CandidateAuxiliarySummary {
    pub files: usize,
    pub bytes: u64,
    pub blobs: usize,
}

impl CandidateFileSource {
    /// Resolves auxiliary references and validates canonical framing, lengths and statistics descriptors.
    /// Partition statistics counters are reconciled with their selected snapshot's manifest inventory.
    /// # Errors
    /// Rejects unavailable files, incorrect descriptors, encryption and exhausted aggregate budgets.
    pub async fn validate_auxiliary_files(
        self: &Arc<Self>,
        limits: CandidateAuxiliaryLimits,
    ) -> Result<CandidateAuxiliarySummary, Error> {
        self.ensure_current().await?;
        if !(1..=100_000).contains(&limits.files)
            || limits.bytes == 0
            || !(1..=1_000_000).contains(&limits.work)
        {
            return Err(Error::Bounds);
        }
        let mut summary = CandidateAuxiliarySummary::default();
        let mut work = limits.work;
        let mut manifests = limits.manifests;
        for field in ["statistics", "partition-statistics"] {
            let Some(entries) = self.candidate.fields().get(field) else {
                continue;
            };
            for entry in entries.as_array().ok_or(Error::Binding)? {
                charge(&mut work)?;
                summary.files = summary.files.checked_add(1).ok_or(Error::Bounds)?;
                if summary.files > limits.files {
                    return Err(Error::Bounds);
                }
                let path = entry["statistics-path"].as_str().ok_or(Error::Binding)?;
                let record = self
                    .load(&path.parse().map_err(file_error)?)
                    .await?
                    .bind_kind(FileKind::Statistics)
                    .map_err(file_error)?;
                if entry["file-size-in-bytes"].as_u64() != Some(record.length) {
                    return Err(Error::Binding);
                }
                summary.bytes = summary.bytes.checked_add(record.length).ok_or(Error::Bounds)?;
                if summary.bytes > limits.bytes {
                    return Err(Error::Bounds);
                }
                if field == "statistics" {
                    summary.blobs += self.statistics(entry, &record, limits, &mut work).await?;
                } else {
                    self.partition_statistics(entry, &record, limits, &mut manifests, &mut work)
                        .await?;
                }
                let mut reader =
                    FileReader::new(self.blocks.clone(), record, None, 16 * 1024).map_err(file_error)?;
                while reader.next().await.map_err(file_error)?.is_some() {}
            }
        }
        self.ensure_current().await?;
        Ok(summary)
    }

    async fn partition_statistics(
        self: &Arc<Self>,
        entry: &Value,
        record: &FileRecord,
        limits: CandidateAuxiliaryLimits,
        remaining: &mut SnapshotManifestLimits,
        work: &mut usize,
    ) -> Result<(), Error> {
        let snapshot_id = entry["snapshot-id"].as_i64().ok_or(Error::Binding)?;
        let snapshot = self
            .candidate
            .snapshots()
            .get(&snapshot_id)
            .ok_or(Error::Binding)?;
        let version = match self.candidate.selected_head().format_version {
            1 => ManifestVersion::V1,
            2 => ManifestVersion::V2,
            3 => ManifestVersion::V3,
            _ => return Err(Error::Binding),
        };
        let mut reader = if snapshot.manifest_list.is_some() {
            let selection = snapshot.manifest_selection(version).map_err(file_error)?;
            let list = self.load(&selection.location).await?;
            SnapshotManifestReader::open(self.blocks.clone(), self.clone(), list, selection, *remaining)
                .await?
        } else {
            SnapshotManifestReader::open_legacy(
                self.blocks.clone(),
                self.clone(),
                record.location.table(),
                snapshot_id,
                snapshot.manifests.clone(),
                *remaining,
            )?
        };
        let metadata = read_parquet_metadata(self.blocks.clone(), record, limits.parquet)
            .await
            .map_err(file_error)?;
        validate_partition_statistics_inventory(
            self.blocks.clone(),
            record,
            &metadata,
            &self.candidate,
            &mut reader,
            limits.partition_rows,
            work,
        )
        .await?;
        let summary = reader.finish()?;
        remaining.manifests = remaining
            .manifests
            .checked_sub(summary.manifests)
            .ok_or(Error::Bounds)?;
        remaining.entries = remaining
            .entries
            .checked_sub(summary.entries)
            .ok_or(Error::Bounds)?;
        remaining.manifest_bytes = remaining
            .manifest_bytes
            .checked_sub(summary.manifest_bytes)
            .ok_or(Error::Bounds)?;
        Ok(())
    }

    async fn statistics(
        &self,
        entry: &Value,
        record: &FileRecord,
        limits: CandidateAuxiliaryLimits,
        work: &mut usize,
    ) -> Result<usize, Error> {
        if record.format != ContentFormat::Puffin
            || entry.get("key-metadata").is_some_and(|value| !value.is_null())
        {
            return Err(Error::Binding);
        }
        let footer = probe_puffin_footer(self.blocks.clone(), record)
            .await
            .map_err(file_error)?;
        if entry["file-footer-size-in-bytes"].as_u64() != footer.payload.length.checked_add(16) {
            return Err(Error::Binding);
        }
        let metadata = read_puffin_metadata(
            self.blocks.clone(),
            record,
            limits.puffin_encoded_bytes,
            limits.puffin_decoded_bytes,
        )
        .await
        .map_err(file_error)?;
        let descriptors = entry["blob-metadata"].as_array().ok_or(Error::Binding)?;
        let mut selected = vec![false; metadata.blobs.len()];
        for descriptor in descriptors {
            let mut found = false;
            for (index, blob) in metadata.blobs.iter().enumerate() {
                charge(work)?;
                if !selected[index] && matches_blob(descriptor, blob, work)? {
                    selected[index] = true;
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(Error::Binding);
            }
        }
        Ok(descriptors.len())
    }
}

fn matches_blob(descriptor: &Value, blob: &PuffinBlob, work: &mut usize) -> Result<bool, Error> {
    if descriptor["type"].as_str() != Some(blob.kind.as_str())
        || descriptor["snapshot-id"].as_i64() != Some(blob.snapshot_id)
        || descriptor["sequence-number"].as_i64() != Some(blob.sequence_number)
    {
        return Ok(false);
    }
    let fields = descriptor["fields"].as_array().ok_or(Error::Binding)?;
    if fields.len() != blob.fields.len() {
        return Ok(false);
    }
    for (field, expected) in fields.iter().zip(&blob.fields) {
        charge(work)?;
        if field.as_i64() != Some(i64::from(*expected)) {
            return Ok(false);
        }
    }
    if let Some(properties) = descriptor.get("properties") {
        for (name, value) in properties.as_object().ok_or(Error::Binding)? {
            charge(work)?;
            if blob.properties.get(name).map(String::as_str) != value.as_str() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn charge(work: &mut usize) -> Result<(), Error> {
    *work = work.checked_sub(1).ok_or(Error::Bounds)?;
    Ok(())
}
