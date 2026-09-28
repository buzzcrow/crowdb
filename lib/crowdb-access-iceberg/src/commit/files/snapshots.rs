use std::sync::Arc;

use super::CandidateFileSource;
use crate::{
    manifest::{
        read_parquet_selection, validate_parquet_schema, validate_snapshot_delete_preservation,
        validate_snapshot_files, EntryStatus, FileContentKind, ManifestContext, ManifestVersion,
        ParquetFieldMapping, ParquetSelection, SnapshotDvScope, SnapshotFileLimits, SnapshotManifestReader,
        SnapshotManifestSummary, SnapshotValidationError as Error, SnapshotValidationInput,
    },
    table::{TableMetadataDocument, TableSnapshot},
};

#[derive(Clone, Copy, Debug)]
pub struct CandidateSnapshotLimits {
    pub snapshots: usize,
    pub entries: u64,
    pub manifest_bytes: u64,
    pub ranges: usize,
    pub files: SnapshotFileLimits,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CandidateSnapshotSummary {
    pub snapshots: usize,
    pub manifests: u64,
    pub entries: u64,
    pub manifest_bytes: u64,
    pub data_files: u64,
    pub data_rows: u64,
}

impl CandidateFileSource {
    /// Validates every retained candidate snapshot and its current-schema Parquet projection.
    /// New child snapshots also preserve prior DVs and positions absorbed into replacement DVs.
    /// Auxiliary statistics, ordered metadata evaluation and publication remain separate phases.
    /// # Errors
    /// Rejects missing parent history, unsupported data formats, incompatible files and exceeded budgets.
    pub async fn validate_snapshots(
        self: Arc<Self>,
        prior: &TableMetadataDocument,
        limits: CandidateSnapshotLimits,
    ) -> Result<CandidateSnapshotSummary, Error> {
        if !self
            .fence
            .prior()
            .is_some_and(|source| prior.selected_head() == &source.selected().head)
        {
            return Err(Error::Binding);
        }
        self.validate_snapshot_set(Some(prior), &limits).await
    }

    /// Validates all initial snapshots, including parent-child DV checks within a staged transaction.
    /// # Errors
    /// Rejects missing reservation authority, foreign files, missing parents and exhausted limits.
    pub async fn validate_initial_snapshots(
        self: Arc<Self>,
        limits: CandidateSnapshotLimits,
    ) -> Result<CandidateSnapshotSummary, Error> {
        if self.fence.prior().is_some() {
            return Err(Error::Binding);
        }
        self.validate_snapshot_set(None, &limits).await
    }

    async fn validate_snapshot_set(
        self: Arc<Self>,
        prior: Option<&TableMetadataDocument>,
        limits: &CandidateSnapshotLimits,
    ) -> Result<CandidateSnapshotSummary, Error> {
        self.ensure_current().await?;
        if limits.snapshots == 0
            || limits.snapshots > 100_000
            || self.candidate.snapshots().len() > limits.snapshots
            || limits.entries == 0
            || limits.manifest_bytes == 0
            || !(1..=1_000_000).contains(&limits.ranges)
        {
            return Err(Error::Bounds);
        }
        let current = self.current_context()?;
        let mapping = self
            .candidate
            .parquet_field_mapping(
                crate::table::TableMetadataLimits {
                    bytes: 64 * 1024 * 1024,
                    values: 1_000_000,
                    depth: 64,
                    string_bytes: 64 * 1024 * 1024,
                    collection_entries: 100_000,
                },
                1_000_000,
            )
            .map_err(super::file_error)?;
        let mut summary = CandidateSnapshotSummary::default();
        let mut remaining = *limits;
        for snapshot in self.candidate.snapshots().values() {
            let files = remaining.file_limits()?;
            if snapshot.manifest_list.is_some() {
                let input = self.input(snapshot, mapping.clone()).await?;
                let checked = validate_snapshot_files(self.blocks.clone(), input, files).await?;
                remaining.charge(checked.manifests)?;
                if !prior.is_some_and(|prior| prior.snapshots().contains_key(&snapshot.snapshot_id)) {
                    self.preserve(prior, snapshot, mapping.clone(), &mut remaining)
                        .await?;
                }
            }
            let (manifests, count, rows) = self.project(snapshot, &current, mapping.as_ref(), files).await?;
            if snapshot.manifest_list.is_none() {
                remaining.charge(manifests)?;
            }
            summary.snapshots += 1;
            summary.manifests = summary
                .manifests
                .checked_add(manifests.manifests)
                .ok_or(Error::Bounds)?;
            summary.entries = summary
                .entries
                .checked_add(manifests.entries)
                .ok_or(Error::Bounds)?;
            summary.manifest_bytes = summary
                .manifest_bytes
                .checked_add(manifests.manifest_bytes)
                .ok_or(Error::Bounds)?;
            summary.data_files = summary.data_files.checked_add(count).ok_or(Error::Bounds)?;
            summary.data_rows = summary.data_rows.checked_add(rows).ok_or(Error::Bounds)?;
        }
        self.ensure_current().await?;
        Ok(summary)
    }

    fn current_context(&self) -> Result<ManifestContext, Error> {
        self.candidate
            .current_manifest_context(1_000_000)
            .map_err(super::file_error)
    }

    async fn input(
        self: &Arc<Self>,
        snapshot: &TableSnapshot,
        mapping: Option<ParquetFieldMapping>,
    ) -> Result<SnapshotValidationInput, Error> {
        let version = match self.candidate.selected_head().format_version {
            1 => ManifestVersion::V1,
            2 => ManifestVersion::V2,
            3 => ManifestVersion::V3,
            _ => return Err(Error::Binding),
        };
        let selection = snapshot.manifest_selection(version).map_err(super::file_error)?;
        let list = self.load(&selection.location).await?;
        Ok(SnapshotValidationInput {
            scope: SnapshotDvScope {
                context: self.context,
                table: list.location.table(),
                snapshot_id: snapshot.snapshot_id,
                sequence: snapshot.sequence,
                manifest_list: list.file,
            },
            list,
            selection,
            manifests: self.clone(),
            files: self.clone(),
            mapping,
        })
    }

    async fn preserve(
        self: &Arc<Self>,
        prior: Option<&TableMetadataDocument>,
        snapshot: &TableSnapshot,
        mapping: Option<ParquetFieldMapping>,
        remaining: &mut CandidateSnapshotLimits,
    ) -> Result<(), Error> {
        let Some(parent_id) = snapshot.parent_snapshot_id else {
            return Ok(());
        };
        let parent = prior
            .and_then(|prior| prior.snapshots().get(&parent_id))
            .or_else(|| self.candidate.snapshots().get(&parent_id))
            .ok_or(Error::Binding)?;
        if parent.manifest_list.is_none() || snapshot.sequence == 0 {
            return Ok(());
        }
        let parent = self.input(parent, mapping.clone()).await?;
        let parent_summary =
            validate_snapshot_files(self.blocks.clone(), parent, remaining.file_limits()?).await?;
        remaining.charge(parent_summary.manifests)?;
        let parent = prior
            .and_then(|prior| prior.snapshots().get(&parent_id))
            .or_else(|| self.candidate.snapshots().get(&parent_id))
            .ok_or(Error::Binding)?;
        let parent = self.input(parent, mapping.clone()).await?;
        let child = self.input(snapshot, mapping).await?;
        Box::pin(validate_snapshot_delete_preservation(
            self.blocks.clone(),
            &parent,
            &child,
            remaining.files,
            remaining.ranges,
        ))
        .await?;
        Ok(())
    }

    async fn project(
        self: &Arc<Self>,
        snapshot: &TableSnapshot,
        current: &ManifestContext,
        mapping: Option<&ParquetFieldMapping>,
        limits: SnapshotFileLimits,
    ) -> Result<(SnapshotManifestSummary, u64, u64), Error> {
        let mut reader = if snapshot.manifest_list.is_some() {
            let input = self.input(snapshot, None).await?;
            SnapshotManifestReader::open(
                self.blocks.clone(),
                self.clone(),
                input.list,
                input.selection,
                limits.manifests,
            )
            .await?
        } else {
            SnapshotManifestReader::open_legacy(
                self.blocks.clone(),
                self.clone(),
                self.candidate.selected_head().metadata_location.table(),
                snapshot.snapshot_id,
                snapshot.manifests.clone(),
                limits.manifests,
            )?
        };
        let mut count = 0_u64;
        let mut rows = 0_u64;
        while let Some(entry) = reader.next_entry().await? {
            if entry.entry.status == EntryStatus::Deleted || entry.entry.content != FileContentKind::Data {
                continue;
            }
            count = count
                .checked_add(1)
                .filter(|count| *count <= limits.data_files as u64)
                .ok_or(Error::Bounds)?;
            let writer = reader.current_manifest().ok_or(Error::Binding)?.1;
            let record = self.load(&entry.file.location).await?;
            let selection = ParquetSelection {
                entry: &entry,
                context: writer,
                table: record.location.table(),
                mapping,
            };
            let (metadata, _) = read_parquet_selection(
                self.blocks.clone(),
                &record,
                &selection,
                limits.position_deletes.metadata,
            )
            .await?;
            let projection = current
                .clone()
                .with_schema_history(std::slice::from_ref(writer))
                .map_err(super::file_error)?;
            validate_parquet_schema(&metadata, &projection, &entry, mapping)?;
            rows = rows.checked_add(metadata.rows).ok_or(Error::Bounds)?;
        }
        Ok((reader.finish()?, count, rows))
    }
}

impl CandidateSnapshotLimits {
    fn file_limits(self) -> Result<SnapshotFileLimits, Error> {
        if self.entries == 0 || self.manifest_bytes == 0 {
            return Err(Error::Bounds);
        }
        let mut limits = self.files;
        limits.manifests.entries = limits.manifests.entries.min(self.entries);
        limits.manifests.manifest_bytes = limits.manifests.manifest_bytes.min(self.manifest_bytes);
        Ok(limits)
    }

    fn charge(&mut self, summary: SnapshotManifestSummary) -> Result<(), Error> {
        self.entries = self.entries.checked_sub(summary.entries).ok_or(Error::Bounds)?;
        self.manifest_bytes = self
            .manifest_bytes
            .checked_sub(summary.manifest_bytes)
            .ok_or(Error::Bounds)?;
        Ok(())
    }
}
