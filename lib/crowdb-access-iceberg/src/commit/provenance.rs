use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;

mod scan;

use crate::{
    catalog::{CatalogContext, CatalogStore},
    file::{AvroBlocks, FileBlockStore, FileContent, FileKind, FileLocation, FileRecord, FileRepository},
    manifest::{
        ManifestContent, ManifestContext, ManifestListReader, ManifestMetadata, ManifestVersion,
        SnapshotManifestError as Error, SnapshotManifestLimits, SnapshotManifestSource,
    },
    table::{SelectedTable, TableLifecycle, TableMetadataDocument, TableRepository},
};

#[derive(Clone, Copy, Debug)]
pub struct PriorManifestLimits {
    pub snapshots: usize,
    pub references: u64,
    pub index_bytes: usize,
    pub manifests: SnapshotManifestLimits,
}

struct Anchor {
    record: FileRecord,
    spec_id: i32,
    content: ManifestContent,
}

/// Canonical manifest provenance from one currently selected metadata generation.
/// Only reachable immutable manifests may recover writer definitions absent from table history.
/// This is not candidate file validation or publication authority; the publisher still needs a head CAS.
pub struct PriorManifestSource {
    repository: TableRepository,
    context: CatalogContext,
    selected: SelectedTable,
    blocks: Arc<dyn FileBlockStore>,
    anchors: BTreeMap<String, Anchor>,
    limits: PriorManifestLimits,
    history: ManifestContext,
}

impl PriorManifestSource {
    /// Scans every retained canonical manifest list through EOF before returning provenance.
    /// No uploaded header can add a path to this index. Legacy embedded manifest paths
    /// are anchored directly in the same selected canonical metadata document.
    /// # Errors
    /// Rejects stale heads, missing authority, inconsistent immutable references and bounded-work excess.
    pub async fn build(
        store: Arc<dyn CatalogStore>,
        blocks: Arc<dyn FileBlockStore>,
        context: CatalogContext,
        selected: &SelectedTable,
        document: &TableMetadataDocument,
        limits: PriorManifestLimits,
    ) -> Result<Self, Error> {
        if !(1..=100_000).contains(&limits.snapshots)
            || limits.references == 0
            || limits.references > 1_000_000
            || limits.index_bytes == 0
            || limits.index_bytes > 256 * 1024 * 1024
            || document.snapshots().len() > limits.snapshots
            || limits.manifests.manifests == 0
            || limits.manifests.entries == 0
            || limits.manifests.manifest_bytes == 0
        {
            return Err(Error::Bounds);
        }
        if document.selected_head() != &selected.head
            || selected.head.lifecycle != TableLifecycle::Ready
            || selected.metadata.file != selected.head.metadata_file
            || selected.metadata.location != selected.head.metadata_location
            || selected.metadata.digest != selected.head.metadata_digest
        {
            return Err(Error::Unavailable);
        }
        let repository = TableRepository::new(store.clone());
        repository
            .ensure_current(context, selected)
            .await
            .map_err(source)?;
        let files = FileRepository::new(store);
        let mut result = Self {
            repository,
            context,
            selected: selected.clone(),
            blocks,
            anchors: BTreeMap::new(),
            limits,
            history: document.current_manifest_context(1_000_000).map_err(source)?,
        };
        result.scan(&files, document).await?;
        result
            .repository
            .ensure_current(context, selected)
            .await
            .map_err(source)?;
        Ok(result)
    }

    #[must_use]
    pub fn selected(&self) -> &SelectedTable {
        &self.selected
    }

    pub(crate) fn contains(&self, location: &FileLocation) -> bool {
        location.table().catalog == self.context.catalog
            && location.table().table == self.selected.head.table
            && self.anchors.contains_key(location.relative_key())
    }

    fn insert(
        &mut self,
        record: FileRecord,
        spec_id: i32,
        content: ManifestContent,
        retained: &mut usize,
    ) -> Result<(), Error> {
        let path = record.location.relative_key();
        if let Some(previous) = self.anchors.get(path) {
            if previous.record != record || previous.spec_id != spec_id || previous.content != content {
                return Err(Error::Unavailable);
            }
            return Ok(());
        }
        let payload = match &record.content {
            FileContent::Inline { bytes, .. } => bytes.capacity(),
            FileContent::Chunks { .. } => 0,
        };
        let bytes = std::mem::size_of::<Anchor>() + 128 + path.len() * 2 + payload;
        *retained = retained
            .checked_add(bytes)
            .filter(|bytes| *bytes <= self.limits.index_bytes)
            .ok_or(Error::Bounds)?;
        self.anchors.insert(
            path.to_owned(),
            Anchor {
                record,
                spec_id,
                content,
            },
        );
        Ok(())
    }
}

#[async_trait]
impl SnapshotManifestSource for PriorManifestSource {
    async fn resolve(&self, location: &FileLocation) -> Result<(FileRecord, ManifestContext), Error> {
        self.repository
            .ensure_current(self.context, &self.selected)
            .await
            .map_err(source)?;
        if location.table().catalog != self.selected.head.catalog
            || location.table().table != self.selected.head.table
        {
            return Err(Error::Unavailable);
        }
        let anchor = self
            .anchors
            .get(location.relative_key())
            .ok_or(Error::Unavailable)?;
        let mut reader = AvroBlocks::open(
            self.blocks.clone(),
            anchor.record.clone(),
            self.limits.manifests.framing,
        )
        .await
        .map_err(source)?;
        let metadata = ManifestMetadata::parse(reader.metadata()).map_err(source)?;
        let version = match metadata.version {
            ManifestVersion::V1 => 1,
            ManifestVersion::V2 => 2,
            ManifestVersion::V3 => 3,
        };
        if metadata.content != anchor.content || version > self.selected.head.format_version {
            return Err(Error::Unavailable);
        }
        let schema: serde_json::Value = serde_json::from_slice(metadata.schema_json).map_err(source)?;
        let schema_id = metadata
            .schema_id
            .or_else(|| {
                schema
                    .get("schema-id")
                    .and_then(serde_json::Value::as_i64)
                    .and_then(|value| i32::try_from(value).ok())
            })
            .unwrap_or(0);
        let context = ManifestContext::parse(
            metadata.version,
            schema_id,
            anchor.spec_id,
            metadata.schema_json,
            metadata.partition_spec_json,
        )
        .map_err(source)?;
        context
            .validate_metadata(metadata, anchor.spec_id)
            .map_err(source)?;
        let mut entries = 0_u64;
        if anchor.record.length > self.limits.manifests.manifest_bytes {
            return Err(Error::Bounds);
        }
        while let Some(block) = reader.next().await.map_err(source)? {
            entries = entries
                .checked_add(block.records)
                .filter(|count| *count <= self.limits.manifests.entries)
                .ok_or(Error::Bounds)?;
        }
        self.repository
            .ensure_current(self.context, &self.selected)
            .await
            .map_err(source)?;
        let context = context
            .with_schema_history(std::slice::from_ref(&self.history))
            .map_err(source)?;
        Ok((anchor.record.clone(), context))
    }
}

fn source(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Source(Box::new(error))
}

fn charge_bytes(current: u64, additional: u64, limit: u64) -> Result<u64, Error> {
    current
        .checked_add(additional)
        .filter(|bytes| *bytes <= limit)
        .ok_or(Error::Bounds)
}
