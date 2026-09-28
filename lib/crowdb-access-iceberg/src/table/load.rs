use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use super::{TableHead, TableMetadataDocument, TableMetadataError, TableMetadataLimits, TableRepository};
use crate::{
    catalog::{Capabilities, CatalogContext, CatalogError, FormatAction},
    file::FileBlockStore,
    metadata_projection::ProjectionStore,
    namespace::{NamespaceIdentifier, NamespaceRepository, NamespaceStore},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotLoadingMode {
    All,
    Refs,
}

#[derive(Debug)]
pub enum TableLoad {
    Missing,
    NotModified {
        etag: String,
    },
    Loaded {
        head: Box<TableHead>,
        etag: String,
        metadata: Vec<u8>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum TableLoadError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Metadata(#[from] TableMetadataError),
    #[error("the selected table version is not enabled for reading")]
    UnsupportedVersion,
}

pub struct TableLoader {
    namespaces: NamespaceRepository,
    tables: TableRepository,
    blocks: Arc<dyn FileBlockStore>,
    limits: TableMetadataLimits,
    projections: ProjectionStore,
    pins: crate::gc::ReaderPins,
    pin_lifetime_ms: Option<u64>,
    #[cfg(feature = "test-util")]
    projection_hits: std::sync::atomic::AtomicUsize,
}

impl TableLoader {
    #[must_use]
    pub fn new<Store: NamespaceStore + 'static>(
        store: Arc<Store>,
        blocks: Arc<dyn FileBlockStore>,
        limits: TableMetadataLimits,
    ) -> Self {
        Self {
            pins: crate::gc::ReaderPins::new(store.clone()),
            pin_lifetime_ms: None,
            namespaces: NamespaceRepository::new(store.clone()),
            projections: ProjectionStore::new(store.clone(), blocks.clone()),
            tables: TableRepository::new(store),
            blocks,
            limits,
            #[cfg(feature = "test-util")]
            projection_hits: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// # Errors
    /// Rejects zero or excessive request protection lifetimes.
    pub fn with_reader_pins(mut self, lifetime_ms: u64) -> Result<Self, crate::error::ValidationError> {
        if lifetime_ms == 0 || lifetime_ms > 24 * 60 * 60 * 1000 {
            return Err(crate::error::ValidationError::Deadline);
        }
        self.pin_lifetime_ms = Some(lifetime_ms);
        Ok(self)
    }

    #[must_use]
    pub fn with_catalog_reader_pins(mut self) -> Self {
        self.pin_lifetime_ms = Some(0);
        self
    }

    /// Resolves live namespace identity before table selection. No table publisher is implied.
    /// # Errors
    /// Corruption, retirement and changed namespace/head identity remain errors, not absence.
    pub async fn exists(
        &self,
        context: CatalogContext,
        namespace: &NamespaceIdentifier,
        name: &str,
    ) -> Result<bool, TableLoadError> {
        Ok(self.head(context, namespace, name).await?.is_some())
    }

    /// Resolves the selected head without materializing metadata bytes.
    /// # Errors
    /// Corruption, retirement and changed namespace/head identity remain errors.
    pub async fn head(
        &self,
        context: CatalogContext,
        namespace: &NamespaceIdentifier,
        name: &str,
    ) -> Result<Option<TableHead>, TableLoadError> {
        let Some(parent) = self.namespaces.load(context, namespace).await? else {
            return Ok(None);
        };
        let selected = self.tables.select(context, parent.namespace, name).await?;
        if let Some(selected) = &selected {
            self.tables.ensure_current(context, selected).await?;
        }
        self.check_namespace(context, namespace, parent.namespace, parent.name_epoch)
            .await?;
        Ok(selected.map(|value| value.head))
    }

    /// Builds a bounded read representation, never a commit-validation proof.
    /// Always validates canonical bytes; disposable projections cannot bypass validation.
    /// `ALL` preserves exact bytes and `REFS` preserves raw values outside snapshots.
    /// # Errors
    /// Rejects corruption, resource exhaustion and namespace/head changes during the read.
    pub async fn load(
        &self,
        context: CatalogContext,
        namespace: &NamespaceIdentifier,
        name: &str,
        mode: SnapshotLoadingMode,
        if_none_match: Option<&str>,
    ) -> Result<TableLoad, TableLoadError> {
        self.load_inner(context, namespace, name, mode, if_none_match, None)
            .await
    }

    /// Reads canonical metadata only for an enabled selected format version.
    /// # Errors
    /// Rejects disabled versions before file I/O, corruption and changed bindings.
    pub async fn load_with_capabilities(
        &self,
        context: CatalogContext,
        namespace: &NamespaceIdentifier,
        name: &str,
        mode: SnapshotLoadingMode,
        capabilities: Capabilities,
    ) -> Result<TableLoad, TableLoadError> {
        self.load_inner(context, namespace, name, mode, None, Some(capabilities))
            .await
    }

    async fn load_inner(
        &self,
        context: CatalogContext,
        namespace: &NamespaceIdentifier,
        name: &str,
        mode: SnapshotLoadingMode,
        if_none_match: Option<&str>,
        capabilities: Option<Capabilities>,
    ) -> Result<TableLoad, TableLoadError> {
        if if_none_match.is_some_and(|value| value.len() > 8192) {
            return Err(TableMetadataError::Bounds.into());
        }
        let Some(parent) = self.namespaces.load(context, namespace).await? else {
            return Ok(TableLoad::Missing);
        };
        let Some(selected) = self.tables.select(context, parent.namespace, name).await? else {
            self.check_namespace(context, namespace, parent.namespace, parent.name_epoch)
                .await?;
            return Ok(TableLoad::Missing);
        };
        if capabilities
            .is_some_and(|profile| !profile.supports(selected.head.format_version, FormatAction::Read))
        {
            return Err(TableLoadError::UnsupportedVersion);
        }
        let pin = if let Some(lifetime_ms) = self.pin_lifetime_ms {
            let now_ms = u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| TableMetadataError::Bounds)?
                    .as_millis(),
            )
            .map_err(|_| TableMetadataError::Bounds)?;
            let pin = crate::gc::GcPin {
                context,
                identity: crate::key::OperationId::random(),
                head: selected.head.clone(),
                principal: "catalog-metadata-reader".into(),
                expires_ms: if lifetime_ms == 0 {
                    self.pins.request_expiry(context, now_ms).await?
                } else {
                    now_ms
                        .checked_add(lifetime_ms)
                        .ok_or(TableMetadataError::Bounds)?
                },
                released: false,
                operator: false,
                protects_uploads: false,
            };
            self.pins.acquire(&pin).await?;
            Some(pin)
        } else {
            None
        };
        let canonical =
            super::metadata::read_table_metadata_bytes(self.blocks.clone(), &selected, self.limits).await?;
        let etag = etag(&selected.head, mode);
        let metadata = self.representation(&selected, canonical, mode).await?;
        if metadata.len() > self.limits.bytes {
            return Err(TableMetadataError::Bounds.into());
        }
        self.tables.ensure_current(context, &selected).await?;
        self.check_namespace(context, namespace, parent.namespace, parent.name_epoch)
            .await?;
        if let Some(pin) = &pin {
            self.pins.release(pin).await?;
        }
        if if_none_match.is_some_and(|header| {
            header.split(',').any(|tag| {
                let tag = tag.trim();
                tag == "*" || tag.strip_prefix("W/").unwrap_or(tag) == etag
            })
        }) {
            return Ok(TableLoad::NotModified { etag });
        }
        Ok(TableLoad::Loaded {
            head: Box::new(selected.head),
            etag,
            metadata,
        })
    }

    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn projection_hits_for_tests(&self) -> usize {
        self.projection_hits.load(std::sync::atomic::Ordering::Relaxed)
    }

    async fn representation(
        &self,
        selected: &super::SelectedTable,
        canonical: Vec<u8>,
        mode: SnapshotLoadingMode,
    ) -> Result<Vec<u8>, TableMetadataError> {
        if mode == SnapshotLoadingMode::Refs {
            if let Some(fields) = self
                .projections
                .document_fields(&selected.metadata, &selected.head, self.limits)
                .await
            {
                if let Ok(metadata) = projected_representation(&fields, &canonical) {
                    #[cfg(feature = "test-util")]
                    self.projection_hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(metadata);
                }
            }
        }
        let document = TableMetadataDocument::parse(canonical, &selected.head, self.limits)?;
        let metadata = representation(&document, mode)?;
        if mode == SnapshotLoadingMode::Refs {
            self.projections
                .put_document(&selected.metadata, &document, self.limits)
                .await;
        }
        Ok(metadata)
    }

    async fn check_namespace(
        &self,
        context: CatalogContext,
        identifier: &NamespaceIdentifier,
        namespace: crate::key::NamespaceId,
        epoch: u64,
    ) -> Result<(), CatalogError> {
        let current = self.namespaces.load(context, identifier).await?;
        if !current.is_some_and(|current| current.namespace == namespace && current.name_epoch == epoch) {
            return Err(CatalogError::Conflict);
        }
        Ok(())
    }
}

fn etag(head: &TableHead, mode: SnapshotLoadingMode) -> String {
    let mut digest = Sha256::new();
    digest.update(b"crowdb-iceberg-table-load-v1");
    digest.update(head.catalog.as_bytes());
    digest.update(head.table.as_bytes());
    digest.update(head.generation.to_be_bytes());
    digest.update(head.metadata_digest);
    digest.update([match mode {
        SnapshotLoadingMode::All => 0,
        SnapshotLoadingMode::Refs => 1,
    }]);
    format!("\"{}\"", data_encoding::HEXLOWER.encode(&digest.finalize()))
}

fn representation(
    document: &TableMetadataDocument,
    mode: SnapshotLoadingMode,
) -> Result<Vec<u8>, TableMetadataError> {
    if mode == SnapshotLoadingMode::All {
        return Ok(document.canonical().to_vec());
    }
    let fields: BTreeMap<&str, &RawValue> = serde_json::from_slice(document.canonical())?;
    refs_representation(fields, document.canonical())
}

fn projected_representation(
    fields: &BTreeMap<String, Vec<u8>>,
    canonical: &[u8],
) -> Result<Vec<u8>, TableMetadataError> {
    let fields = fields
        .iter()
        .map(|(name, bytes)| Ok((name.as_str(), serde_json::from_slice::<&RawValue>(bytes)?)))
        .collect::<Result<BTreeMap<_, _>, serde_json::Error>>()?;
    refs_representation(fields, canonical)
}

fn refs_representation(
    fields: BTreeMap<&str, &RawValue>,
    canonical: &[u8],
) -> Result<Vec<u8>, TableMetadataError> {
    let Some(snapshots) = fields.get("snapshots") else {
        return Ok(canonical.to_vec());
    };
    let snapshots: Vec<&RawValue> = serde_json::from_str(snapshots.get())?;
    let mut referenced = BTreeSet::new();
    if let Some(current) = fields.get("current-snapshot-id") {
        if let Some(current) = serde_json::from_str::<Option<i64>>(current.get())?.filter(|id| *id != -1) {
            referenced.insert(current);
        }
    }
    if let Some(refs) = fields.get("refs") {
        let refs: BTreeMap<&str, serde_json::Value> = serde_json::from_str(refs.get())?;
        for reference in refs.values() {
            if let Some(id) = reference["snapshot-id"].as_i64() {
                referenced.insert(id);
            }
        }
    }
    let mut selected = Vec::new();
    for snapshot in snapshots {
        #[derive(serde::Deserialize)]
        struct Identity {
            #[serde(rename = "snapshot-id")]
            snapshot_id: i64,
        }
        let identity: Identity = serde_json::from_str(snapshot.get())?;
        if referenced.contains(&identity.snapshot_id) {
            selected.push(snapshot);
        }
    }
    let selected = serde_json::value::to_raw_value(&selected)?;
    let mut fields: BTreeMap<_, _> = fields.into_iter().collect();
    fields.insert("snapshots", &selected);
    Ok(serde_json::to_vec(&fields)?)
}
