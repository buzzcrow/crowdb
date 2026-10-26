use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use super::{TableHead, TableMetadataDocument, TableMetadataError, TableMetadataLimits, TableRepository};
use crate::{
    catalog::{check_context, Capabilities, CatalogContext, CatalogError, CatalogStore, FormatAction},
    file::FileBlockStore,
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

/// Each call brackets its binding reads with fresh catalog availability checks.
/// Replacement advances the activation epoch; same-context activation only adds capabilities.
pub struct TableLoader {
    store: Arc<dyn CatalogStore>,
    namespaces: NamespaceRepository,
    tables: TableRepository,
    blocks: Arc<dyn FileBlockStore>,
    limits: TableMetadataLimits,
}

impl TableLoader {
    #[must_use]
    pub fn new<Store: NamespaceStore + 'static>(
        store: Arc<Store>,
        blocks: Arc<dyn FileBlockStore>,
        limits: TableMetadataLimits,
    ) -> Self {
        Self {
            store: store.clone(),
            namespaces: NamespaceRepository::new(store.clone()),
            tables: TableRepository::new(store),
            blocks,
            limits,
        }
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
        check_context(self.store.as_ref(), context).await?;
        let selected = self.head_in_context(context, namespace, name).await?;
        check_context(self.store.as_ref(), context).await?;
        Ok(selected)
    }

    async fn head_in_context(
        &self,
        context: CatalogContext,
        namespace: &NamespaceIdentifier,
        name: &str,
    ) -> Result<Option<TableHead>, TableLoadError> {
        let Some(parent) = self.namespaces.resolve(context, namespace).await? else {
            return Ok(None);
        };
        let selected = self.tables.resolve(context, parent.namespace, name).await?;
        if let Some(selected) = &selected {
            self.tables.ensure_current_in_context(context, selected).await?;
        }
        self.namespaces
            .ensure_current_in_context(context, &parent)
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
        check_context(self.store.as_ref(), context).await?;
        let result = self
            .load_in_context(context, namespace, name, mode, if_none_match, capabilities)
            .await?;
        check_context(self.store.as_ref(), context).await?;
        Ok(result)
    }

    async fn load_in_context(
        &self,
        context: CatalogContext,
        namespace: &NamespaceIdentifier,
        name: &str,
        mode: SnapshotLoadingMode,
        if_none_match: Option<&str>,
        capabilities: Option<Capabilities>,
    ) -> Result<TableLoad, TableLoadError> {
        let Some(parent) = self.namespaces.resolve(context, namespace).await? else {
            return Ok(TableLoad::Missing);
        };
        let Some(selected) = self.tables.resolve(context, parent.namespace, name).await? else {
            self.namespaces
                .ensure_current_in_context(context, &parent)
                .await?;
            return Ok(TableLoad::Missing);
        };
        if capabilities
            .is_some_and(|profile| !profile.supports(selected.head.format_version, FormatAction::Read))
        {
            return Err(TableLoadError::UnsupportedVersion);
        }
        let canonical =
            super::metadata::read_table_metadata_bytes(self.blocks.clone(), &selected, self.limits).await?;
        let etag = etag(&selected.head, mode);
        let document = TableMetadataDocument::parse(canonical, &selected.head, self.limits)?;
        let metadata = representation(document, mode)?;
        if metadata.len() > self.limits.bytes {
            return Err(TableMetadataError::Bounds.into());
        }
        self.tables.ensure_current_in_context(context, &selected).await?;
        self.namespaces
            .ensure_current_in_context(context, &parent)
            .await?;
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
    document: TableMetadataDocument,
    mode: SnapshotLoadingMode,
) -> Result<Vec<u8>, TableMetadataError> {
    if mode == SnapshotLoadingMode::All {
        return Ok(document.into_canonical());
    }
    let Some(snapshots) = document
        .fields()
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
    else {
        return Ok(document.into_canonical());
    };
    let mut referenced: BTreeSet<_> = document.current_snapshot().into_iter().collect();
    if let Some(refs) = document
        .fields()
        .get("refs")
        .and_then(serde_json::Value::as_object)
    {
        for reference in refs.values() {
            if let Some(id) = reference["snapshot-id"].as_i64() {
                referenced.insert(id);
            }
        }
    }
    let retained: Vec<_> = snapshots
        .iter()
        .map(|snapshot| {
            snapshot["snapshot-id"]
                .as_i64()
                .is_some_and(|id| referenced.contains(&id))
        })
        .collect();
    let canonical = document.into_canonical();
    // Validation already decoded snapshot identities and references. Decode raw
    // spans only to preserve each retained value's original JSON representation.
    let mut fields: BTreeMap<&str, &RawValue> = serde_json::from_slice(&canonical)?;
    let snapshots: Vec<&RawValue> = serde_json::from_str(fields["snapshots"].get())?;
    let selected: Vec<_> = snapshots
        .into_iter()
        .zip(retained)
        .filter_map(|(snapshot, keep)| keep.then_some(snapshot))
        .collect();
    let selected = serde_json::value::to_raw_value(&selected)?;
    fields.insert("snapshots", &selected);
    Ok(serde_json::to_vec(&fields)?)
}
