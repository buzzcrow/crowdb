use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use super::{
    read_table_metadata_document, TableHead, TableMetadataDocument, TableMetadataError, TableMetadataLimits,
    TableRepository,
};
use crate::{
    catalog::{CatalogContext, CatalogError},
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
}

pub struct TableLoader {
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
        let Some(parent) = self.namespaces.load(context, namespace).await? else {
            return Ok(false);
        };
        let selected = self.tables.select(context, parent.namespace, name).await?;
        if let Some(selected) = &selected {
            self.tables.ensure_current(context, selected).await?;
        }
        self.check_namespace(context, namespace, parent.namespace, parent.name_epoch)
            .await?;
        Ok(selected.is_some())
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
        let document = read_table_metadata_document(self.blocks.clone(), &selected, self.limits).await?;
        let etag = etag(&selected.head, mode);
        let metadata = representation(&document, mode)?;
        if metadata.len() > self.limits.bytes {
            return Err(TableMetadataError::Bounds.into());
        }
        self.tables.ensure_current(context, &selected).await?;
        self.check_namespace(context, namespace, parent.namespace, parent.name_epoch)
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
    let mut fields: BTreeMap<&str, &RawValue> = serde_json::from_slice(document.canonical())?;
    let Some(snapshots) = fields.get("snapshots") else {
        return Ok(document.canonical().to_vec());
    };
    let snapshots: Vec<&RawValue> = serde_json::from_str(snapshots.get())?;
    let mut referenced: BTreeSet<i64> = document.current_snapshot().into_iter().collect();
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
    fields.insert("snapshots", &selected);
    Ok(serde_json::to_vec(&fields)?)
}
