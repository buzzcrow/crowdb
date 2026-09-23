use std::collections::BTreeMap;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::catalog::{CasOutcome, CatalogStore};
use crate::file::{ContentFormat, FileBlockStore, FileIoError, FileKind, FileReader, FileRecord};
use crate::operation::mutation_identity;

use super::model::{Child, Fields, Root, MAX_CHILDREN};
use super::{ProjectionIdentity, MAX_PROJECTION_BYTES, PROJECTION_PAGE_BYTES};

pub enum MetadataRead {
    Selected(BTreeMap<String, Vec<u8>>),
    Canonical(Box<FileReader>),
}

pub struct ProjectionStore {
    store: Arc<dyn CatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
}

impl ProjectionStore {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>, blocks: Arc<dyn FileBlockStore>) -> Self {
        Self { store, blocks }
    }

    /// Builds optional children from already sealed canonical JSON, publishing the root last.
    /// False means optimization was skipped or failed, never a file-publication failure.
    pub async fn put(&self, record: &FileRecord, generation: u64, canonical: &[u8]) -> bool {
        if !eligible(record)
            || canonical.len() as u64 != record.length
            || <[u8; 32]>::from(Sha256::digest(canonical)) != record.digest
            || record.validate().is_err()
        {
            return false;
        }
        let Ok(fields) = serde_json::from_slice::<Fields<'_>>(canonical) else {
            return false;
        };
        let identity = ProjectionIdentity::new(record, generation);
        let Some(root) = Root::new(identity, record.length, &fields) else {
            return false;
        };
        let Some(encoded) = root.encode() else {
            return false;
        };
        for (name, child) in &root.children {
            for (page, bytes) in fields.0[name]
                .get()
                .as_bytes()
                .chunks(PROJECTION_PAGE_BYTES)
                .enumerate()
            {
                let Ok(page) = u16::try_from(page) else {
                    return false;
                };
                if !self.put_page(identity, child.index, page, bytes).await {
                    return false;
                }
            }
        }
        self.put_page(identity, 0, 0, &encoded).await
    }

    /// Returns exact child JSON bytes, or a fresh stream of the unchanged complete file.
    /// The caller supplies the selected generation's authoritative file record.
    /// An empty selection requests the complete canonical file without consulting projections.
    /// # Errors
    /// Only invalid canonical records fail here; projection failures always fall back.
    pub async fn select(
        &self,
        record: FileRecord,
        generation: u64,
        names: &[&str],
    ) -> Result<MetadataRead, FileIoError> {
        if record.kind != FileKind::Metadata || record.format != ContentFormat::Json {
            return Err(FileIoError::Bounds);
        }
        record.validate()?;
        if eligible(&record) && !names.is_empty() && names.len() <= MAX_CHILDREN {
            if let Some(selected) = self.selected(&record, generation, names).await {
                return Ok(MetadataRead::Selected(selected));
            }
        }
        Ok(MetadataRead::Canonical(Box::new(FileReader::new(
            self.blocks.clone(),
            record,
            None,
            PROJECTION_PAGE_BYTES,
        )?)))
    }

    async fn selected(
        &self,
        record: &FileRecord,
        generation: u64,
        names: &[&str],
    ) -> Option<BTreeMap<String, Vec<u8>>> {
        let identity = ProjectionIdentity::new(record, generation);
        let encoded = self.store.get(&identity.key(0, 0)?).await.ok()??;
        let root = Root::decode(&encoded.bytes, identity, record.length)?;
        let mut selected = BTreeMap::new();
        for name in names {
            if selected.contains_key(*name) {
                continue;
            }
            let child = root.children.get(*name)?;
            let bytes = self.read_child(identity, child).await?;
            selected.insert((*name).to_owned(), bytes);
        }
        Some(selected)
    }

    async fn read_child(&self, identity: ProjectionIdentity, child: &Child) -> Option<Vec<u8>> {
        let mut bytes = Vec::with_capacity(child.length);
        for page in 0..child.length.div_ceil(PROJECTION_PAGE_BYTES) {
            let key = identity.key(child.index, u16::try_from(page).ok()?)?;
            let stored = self.store.get(&key).await.ok()??;
            if stored.bytes.len() != (child.length - bytes.len()).min(PROJECTION_PAGE_BYTES) {
                return None;
            }
            bytes.extend_from_slice(&stored.bytes);
        }
        (<[u8; 32]>::from(Sha256::digest(&bytes)) == child.digest).then_some(bytes)
    }

    async fn put_page(&self, identity: ProjectionIdentity, child: u16, page: u16, bytes: &[u8]) -> bool {
        let Some(key) = identity.key(child, page) else {
            return false;
        };
        match self
            .store
            .compare_exchange(&key, None, bytes, mutation_identity(&key, None, bytes))
            .await
        {
            Ok(CasOutcome::Applied(_)) => true,
            Ok(CasOutcome::Conflict(Some(existing))) => existing.bytes == bytes,
            _ => false,
        }
    }
}

fn eligible(record: &FileRecord) -> bool {
    record.kind == FileKind::Metadata
        && record.format == ContentFormat::Json
        && record.length <= MAX_PROJECTION_BYTES as u64
}
