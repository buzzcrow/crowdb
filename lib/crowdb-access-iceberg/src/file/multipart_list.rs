use std::sync::Arc;

use crowdb_chunk_kv_client::MultiScanPage;

use crate::catalog::{CatalogError, StoreError};
use crate::error::ValidationError;
use crate::key::IcebergKey;
use crate::record::StorageRecord;

use super::{MultipartPart, MultipartPhase, MultipartRepository, MultipartSession};

mod scan;
pub use scan::{MultipartPartScan, MultipartPartStore};

pub struct MultipartLister {
    repository: MultipartRepository,
    store: Arc<dyn MultipartPartStore>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartPartPage {
    pub parts: Vec<MultipartPart>,
    pub next_marker: Option<u16>,
}

impl MultipartLister {
    #[must_use]
    pub fn new<Store: MultipartPartStore + 'static>(store: Arc<Store>) -> Self {
        Self {
            repository: MultipartRepository::new(store.clone()),
            store,
        }
    }

    /// Returns one storage-bounded page, possibly smaller than the requested maximum.
    /// # Errors
    /// Rejects expired/terminal sessions, stale snapshots, pending mutations and corrupt pages.
    pub async fn list(
        &self,
        session: &MultipartSession,
        after: u16,
        max_parts: u16,
        now_ms: u64,
    ) -> Result<MultipartPartPage, CatalogError> {
        session.validate()?;
        if max_parts == 0 || max_parts > 1000 {
            return Err(ValidationError::Record.into());
        }
        if now_ms < session.created_ms
            || now_ms >= session.expires_ms
            || !matches!(
                session.phase,
                MultipartPhase::Open | MultipartPhase::Completing | MultipartPhase::Publishing
            )
        {
            return Err(CatalogError::Conflict);
        }
        if session.pending.is_some() {
            return Err(CatalogError::Busy);
        }
        let scan = MultipartPartScan {
            catalog: session.context.catalog,
            upload: session.upload,
            after,
            limit: max_parts.min(256),
        };
        scan.request()?;
        self.check_snapshot(session).await?;
        let page = self.store.scan_multipart_parts(scan.clone()).await?;
        let page = decode_page(session, &scan, page)?;
        self.check_snapshot(session).await?;
        Ok(page)
    }

    async fn check_snapshot(&self, session: &MultipartSession) -> Result<(), CatalogError> {
        if self
            .repository
            .load(session.context, session.upload)
            .await?
            .as_ref()
            != Some(session)
        {
            return Err(CatalogError::Busy);
        }
        Ok(())
    }
}

fn decode_page(
    session: &MultipartSession,
    scan: &MultipartPartScan,
    page: MultiScanPage,
) -> Result<MultipartPartPage, CatalogError> {
    if let Some(failure) = page.terminal_failure {
        return Err(StoreError::Rejected(failure).into());
    }
    let request = scan.request()?;
    if page.items.len() > request.max_items {
        return Err(ValidationError::RecordTooLarge.into());
    }
    let start = request.start.as_ref().ok_or(ValidationError::Key)?;
    let end = request.end.as_ref().ok_or(ValidationError::Key)?;
    let mut last: Option<&Vec<u8>> = None;
    let mut bytes = 0_usize;
    let mut parts = Vec::with_capacity(page.items.len());
    for item in &page.items {
        bytes = bytes
            .saturating_add(item.key.len())
            .saturating_add(item.value.len());
        if item.key < *start
            || item.key >= *end
            || item.revision == 0
            || last.is_some_and(|last| item.key <= *last)
            || bytes > request.max_bytes
        {
            return Err(ValidationError::Record.into());
        }
        last = Some(&item.key);
        let key = IcebergKey::decode(&item.key)?;
        let StorageRecord::MultipartPart(part) = StorageRecord::decode(&key, &item.value)? else {
            return Err(ValidationError::Record.into());
        };
        part.validate_for(session)?;
        parts.push(*part);
    }
    let next_marker = if let Some(cursor) = page.continuation {
        if cursor.original_start != request.start
            || cursor.original_end != request.end
            || cursor.direction != request.direction
            || cursor.catalog_generation == 0
            || Some(&cursor.last_key) != last
        {
            return Err(ValidationError::Key.into());
        }
        Some(parts.last().ok_or(ValidationError::Record)?.number)
    } else {
        None
    };
    Ok(MultipartPartPage { parts, next_marker })
}
