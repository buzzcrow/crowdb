use crate::catalog::{check_context, CatalogError, StoreError};
use crate::error::ValidationError;
use crate::key::IcebergKey;
use crate::record::StorageRecord;

use super::{successor, FileListPage, FileListRequest, FileListTokens, FileLocationScan, ListedFile};
use crate::file::{FileGrant, FileOperation, FileRepository};

impl FileRepository {
    /// Lists selected published records in one authorized table/prefix interval.
    /// Each page observes a bounded live scan. Publication behind the cursor is
    /// seen only by a new traversal; deletion may remove not-yet-returned files.
    ///
    /// # Errors
    /// Rejects unauthorized, expired, cross-scope, malformed or corrupt scans.
    pub async fn list(
        &self,
        grant: &FileGrant,
        request: &FileListRequest,
        tokens: &FileListTokens,
        now_ms: u64,
    ) -> Result<FileListPage, CatalogError> {
        request.validate()?;
        if grant.context.catalog != request.table.catalog
            || grant.table != request.table.table
            || !grant.operations.allows(FileOperation::ListObjects)
            || now_ms < grant.issued_ms
            || now_ms >= grant.expires_ms
        {
            return Err(CatalogError::Forbidden);
        }
        let (lower, end) = request.bounds()?;
        let (mut start, expires) = if let Some(token) = &request.continuation_token {
            tokens.verify(grant, request, token, now_ms)?
        } else {
            let mut start = lower.clone();
            if let Some(after) = &request.start_after {
                let mut key = request.storage_key(after)?;
                key.push(0);
                start = start.max(key);
            }
            (start, now_ms.saturating_add(15 * 60 * 1000).min(grant.expires_ms))
        };
        if request.continuation_token.is_some() && (start < lower || start >= end) {
            return Err(ValidationError::Key.into());
        }
        check_context(self.store.as_ref(), grant.context).await?;
        let mut result = FileListPage {
            files: Vec::new(),
            common_prefixes: Vec::new(),
            next_continuation_token: None,
        };
        if request.max_keys == 0 || start >= end {
            return Ok(result);
        }
        let scan = FileLocationScan {
            listing: request.clone(),
            start: start.clone(),
        };
        let bounds = scan.request()?;
        let page = self.store.scan_file_locations(scan).await?;
        validate_page(&page, &bounds)?;
        let consumed = self.collect_page(&page, request, &mut start, &mut result).await?;
        let more = consumed < page.items.len() || page.continuation.is_some();
        if more && start < end {
            result.next_continuation_token = Some(tokens.issue(grant, request, &start, expires));
        }
        check_context(self.store.as_ref(), grant.context).await?;
        Ok(result)
    }
    async fn collect_page(
        &self,
        page: &crowdb_chunk_kv_client::MultiScanPage,
        request: &FileListRequest,
        start: &mut Vec<u8>,
        result: &mut FileListPage,
    ) -> Result<usize, CatalogError> {
        let mut consumed = 0;
        for item in &page.items {
            if item.key < *start {
                consumed += 1;
                continue;
            }
            if result.files.len() + result.common_prefixes.len() == usize::from(request.max_keys) {
                break;
            }
            consumed += 1;
            start.clone_from(&item.key);
            start.push(0);
            let key = IcebergKey::decode(&item.key)?;
            if matches!(
                StorageRecord::decode(&key, &item.value)?,
                StorageRecord::DeletedFile(_)
            ) {
                continue;
            }
            let IcebergKey::Catalog { suffix, .. } = key else {
                return Err(ValidationError::Key.into());
            };
            let relative = std::str::from_utf8(suffix.get(16..).ok_or(ValidationError::Key)?)
                .map_err(|_| ValidationError::Key)?;
            let location = request.table.file(relative)?;
            let record = self.resolve_value(&location, &item.value).await?;
            let object_key = location.object_key();
            if let Some(delimiter) = &request.delimiter {
                let rest = object_key
                    .strip_prefix(&request.prefix)
                    .ok_or(ValidationError::Key)?;
                if let Some(offset) = rest.find(delimiter) {
                    let common = object_key[..request.prefix.len() + offset + delimiter.len()].to_owned();
                    *start = successor(&request.storage_key(&common)?).ok_or(ValidationError::Key)?;
                    if request.start_after.as_ref().map_or(true, |after| common > *after) {
                        result.common_prefixes.push(common);
                    }
                    continue;
                }
            }
            result.files.push(ListedFile {
                key: object_key,
                length: record.length,
                etag: record
                    .content
                    .etag()
                    .map_or_else(|| data_encoding::HEXLOWER.encode(&record.digest), str::to_owned),
            });
        }
        Ok(consumed)
    }
}

fn validate_page(
    page: &crowdb_chunk_kv_client::MultiScanPage,
    request: &crowdb_chunk_kv_client::MultiScanRequest,
) -> Result<(), CatalogError> {
    if let Some(failure) = &page.terminal_failure {
        return Err(StoreError::Rejected(failure.clone()).into());
    }
    let start = request.start.as_ref().ok_or(ValidationError::Key)?;
    let end = request.end.as_ref().ok_or(ValidationError::Key)?;
    let mut last: Option<&Vec<u8>> = None;
    let mut bytes = 0_usize;
    if page.items.len() > request.max_items {
        return Err(ValidationError::RecordTooLarge.into());
    }
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
    }
    if let Some(cursor) = &page.continuation {
        if cursor.original_start != request.start
            || cursor.original_end != request.end
            || cursor.direction != request.direction
            || cursor.catalog_generation == 0
            || Some(&cursor.last_key) != last
        {
            return Err(ValidationError::Key.into());
        }
    }
    Ok(())
}
