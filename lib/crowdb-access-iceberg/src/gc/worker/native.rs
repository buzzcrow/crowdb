use crate::key::{StorageKey, TableStorageScope};

use super::{
    CatalogError, GcPhase, GcScan, GcStalledReason, GcTask, GcTaskKind, GcWorkError, GcWorker, StorageRecord,
    ValidationError,
};

impl GcWorker {
    pub(super) async fn native_roots(&self, task: &GcTask, now_ms: u64) -> Result<GcTask, GcWorkError> {
        if task.kind == GcTaskKind::LiveTable {
            self.verify_live_head(task).await?;
        } else {
            self.verify_inactive(task).await?;
        }
        let scan = GcScan {
            native: true,
            catalog: task.context.catalog,
            scope: None,
            prefix: task
                .head
                .as_ref()
                .and_then(|head| head.storage_location().storage_path())
                .map_or_else(Vec::new, |table| table.to_string().into_bytes()),
            after: task.scan_after.clone(),
            items: usize::from(self.limits.page_items),
            bytes: self.limits.step_bytes.min(16 * 1024 * 1024) as usize,
        };
        let page = self
            .repository
            .store
            .scan_gc(scan.clone())
            .await
            .map_err(CatalogError::from)?;
        scan.validate_page(&page).map_err(CatalogError::from)?;
        let mut next = task.progress()?;
        for item in &page.items {
            let StorageKey::NativeTable(key) = StorageKey::decode(&item.key)? else {
                return Err(ValidationError::Key.into());
            };
            if key.scope != TableStorageScope::Operation {
                continue;
            }
            let StorageRecord::NativeInjection(receipt) =
                StorageRecord::decode_storage(&StorageKey::NativeTable(key), &item.value)?
            else {
                return Err(ValidationError::Record.into());
            };
            if receipt.retained(now_ms) {
                next.stalled = GcStalledReason::Protected;
                next.scan_after.clear();
                next.retry_at_ms = receipt
                    .identity
                    .issued_ms
                    .saturating_add(crate::operation::RETRY_WINDOW_MS)
                    .saturating_add(1);
                if task.kind == GcTaskKind::LiveTable && task.fenced {
                    self.repository.release_table_fence(task).await?;
                    next.fenced = false;
                    next.phase = GcPhase::Complete;
                } else {
                    next.phase = GcPhase::Waiting;
                }
                self.repository.update(task, &next).await?;
                return Ok(next);
            }
            self.delete_exact(&item.key, &item.value).await?;
        }
        if let Some(last) = page.items.last() {
            next.scan_after.clone_from(&last.key);
        } else {
            next.scan_after.clear();
            next.phase = if task.kind == GcTaskKind::LiveTable {
                next.proof.complete = false;
                GcPhase::Mark
            } else {
                GcPhase::Rescan
            };
        }
        self.repository.update(task, &next).await?;
        Ok(next)
    }
}
