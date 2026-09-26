use crate::{catalog::RootState, key::SystemScope};

use super::{
    CatalogError, GcPhase, GcStalledReason, GcTask, GcTaskKind, GcWorkError, GcWorker, IcebergKey,
    StorageRecord, ValidationError,
};

impl GcWorker {
    pub(super) async fn scan_system_protection(
        &self,
        task: &GcTask,
        now_ms: u64,
    ) -> Result<GcTask, GcWorkError> {
        self.verify_inactive(task).await?;
        if task.kind != GcTaskKind::RetiredCatalog {
            return Err(CatalogError::Busy.into());
        }
        let scan = super::super::GcSystemScan {
            after: task.scan_after.clone(),
            items: usize::from(self.limits.page_items),
            bytes: self.limits.step_bytes.min(16 * 1024 * 1024) as usize,
        };
        let page = self
            .repository
            .store
            .scan_gc_system(scan.clone())
            .await
            .map_err(CatalogError::from)?;
        scan.validate_page(&page).map_err(CatalogError::from)?;
        let root_key = IcebergKey::System {
            scope: SystemScope::ActiveRoot,
            suffix: Vec::new(),
        };
        let value = self
            .repository
            .store
            .get(&root_key.encode()?)
            .await
            .map_err(CatalogError::from)?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::Active(root) = StorageRecord::decode(&root_key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if root.state != RootState::Ready || root.context.catalog == task.context.catalog {
            return Err(CatalogError::Busy.into());
        }
        for item in &page.items {
            let key = IcebergKey::decode(&item.key)?;
            let protected = match StorageRecord::decode(&key, &item.value)? {
                StorageRecord::Management(operation) => {
                    operation.id() != root.operation
                        && (operation.candidate == task.context.catalog
                            || operation.request.confirmation == Some(task.context.catalog))
                        && (!operation.terminal() || now_ms < operation.retained_until_ms)
                }
                StorageRecord::Retry(binding) => {
                    binding.context == task.context
                        && (binding.status == 0 || now_ms < binding.retained_until_ms)
                }
                _ => return Err(ValidationError::Record.into()),
            };
            if protected {
                let mut next = task.advance()?;
                next.phase = GcPhase::Waiting;
                next.scan_after.clear();
                next.stalled = GcStalledReason::Protected;
                next.retry_at_ms = now_ms
                    .checked_add(u64::from(self.limits.retry_base_ms))
                    .ok_or(ValidationError::Deadline)?;
                self.repository.update(task, &next).await?;
                return Ok(next);
            }
        }
        let mut next = task.progress()?;
        if let Some(last) = page.items.last() {
            next.scan_after.clone_from(&last.key);
        } else {
            next.scan_after.clear();
            next.phase = if task.phase == GcPhase::RootsSystem {
                GcPhase::Roots
            } else {
                GcPhase::Sweep
            };
        }
        self.repository.update(task, &next).await?;
        Ok(next)
    }
}
