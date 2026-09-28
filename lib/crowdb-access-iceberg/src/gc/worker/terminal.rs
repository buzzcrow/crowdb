use crate::key::CatalogScope;

use super::{
    CandidatePhase, CatalogError, GcPhase, GcScan, GcTask, GcWorkError, GcWorker, IcebergKey, StorageRecord,
    ValidationError,
};

impl GcWorker {
    pub(super) async fn verify_cleanup(&self, task: &GcTask) -> Result<GcTask, GcWorkError> {
        self.verify_inactive(task).await?;
        let scan = self.terminal_scan(task);
        let page = self
            .repository
            .store
            .scan_gc(scan.clone())
            .await
            .map_err(CatalogError::from)?;
        scan.validate_page(&page).map_err(CatalogError::from)?;
        for item in &page.items {
            let key = IcebergKey::decode(&item.key)?;
            verify_terminal_record(&key, &item.value)?;
        }
        let mut next = task.progress()?;
        if let Some(last) = page.items.last() {
            next.scan_after.clone_from(&last.key);
        } else {
            next.scan_after.clear();
            next.phase = GcPhase::CleanupGc;
            self.repository
                .change(
                    &GcTask::retirement_key(task.context.catalog),
                    None,
                    &StorageRecord::GcTask(Box::new(next.clone())),
                )
                .await?;
        }
        self.repository.update(task, &next).await?;
        Ok(next)
    }

    pub(super) async fn cleanup_gc(&self, task: &GcTask) -> Result<GcTask, GcWorkError> {
        self.verify_inactive(task).await?;
        if self
            .repository
            .retirement(task.context.catalog)
            .await?
            .map_or(true, |owner| {
                owner.identity != task.identity || owner.context != task.context
            })
        {
            return Err(CatalogError::Busy.into());
        }
        let scan = self.terminal_scan(task);
        let page = self
            .repository
            .store
            .scan_gc(scan.clone())
            .await
            .map_err(CatalogError::from)?;
        scan.validate_page(&page).map_err(CatalogError::from)?;
        for item in &page.items {
            let key = IcebergKey::decode(&item.key)?;
            verify_terminal_record(&key, &item.value)?;
            if key != task.key()
                && !matches!(
                    key,
                    IcebergKey::Catalog {
                        scope: CatalogScope::Authority | CatalogScope::GcRetirement,
                        ..
                    }
                )
            {
                self.delete_exact(&item.key, &item.value).await?;
            }
        }
        let mut next = task.progress()?;
        if let Some(last) = page.items.last() {
            next.scan_after.clone_from(&last.key);
        } else {
            next.scan_after.clear();
            next.phase = GcPhase::Complete;
        }
        self.repository.update(task, &next).await?;
        Ok(next)
    }

    fn terminal_scan(&self, task: &GcTask) -> GcScan {
        GcScan {
            catalog: task.context.catalog,
            scope: None,
            prefix: Vec::new(),
            after: task.scan_after.clone(),
            items: usize::from(self.limits.page_items),
            bytes: self.limits.step_bytes.min(16 * 1024 * 1024) as usize,
        }
    }
}

fn verify_terminal_record(key: &IcebergKey, bytes: &[u8]) -> Result<(), GcWorkError> {
    let IcebergKey::Catalog { scope, .. } = key else {
        return Err(ValidationError::Key.into());
    };
    let record = StorageRecord::decode(key, bytes)?;
    match record {
        StorageRecord::GcTask(task) if task.paused || task.phase == GcPhase::Quarantined => {
            Err(CatalogError::Busy.into())
        }
        StorageRecord::GcCandidate(candidate)
            if *scope == CatalogScope::GcCandidate && candidate.phase != CandidatePhase::Complete =>
        {
            Err(CatalogError::Busy.into())
        }
        StorageRecord::Authority(_)
        | StorageRecord::GcTask(_)
        | StorageRecord::GcCandidate(_)
        | StorageRecord::GcPage(_)
        | StorageRecord::GcNode(_)
        | StorageRecord::PayloadPage(_) => Ok(()),
        StorageRecord::FileWriteIntent(_) if *scope == CatalogScope::FileWriteFence => Ok(()),
        _ => Err(CatalogError::Busy.into()),
    }
}
