use crate::{
    catalog::{CasOutcome, RootState},
    file::MultipartPhase,
    key::{CatalogScope, SystemScope},
    operation::mutation_identity,
};

use super::{
    CatalogError, GcPhase, GcScan, GcTask, GcTaskKind, GcWorkError, GcWorker, IcebergKey, StorageRecord,
    ValidationError,
};

impl GcWorker {
    pub(super) async fn cleanup_system(&self, task: &GcTask, now_ms: u64) -> Result<GcTask, GcWorkError> {
        self.verify_inactive(task).await?;
        if task.kind != GcTaskKind::RetiredCatalog || now_ms < task.not_before_ms {
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
            let remove = match StorageRecord::decode(&key, &item.value)? {
                StorageRecord::Management(operation) => {
                    let related = operation.candidate == task.context.catalog
                        || operation.request.confirmation == Some(task.context.catalog);
                    if related
                        && operation.id() != root.operation
                        && (!operation.terminal() || now_ms < operation.retained_until_ms)
                    {
                        return Err(CatalogError::Busy.into());
                    }
                    related && operation.id() != root.operation
                }
                StorageRecord::Retry(binding) => {
                    if binding.context == task.context
                        && (binding.status == 0 || now_ms < binding.retained_until_ms)
                    {
                        return Err(CatalogError::Busy.into());
                    }
                    binding.context == task.context
                }
                _ => return Err(ValidationError::Record.into()),
            };
            if remove {
                self.delete_exact(&item.key, &item.value).await?;
            }
        }
        let mut next = task.progress()?;
        if let Some(last) = page.items.last() {
            next.scan_after.clone_from(&last.key);
        } else {
            next.scan_after.clear();
            next.phase = GcPhase::CleanupCatalog;
        }
        self.repository.update(task, &next).await?;
        Ok(next)
    }

    pub(super) async fn cleanup_catalog(&self, task: &GcTask, now_ms: u64) -> Result<GcTask, GcWorkError> {
        self.verify_inactive(task).await?;
        if task.kind != GcTaskKind::RetiredCatalog || now_ms < task.not_before_ms {
            return Err(CatalogError::Busy.into());
        }
        let scan = GcScan {
            catalog: task.context.catalog,
            scope: None,
            prefix: Vec::new(),
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
        let grace = self.protection_grace(task).await?;
        for item in &page.items {
            let key = IcebergKey::decode(&item.key)?;
            if self
                .cleanup_record(task, now_ms, grace, &key, &item.value)
                .await?
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

    async fn cleanup_record(
        &self,
        task: &GcTask,
        now_ms: u64,
        grace_ms: u64,
        key: &IcebergKey,
        bytes: &[u8],
    ) -> Result<bool, GcWorkError> {
        let IcebergKey::Catalog { scope, suffix, .. } = key else {
            return Err(ValidationError::Key.into());
        };
        if *scope == CatalogScope::MetadataProjection {
            return Ok(true);
        }
        if matches!(
            scope,
            CatalogScope::Authority
                | CatalogScope::GcTask
                | CatalogScope::GcCandidate
                | CatalogScope::GcClaim
                | CatalogScope::GcPage
                | CatalogScope::GcNode
                | CatalogScope::GcPending
        ) {
            return Ok(false);
        }
        let record = StorageRecord::decode(key, bytes)?;
        if super::inactive::protects(task, &record, now_ms, grace_ms) {
            return Err(CatalogError::Busy.into());
        }
        match &record {
            StorageRecord::File(_) | StorageRecord::MultipartPart(_) => Err(ValidationError::Record.into()),
            StorageRecord::MultipartSession(session) => {
                if !matches!(
                    session.phase,
                    MultipartPhase::Published | MultipartPhase::Aborted | MultipartPhase::Conflicted
                ) || now_ms < session.expires_ms.saturating_add(grace_ms)
                    || session.pending.is_some()
                    || session.completion.as_ref().is_some_and(|completion| {
                        completion.progress.writer.is_some()
                            || (completion.candidate.is_some() && session.phase != MultipartPhase::Published)
                    })
                {
                    return Err(CatalogError::Busy.into());
                }
                let parts = GcScan {
                    catalog: task.context.catalog,
                    scope: Some(CatalogScope::MultipartPart),
                    prefix: session.upload.as_bytes().to_vec(),
                    after: Vec::new(),
                    items: 1,
                    bytes: self.limits.step_bytes.min(16 * 1024 * 1024) as usize,
                };
                let page = self
                    .repository
                    .store
                    .scan_gc(parts.clone())
                    .await
                    .map_err(CatalogError::from)?;
                parts.validate_page(&page).map_err(CatalogError::from)?;
                if page.items.is_empty() {
                    Ok(true)
                } else {
                    Err(CatalogError::Busy.into())
                }
            }
            StorageRecord::PayloadPage(_) => {
                let operation = crate::key::OperationId::from_bytes(&suffix[..16])?;
                Ok(self
                    .repository
                    .task(task.context.catalog, operation)
                    .await?
                    .is_none())
            }
            StorageRecord::GcPin(pin) => Ok(!pin.protects(now_ms)),
            _ => Ok(true),
        }
    }

    async fn delete_exact(&self, key: &[u8], bytes: &[u8]) -> Result<(), GcWorkError> {
        match self
            .repository
            .store
            .delete_gc_record(key, bytes, mutation_identity(key, Some(bytes), &[]))
            .await
            .map_err(CatalogError::from)?
        {
            CasOutcome::Applied(_) | CasOutcome::Conflict(None) => Ok(()),
            CasOutcome::Conflict(_) => Err(CatalogError::Conflict.into()),
        }
    }
}
