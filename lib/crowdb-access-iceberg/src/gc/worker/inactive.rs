use super::{
    CatalogError, CatalogScope, GcPhase, GcScan, GcStalledReason, GcTask, GcTaskKind, GcWorkError, GcWorker,
    IcebergKey, StorageRecord, ValidationError,
};

impl GcWorker {
    pub(super) async fn protection_grace(&self, task: &GcTask) -> Result<u64, GcWorkError> {
        let key = IcebergKey::Catalog {
            catalog: task.context.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        };
        let value = self
            .repository
            .store
            .get(&key.encode()?)
            .await
            .map_err(CatalogError::from)?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::Authority(authority) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        authority
            .admission_bounds
            .request_ms
            .checked_add(authority.admission_bounds.clock_skew_ms)
            .ok_or_else(|| ValidationError::Deadline.into())
    }

    pub(super) async fn inactive_roots(&self, task: &GcTask, now_ms: u64) -> Result<GcTask, GcWorkError> {
        self.verify_inactive(task).await?;
        if now_ms < task.not_before_ms {
            let mut next = task.advance()?;
            next.retry_at_ms = task.not_before_ms;
            next.stalled = GcStalledReason::Retention;
            self.repository.update(task, &next).await?;
            return Ok(next);
        }
        if task.kind == GcTaskKind::PurgeTable && !task.fenced {
            let mut next = task.advance()?;
            next.phase = GcPhase::Fence;
            next.scan_after.clear();
            self.repository.update(task, &next).await?;
            return Ok(next);
        }
        if task.fenced {
            self.repository.verify_table_fence(task).await?;
        }
        let scan = GcScan {
            native: false,
            catalog: task.context.catalog,
            scope: None,
            prefix: Vec::new(),
            after: task.scan_after.clone(),
            items: usize::from(self.limits.page_items),
            bytes: usize::try_from(self.limits.step_bytes.min(16 * 1024 * 1024))
                .map_err(|_| ValidationError::Record)?,
        };
        let page = self
            .repository
            .store
            .scan_gc(scan.clone())
            .await
            .map_err(CatalogError::from)?;
        scan.validate_page(&page).map_err(CatalogError::from)?;
        let grace_ms = self.protection_grace(task).await?;
        for item in &page.items {
            let key = IcebergKey::decode(&item.key)?;
            let record = match &key {
                IcebergKey::Catalog {
                    scope:
                        CatalogScope::GcTask
                        | CatalogScope::GcClaim
                        | CatalogScope::GcAssemblyClaim
                        | CatalogScope::GcCandidate
                        | CatalogScope::GcPage
                        | CatalogScope::GcNode
                        | CatalogScope::GcPending
                        | CatalogScope::MetadataProjection,
                    ..
                } => continue,
                _ => StorageRecord::decode(&key, &item.value)?,
            };
            if self.record_protected(task, &record, now_ms, grace_ms).await? {
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
            next.phase = GcPhase::RootsNative;
        }
        self.repository.update(task, &next).await?;
        Ok(next)
    }

    pub(super) async fn fence(&self, task: &GcTask, now_ms: u64) -> Result<GcTask, GcWorkError> {
        self.verify_inactive(task).await?;
        if now_ms < task.not_before_ms {
            return Err(CatalogError::Busy.into());
        }
        if task.kind == GcTaskKind::PurgeTable {
            self.repository.fence_table(task).await?;
        }
        let mut next = task.progress()?;
        next.phase = GcPhase::Roots;
        next.fenced = task.kind == GcTaskKind::PurgeTable;
        next.scan_after.clear();
        self.repository.update(task, &next).await?;
        Ok(next)
    }

    pub(super) async fn record_protected(
        &self,
        task: &GcTask,
        record: &StorageRecord,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<bool, GcWorkError> {
        let StorageRecord::TableCreateOperation(operation) = record else {
            return Ok(protects(task, record, now_ms, grace_ms));
        };
        let table = operation.candidate.storage_location();
        if task
            .head
            .as_ref()
            .is_some_and(|head| head.storage_location() != table)
        {
            return Ok(false);
        }
        if task.kind == GcTaskKind::RetiredCatalog
            && now_ms
                < operation
                    .identity
                    .issued_ms
                    .saturating_add(crate::operation::RETRY_WINDOW_MS)
                    .saturating_add(grace_ms)
        {
            return Ok(true);
        }
        let key = crate::table::storage_head_key(table);
        let current = self
            .repository
            .store
            .get(&key.encode()?)
            .await
            .map_err(CatalogError::from)?;
        if task.kind == GcTaskKind::RetiredCatalog {
            self.retire_table_slot(table, current.as_ref().map(|value| value.bytes.as_slice()))
                .await?;
            return Ok(false);
        }
        let slot = if let Some(value) = current {
            match StorageRecord::decode(&key, &value.bytes)? {
                StorageRecord::RetiredTable(_) => crate::table::TableSlot::Retired,
                StorageRecord::TableHead(head) => crate::table::TableSlot::Existing(head),
                _ => return Err(ValidationError::Record.into()),
            }
        } else {
            return Ok(true);
        };
        // A final occupied head slot permanently excludes the old None-to-head write.
        Ok(matches!(slot, crate::table::TableSlot::Existing(head)
            if task.kind != GcTaskKind::RetiredCatalog && *head == operation.candidate))
    }
}

pub(super) fn protects(task: &GcTask, record: &StorageRecord, now_ms: u64, grace_ms: u64) -> bool {
    let owns = |table| {
        task.head
            .as_ref()
            .map_or(true, |head| head.storage_location() == table)
    };
    let retained = |issued: u64| {
        now_ms
            < issued
                .saturating_add(crate::operation::RETRY_WINDOW_MS)
                .saturating_add(grace_ms)
    };
    match record {
        StorageRecord::TableCommitOperation(operation) => {
            owns(operation.before.storage_location())
                && ((task.kind == GcTaskKind::RetiredCatalog && retained(operation.identity.issued_ms))
                    || !matches!(
                        operation.phase,
                        crate::commit::TableCommitPhase::Complete | crate::commit::TableCommitPhase::Rejected
                    ))
        }
        StorageRecord::TableLifecycleOperation(operation) => {
            owns(operation.before.storage_location())
                && ((task.kind == GcTaskKind::RetiredCatalog && retained(operation.identity.issued_ms))
                    || !matches!(
                        operation.phase,
                        crate::table::TableLifecyclePhase::Complete
                            | crate::table::TableLifecyclePhase::Aborted
                    ))
        }
        StorageRecord::MultipartSession(session) => {
            let terminal = matches!(
                session.phase,
                crate::file::MultipartPhase::Published
                    | crate::file::MultipartPhase::Aborted
                    | crate::file::MultipartPhase::Conflicted
            );
            owns(session.owner.table)
                && (!terminal
                    || (task.kind == GcTaskKind::RetiredCatalog
                        && now_ms < session.expires_ms.saturating_add(grace_ms)))
        }
        StorageRecord::RetryResult(result) => {
            task.kind == GcTaskKind::RetiredCatalog
                && (now_ms < result.binding.retained_until_ms || result.binding.status == 0)
        }
        StorageRecord::Retry(result) => {
            task.kind == GcTaskKind::RetiredCatalog
                && (now_ms < result.retained_until_ms || result.status == 0)
        }
        _ => false,
    }
}
