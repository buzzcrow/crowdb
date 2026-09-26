use crate::{
    catalog::check_context,
    file::{AvroDatumLimits, AvroLimits},
    table::{head_key, TableMetadataLimits},
};

use super::{
    CatalogError, GcPhase, GcScan, GcStalledReason, GcTask, GcWorkError, GcWorker, IcebergKey, StorageRecord,
    ValidationError,
};

impl GcWorker {
    pub(super) async fn abandon_changed_live(&self, task: &GcTask) -> Result<Option<GcTask>, CatalogError> {
        if task.kind != super::GcTaskKind::LiveTable || task.fenced {
            return Ok(None);
        }
        let changed_context = match check_context(self.repository.store.as_ref(), task.context).await {
            Ok(()) => false,
            Err(CatalogError::Conflict) => true,
            Err(error) => return Err(error),
        };
        if !changed_context {
            let head = task.head.as_ref().ok_or(ValidationError::Record)?;
            let key = head_key(head.catalog, head.table);
            if let Some(value) = self.repository.store.get(&key.encode()?).await? {
                let StorageRecord::TableHead(current) = StorageRecord::decode(&key, &value.bytes)? else {
                    return Err(ValidationError::Record.into());
                };
                if current.as_ref() == head
                    || (current.lifecycle == crate::table::TableLifecycle::Reclaiming
                        && current.pending_operation == Some(task.identity))
                {
                    return Ok(None);
                }
            }
        }
        let mut next = task.progress()?;
        next.phase = GcPhase::Complete;
        next.stalled = GcStalledReason::ChangedAuthority;
        next.discovery_scope = 0;
        next.scan_after.clear();
        next.retry_at_ms = 0;
        self.repository.update(task, &next).await?;
        Ok(Some(next))
    }

    pub(super) async fn live_fence(&self, task: &GcTask, now_ms: u64) -> Result<GcTask, GcWorkError> {
        check_context(self.repository.store.as_ref(), task.context).await?;
        if !task.proof.complete || now_ms < task.not_before_ms {
            return Err(CatalogError::Busy.into());
        }
        let mut next = task.progress()?;
        next.scan_after.clear();
        if task.fenced {
            self.repository.verify_table_fence(task).await?;
            next.phase = GcPhase::Rescan;
        } else {
            self.repository.fence_table(task).await?;
            next.fenced = true;
            next.phase = GcPhase::Roots;
        }
        self.repository.update(task, &next).await?;
        Ok(next)
    }

    pub(super) async fn live_roots(&self, task: &GcTask, now_ms: u64) -> Result<GcTask, GcWorkError> {
        if task.fenced && self.repository.table_fence_released(task).await? {
            let mut next = task.progress()?;
            next.fenced = false;
            next.phase = GcPhase::Complete;
            next.stalled = GcStalledReason::Protected;
            self.repository.update(task, &next).await?;
            return Ok(next);
        }
        self.verify_live_head(task).await?;
        if task.queue_write == 0 {
            return Ok(self.repository.start_proof(task).await?);
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
        let grace_ms = self.protection_grace(task).await?;
        let mut next = task.progress()?;
        for item in &page.items {
            let key = IcebergKey::decode(&item.key)?;
            if matches!(
                key,
                IcebergKey::Catalog {
                    scope: crate::key::CatalogScope::MetadataProjection,
                    ..
                }
            ) {
                continue;
            }
            let record = StorageRecord::decode(&key, &item.value)?;
            if let StorageRecord::GcPin(pin) = &record {
                if pin.protects(now_ms)
                    && task
                        .head
                        .as_ref()
                        .is_some_and(|head| head.table == pin.head.table)
                    && !pin.protects_uploads
                {
                    self.repository.push_proof_root(&mut next, &pin.head).await?;
                    continue;
                }
            }
            if super::inactive::protects(task, &record, now_ms, grace_ms) {
                next.stalled = GcStalledReason::Protected;
                next.scan_after.clear();
                if task.fenced {
                    self.repository.release_table_fence(task).await?;
                    next.fenced = false;
                    next.phase = GcPhase::Complete;
                } else {
                    next.phase = GcPhase::Waiting;
                    next.retry_at_ms = now_ms
                        .checked_add(u64::from(self.limits.retry_base_ms))
                        .ok_or(ValidationError::Deadline)?;
                }
                self.repository.update(task, &next).await?;
                return Ok(next);
            }
        }
        if let Some(last) = page.items.last() {
            next.scan_after.clone_from(&last.key);
        } else {
            next.scan_after.clear();
            next.proof.complete = false;
            next.phase = GcPhase::Mark;
        }
        self.repository.update(task, &next).await?;
        Ok(next)
    }

    pub(super) async fn live_mark(&self, task: &GcTask) -> Result<GcTask, GcWorkError> {
        self.verify_live_head(task).await?;
        Ok(self
            .repository
            .advance_proof(
                task,
                self.blocks.clone(),
                TableMetadataLimits {
                    bytes: (self.limits.step_bytes as usize).min(2 * 1024 * 1024),
                    values: 200_000,
                    depth: 64,
                    string_bytes: (self.limits.step_bytes as usize).min(1024 * 1024),
                    collection_entries: 10_000,
                },
                AvroLimits {
                    header_bytes: 1024 * 1024,
                    metadata_entries: 128,
                    block_bytes: self.limits.step_bytes as usize,
                    records_per_block: 1_000_000,
                },
                AvroDatumLimits {
                    depth: 64,
                    values: 1_000_000,
                    value_bytes: self.limits.step_bytes as usize,
                },
            )
            .await?)
    }

    pub(super) async fn verify_live_head(&self, task: &GcTask) -> Result<(), GcWorkError> {
        check_context(self.repository.store.as_ref(), task.context).await?;
        if task.fenced {
            return Ok(self.repository.verify_table_fence(task).await?);
        }
        let head = task.head.as_ref().ok_or(ValidationError::Record)?;
        let key = head_key(head.catalog, head.table);
        let value = self
            .repository
            .store
            .get(&key.encode()?)
            .await
            .map_err(CatalogError::from)?
            .ok_or(ValidationError::Record)?;
        if StorageRecord::decode(&key, &value.bytes)? != StorageRecord::TableHead(Box::new(head.clone())) {
            return Err(CatalogError::Conflict.into());
        }
        Ok(())
    }
}
