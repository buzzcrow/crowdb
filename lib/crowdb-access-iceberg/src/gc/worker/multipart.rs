use super::{
    CatalogError, CatalogScope, GcScan, GcTask, GcWorkError, GcWorker, IcebergKey, StorageRecord,
    ValidationError,
};

impl GcWorker {
    pub(super) async fn sweep_multipart_sessions(
        &self,
        task: &GcTask,
        now_ms: u64,
    ) -> Result<GcTask, GcWorkError> {
        self.verify_inactive(task).await?;
        let scan = GcScan {
            catalog: task.context.catalog,
            scope: Some(CatalogScope::MultipartSession),
            prefix: Vec::new(),
            after: task.scan_after.clone(),
            items: 1,
            bytes: (crate::record::MAX_RECORD_BYTES + crate::key::MAX_KEY_BYTES)
                .min(self.limits.step_bytes as usize),
        };
        let page = self
            .repository
            .store
            .scan_gc(scan.clone())
            .await
            .map_err(CatalogError::from)?;
        scan.validate_page(&page).map_err(CatalogError::from)?;
        let mut next = task.progress()?;
        let Some(item) = page.items.first() else {
            return self.finish_sweep(task, next, now_ms).await;
        };
        let key = IcebergKey::decode(&item.key)?;
        let StorageRecord::MultipartSession(session) = StorageRecord::decode(&key, &item.value)? else {
            return Err(ValidationError::Record.into());
        };
        if self
            .repository
            .session_is_abandoned(task, &session, now_ms)
            .await?
        {
            let parts = GcScan {
                catalog: task.context.catalog,
                scope: Some(CatalogScope::MultipartPart),
                prefix: session.upload.as_bytes().to_vec(),
                after: Vec::new(),
                items: 1,
                bytes: (crate::record::MAX_RECORD_BYTES + crate::key::MAX_KEY_BYTES)
                    .min(self.limits.step_bytes as usize),
            };
            let page = self
                .repository
                .store
                .scan_gc(parts.clone())
                .await
                .map_err(CatalogError::from)?;
            parts.validate_page(&page).map_err(CatalogError::from)?;
            if page.items.is_empty()
                && (session
                    .completion
                    .as_ref()
                    .and_then(|completion| completion.progress.writer.as_ref())
                    .is_none()
                    || self.assembly_reclaimed(&session).await?)
            {
                if let Some(completion) = &session.completion {
                    for reference in [Some(&completion.selection), completion.publication.as_ref()]
                        .into_iter()
                        .flatten()
                    {
                        for index in 0..reference.page_count() {
                            let key =
                                reference.page_key(index.try_into().map_err(|_| ValidationError::Key)?)?;
                            let encoded = key.encode()?;
                            if let Some(value) = self
                                .repository
                                .store
                                .get(&encoded)
                                .await
                                .map_err(CatalogError::from)?
                            {
                                let StorageRecord::PayloadPage(page) =
                                    StorageRecord::decode(&key, &value.bytes)?
                                else {
                                    return Err(ValidationError::Record.into());
                                };
                                if page.reference != *reference {
                                    return Err(ValidationError::IdentityMismatch.into());
                                }
                                self.delete_exact(&encoded, &value.bytes).await?;
                                self.repository.update(task, &next).await?;
                                return Ok(next);
                            }
                        }
                    }
                }
                self.delete_exact(&item.key, &item.value).await?;
            }
        }
        next.scan_after.clone_from(&item.key);
        self.repository.update(task, &next).await?;
        Ok(next)
    }
}
