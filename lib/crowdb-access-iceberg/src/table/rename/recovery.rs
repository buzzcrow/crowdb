use super::{
    check_context, mutation_identity, storage_head_key, CasOutcome, CatalogContext, CatalogError,
    NamespaceRename, StorageRecord, TableHead, TableMappingState, TableRenames, ValidationError,
};

impl TableRenames {
    pub(crate) async fn recover(
        &self,
        context: CatalogContext,
        rename: &NamespaceRename,
    ) -> Result<(), CatalogError> {
        rename.validate()?;
        if rename.source.catalog != context.catalog {
            return Err(ValidationError::IdentityMismatch.into());
        }
        check_context(self.store.as_ref(), context).await?;
        let key = storage_head_key(rename.source.storage_location());
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        self.settle(context, rename, *head).await
    }

    pub(super) async fn settle(
        &self,
        context: CatalogContext,
        rename: &NamespaceRename,
        mut head: TableHead,
    ) -> Result<(), CatalogError> {
        let key = storage_head_key(rename.source.storage_location());
        let encoded = key.encode()?;
        for _ in 0..16 {
            if head.operation_fence > rename.head_fence {
                let destination = rename.destination(TableMappingState::Published);
                if destination.resolves(&head) {
                    self.publish_name(&rename.destination(TableMappingState::Reserved))
                        .await?;
                } else {
                    self.remove(&rename.destination(TableMappingState::Reserved))
                        .await?;
                }
                if !rename.source.resolves(&head) {
                    self.remove(&rename.source).await?;
                }
                self.release(rename).await?;
                return check_context(self.store.as_ref(), context).await;
            }
            if head.operation_fence != rename.head_fence
                || head.generation != rename.head_generation
                || !rename.source.resolves(&head)
                || head.pending_operation.is_some()
            {
                return Err(ValidationError::Record.into());
            }
            // Win against any delayed name-switch CAS before removing occupancy.
            let before = StorageRecord::TableHead(Box::new(head.clone())).encode()?;
            let mut fenced = head.clone();
            fenced.operation_fence += 1;
            let bytes = StorageRecord::TableHead(Box::new(fenced.clone())).encode()?;
            match self
                .store
                .compare_exchange(
                    &encoded,
                    Some(&before),
                    &bytes,
                    mutation_identity(&encoded, Some(&before), &bytes),
                )
                .await?
            {
                CasOutcome::Applied(_) => head = fenced,
                CasOutcome::Conflict(Some(observed)) => {
                    let StorageRecord::TableHead(observed) = StorageRecord::decode(&key, &observed.bytes)?
                    else {
                        return Err(ValidationError::Record.into());
                    };
                    head = *observed;
                }
                CasOutcome::Conflict(None) => return Err(CatalogError::Busy),
            }
        }
        Err(CatalogError::Busy)
    }
}
