use super::{Error, Phase, TableCreateOperation, TableCreator};
use crate::{
    catalog::{CasOutcome, CatalogError},
    commit::TableCommitOutcome,
    error::ValidationError,
    file::{ContentFormat, FileKind, FileRepository},
    namespace::authority_key,
    operation::mutation_identity,
    record::StorageRecord,
    table::{head_key, name_key, TableMappingState},
};

impl TableCreator {
    pub(super) async fn publish_head(&self, operation: &TableCreateOperation) -> Result<(), Error> {
        self.current(operation).await?;
        self.check_admission(operation).await?;
        let head = &operation.candidate;
        let file = FileRepository::new(self.store.clone())
            .load(operation.context, &head.metadata_location)
            .await?
            .ok_or(ValidationError::Record)?;
        if file.file != head.metadata_file
            || file.digest != head.metadata_digest
            || file.length != operation.document.length as u64
            || file.kind != FileKind::Metadata
            || file.format != ContentFormat::Json
        {
            return Err(ValidationError::IdentityMismatch.into());
        }
        let key = head_key(head.catalog, head.table).encode()?;
        let bytes = StorageRecord::TableHead(Box::new(head.clone())).encode()?;
        let result = self
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .map_err(CatalogError::from)?;
        if !matches!(result, CasOutcome::Applied(_))
            && !matches!(&result, CasOutcome::Conflict(Some(value)) if value.bytes == bytes)
        {
            return Err(CatalogError::Busy.into());
        }
        self.journal()
            .advance(operation, &operation.next(Phase::Published)?)
            .await?;
        Ok(())
    }

    pub(super) async fn publish_name(&self, operation: &TableCreateOperation) -> Result<(), Error> {
        self.current(operation).await?;
        let mapping = operation.mapping(TableMappingState::Reserved);
        let key = name_key(mapping.catalog, mapping.namespace, &mapping.name)?.encode()?;
        let before = StorageRecord::TableMapping(mapping).encode()?;
        let after = StorageRecord::TableMapping(operation.mapping(TableMappingState::Published)).encode()?;
        let result = self
            .names
            .compare_exchange(
                &key,
                Some(&before),
                &after,
                mutation_identity(&key, Some(&before), &after),
            )
            .await
            .map_err(CatalogError::from)?;
        if !matches!(result, CasOutcome::Applied(_))
            && !matches!(&result, CasOutcome::Conflict(Some(value)) if value.bytes == after)
        {
            return Err(CatalogError::Busy.into());
        }
        let mut next = operation.next(Phase::Complete)?;
        next.outcome = Some(TableCommitOutcome {
            status: 200,
            body: operation.response.clone(),
        });
        self.journal().advance(operation, &next).await?;
        Ok(())
    }

    pub(super) async fn cleanup(&self, operation: &TableCreateOperation) -> Result<(), Error> {
        self.current(operation).await?;
        if let Some(admission) = &operation.admission {
            let (_, before) = self.admission_bytes(operation).await?;
            let key = authority_key(operation.context.catalog, operation.candidate.namespace);
            let StorageRecord::NamespaceAuthority(mut parent) = StorageRecord::decode(&key, &before)? else {
                return Err(ValidationError::Record.into());
            };
            parent.pending_operation = None;
            parent.mutation_revision = parent
                .mutation_revision
                .checked_add(1)
                .ok_or(ValidationError::GenerationExhausted)?;
            let after = StorageRecord::NamespaceAuthority(parent).encode()?;
            self.store
                .compare_exchange(
                    &admission.key,
                    Some(&before),
                    &after,
                    mutation_identity(&admission.key, Some(&before), &after),
                )
                .await
                .map_err(CatalogError::from)?;
        }
        if matches!(operation.phase, Phase::Aborting | Phase::Aborted) {
            let mapping = operation.mapping(TableMappingState::Reserved);
            let key = name_key(mapping.catalog, mapping.namespace, &mapping.name)?.encode()?;
            let bytes = StorageRecord::TableMapping(mapping).encode()?;
            self.names
                .delete_mapping(&key, &bytes, mutation_identity(&key, Some(&bytes), &[]))
                .await
                .map_err(CatalogError::from)?;
        } else if operation.phase == Phase::Complete {
            let head = &operation.candidate;
            let key = head_key(head.catalog, head.table).encode()?;
            let before = StorageRecord::TableHead(Box::new(head.clone())).encode()?;
            let mut settled = head.clone();
            settled.pending_operation = None;
            let after = StorageRecord::TableHead(Box::new(settled)).encode()?;
            self.store
                .compare_exchange(
                    &key,
                    Some(&before),
                    &after,
                    mutation_identity(&key, Some(&before), &after),
                )
                .await
                .map_err(CatalogError::from)?;
        } else {
            return Err(ValidationError::Record.into());
        }
        Ok(())
    }
}
