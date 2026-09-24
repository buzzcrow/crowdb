use super::{advance, candidate, CommitPublicationError as Error, Phase, Publisher};
use crate::{
    catalog::{check_context, CasOutcome, CatalogError},
    commit::{TableCommitOperation, TableCommitOutcome},
    error::ValidationError,
    file::FileRepository,
    operation::{mutation_identity, PayloadStore, MAX_PAYLOAD_BYTES},
    record::StorageRecord,
    table::{head_key, read_table_metadata_document, SelectedTable, TableMetadataLimits},
};

impl Publisher {
    pub(super) async fn reject_superseded(
        &self,
        operation: &TableCommitOperation,
    ) -> Result<Option<TableCommitOperation>, Error> {
        let key = head_key(operation.before.catalog, operation.before.table);
        let value = self
            .store
            .get(&key.encode()?)
            .await
            .map_err(CatalogError::from)?
            .ok_or(CatalogError::Busy)?;
        let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if head.as_ref() == &operation.before {
            return Ok(None);
        }
        if head.generation < operation.before.generation
            || head.operation_fence < operation.before.operation_fence
            || (head.generation == operation.before.generation
                && head.operation_fence == operation.before.operation_fence)
            || head.pending_operation == Some(operation.identity.operation)
        {
            return Err(CatalogError::Busy.into());
        }
        let mut next = advance(operation, Phase::Rejected)?;
        next.outcome = Some(self.conflict_outcome(operation).await?);
        self.change(operation, &next).await?;
        Ok(Some(next))
    }

    async fn conflict_outcome(&self, operation: &TableCommitOperation) -> Result<TableCommitOutcome, Error> {
        let body = PayloadStore::new(self.store.clone()).put(operation.context.catalog,
            operation.identity.operation, br#"{"error":{"message":"Table generation changed","type":"CommitFailedException","code":409}}"#).await?;
        Ok(TableCommitOutcome { status: 409, body })
    }

    pub(super) async fn finish(
        &self,
        mut operation: TableCommitOperation,
    ) -> Result<TableCommitOutcome, Error> {
        self.current(&operation).await?;
        if operation.phase == Phase::Publishing {
            operation = self.select(&operation).await?;
        }
        if operation.phase == Phase::Published {
            let body = self.success_body(&operation).await?;
            let mut next = advance(&operation, Phase::Complete)?;
            next.outcome = Some(TableCommitOutcome { status: 200, body });
            self.change(&operation, &next).await?;
            operation = next;
        }
        if !operation.phase.terminal() {
            return Err(CatalogError::Busy.into());
        }
        let outcome = operation.outcome.clone().ok_or(ValidationError::Record)?;
        PayloadStore::new(self.store.clone()).get(&outcome.body).await?;
        if operation.phase == Phase::Complete {
            self.settle(&operation).await?;
        }
        check_context(self.store.as_ref(), operation.context).await?;
        Ok(outcome)
    }

    async fn select(&self, operation: &TableCommitOperation) -> Result<TableCommitOperation, Error> {
        let candidate = operation.candidate.as_ref().ok_or(ValidationError::Record)?;
        self.success_body(operation).await?;
        self.current(operation).await?;
        let key = head_key(candidate.catalog, candidate.table).encode()?;
        let before = StorageRecord::TableHead(Box::new(operation.before.clone())).encode()?;
        let after = StorageRecord::TableHead(Box::new(candidate.clone())).encode()?;
        let result = self
            .store
            .compare_exchange(
                &key,
                Some(&before),
                &after,
                mutation_identity(&key, Some(&before), &after),
            )
            .await
            .map_err(CatalogError::from)?;
        check_context(self.store.as_ref(), operation.context).await?;
        let published = match result {
            CasOutcome::Applied(_) => true,
            CasOutcome::Conflict(Some(value)) => value.bytes == after,
            CasOutcome::Conflict(None) => return Err(CatalogError::Busy.into()),
        };
        let mut next = advance(
            operation,
            if published {
                Phase::Published
            } else {
                Phase::Rejected
            },
        )?;
        if !published {
            let body = PayloadStore::new(self.store.clone()).put(operation.context.catalog,
                operation.identity.operation, br#"{"error":{"message":"Table generation changed","type":"CommitFailedException","code":409}}"#).await?;
            next.outcome = Some(TableCommitOutcome { status: 409, body });
        }
        self.change(operation, &next).await?;
        Ok(next)
    }

    async fn success_body(
        &self,
        operation: &TableCommitOperation,
    ) -> Result<crate::operation::PayloadReference, Error> {
        let head = operation.candidate.as_ref().ok_or(ValidationError::Record)?;
        let metadata = FileRepository::new(self.store.clone())
            .load(operation.context, &head.metadata_location)
            .await?
            .ok_or(ValidationError::Record)?;
        let document = read_table_metadata_document(
            self.blocks.clone(),
            &SelectedTable {
                head: head.clone(),
                metadata,
            },
            TableMetadataLimits {
                bytes: MAX_PAYLOAD_BYTES,
                values: 1_000_000,
                depth: 64,
                string_bytes: MAX_PAYLOAD_BYTES,
                collection_entries: 100_000,
            },
        )
        .await?;
        let bytes = candidate::response(head, document.canonical())?;
        Ok(PayloadStore::new(self.store.clone())
            .put(operation.context.catalog, operation.identity.operation, &bytes)
            .await?)
    }

    async fn settle(&self, operation: &TableCommitOperation) -> Result<(), Error> {
        let candidate = operation.candidate.as_ref().ok_or(ValidationError::Record)?;
        let mut settled = candidate.clone();
        settled.pending_operation = None;
        let key = head_key(candidate.catalog, candidate.table).encode()?;
        let before = StorageRecord::TableHead(Box::new(candidate.clone())).encode()?;
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
        Ok(())
    }
}
