use crate::catalog::{CatalogError, StoreError};
use crate::error::ValidationError;
use crate::key::{CatalogScope, IcebergKey};
use crate::operation::{mutation_identity, PayloadStore};
use crate::record::StorageRecord;
use crowdb_chunk_kv_client::MultiScanContinuation;
use crowdb_protocol::chunk_kv::ScanDirection;

use super::update_recovery::next_phase;
use super::{
    authority_key, child_range, ChildScan, NamespaceAction, NamespaceDropper, NamespaceJournal,
    NamespaceLifecycle, NamespaceMappingState, NamespaceOperation, NamespacePhase,
};

impl NamespaceDropper {
    pub(super) async fn probe(
        &self,
        operation: &NamespaceOperation,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        let scope = if operation.phase == NamespacePhase::ProbingNamespaces {
            CatalogScope::NamespaceName
        } else {
            CatalogScope::TableName
        };
        let range = child_range(operation.context.catalog, Some(operation.namespace), scope)?;
        let continuation = (!operation.scan_after.is_empty()).then(|| MultiScanContinuation {
            direction: ScanDirection::Forward,
            original_start: Some(range.start.clone()),
            original_end: Some(range.end.clone()),
            last_key: operation.scan_after.clone(),
            catalog_generation: operation.scan_generation,
        });
        let scan = ChildScan {
            catalog: operation.context.catalog,
            parent: Some(operation.namespace),
            scope,
            limit: 16,
            continuation,
        };
        let page = self.creator.names.scan_children(scan.clone()).await?;
        if let Some(failure) = page.terminal_failure {
            return Err(StoreError::Rejected(failure).into());
        }
        if page.items.len() > scan.limit {
            return Err(ValidationError::RecordTooLarge.into());
        }
        let mut last = operation.scan_after.clone();
        for item in &page.items {
            if !range.contains(&item.key) || item.key <= last {
                return Err(ValidationError::Key.into());
            }
            last = item.key.clone();
            let resolved = if scope == CatalogScope::TableName {
                self.inspect_table(operation, &item.key, &item.value, budget)
                    .await?
            } else {
                self.inspect_child(operation, &item.key, &item.value, budget)
                    .await?
            };
            if resolved {
                return Ok(());
            }
        }
        let journal = NamespaceJournal::new(self.creator.repository.store.clone());
        if let Some(continuation) = page.continuation {
            ChildScan {
                continuation: Some(continuation.clone()),
                ..scan
            }
            .request()?;
            if continuation.last_key < last || continuation.last_key <= operation.scan_after {
                return Err(ValidationError::Key.into());
            }
            let mut next = next_phase(operation, operation.phase)?;
            next.scan_after = continuation.last_key;
            next.scan_generation = continuation.catalog_generation;
            journal.advance(operation, &next).await?;
        } else if operation.phase == NamespacePhase::ProbingNamespaces {
            let mut next = next_phase(operation, NamespacePhase::ProbingTables)?;
            next.scan_after.clear();
            next.scan_generation = 0;
            journal.advance(operation, &next).await?;
        } else {
            self.prepare_finish(operation, NamespacePhase::Tombstoning)
                .await?;
        }
        Ok(())
    }

    async fn inspect_child(
        &self,
        operation: &NamespaceOperation,
        encoded: &[u8],
        bytes: &[u8],
        budget: &mut usize,
    ) -> Result<bool, CatalogError> {
        let key = IcebergKey::decode(encoded)?;
        let StorageRecord::NamespaceMapping(mapping) = StorageRecord::decode(&key, bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if mapping.parent != Some(operation.namespace) {
            return Err(ValidationError::IdentityMismatch.into());
        }
        if mapping.state == NamespaceMappingState::Reserved {
            let owner = NamespaceJournal::new(self.creator.repository.store.clone())
                .load(operation.context, mapping.operation)
                .await?
                .ok_or(ValidationError::Record)?;
            if owner.action != NamespaceAction::Create
                || owner.parent != mapping.parent
                || owner.namespace != mapping.namespace
                || owner.identifier.parent().as_ref() != Some(&operation.identifier)
                || owner.identifier.name() != mapping.name
            {
                return Err(ValidationError::Record.into());
            }
            self.creator
                .resume_with_budget(operation.context, mapping.operation, budget)
                .await?;
            return Ok(true);
        }
        let key = authority_key(operation.context.catalog, mapping.namespace);
        if let Some(value) = self.creator.names.get(&key.encode()?).await? {
            let StorageRecord::NamespaceAuthority(authority) = StorageRecord::decode(&key, &value.bytes)?
            else {
                return Err(ValidationError::Record.into());
            };
            if mapping.resolves(&authority)
                && authority.identifier.parent().as_ref() == Some(&operation.identifier)
            {
                self.prepare_finish(operation, NamespacePhase::Restoring).await?;
                return Ok(true);
            }
        }
        self.creator
            .names
            .delete_mapping(encoded, bytes, mutation_identity(encoded, Some(bytes), &[]))
            .await?;
        Ok(false)
    }

    async fn inspect_table(
        &self,
        operation: &NamespaceOperation,
        encoded: &[u8],
        bytes: &[u8],
        budget: &mut usize,
    ) -> Result<bool, CatalogError> {
        let key = IcebergKey::decode(encoded)?;
        let StorageRecord::TableMapping(mapping) = StorageRecord::decode(&key, bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if mapping.namespace != operation.namespace {
            return Err(ValidationError::IdentityMismatch.into());
        }
        if mapping.state == crate::table::TableMappingState::Reserved {
            Box::pin(crate::commit::TableCreator::help_reservation(
                self.creator.repository.store.clone(),
                self.creator.names.clone(),
                operation.context,
                &mapping,
                budget,
            ))
            .await?;
            return Ok(true);
        }
        let key = crate::table::head_key(mapping.catalog, mapping.table);
        let value = self
            .creator
            .names
            .get(&key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if mapping.resolves(&head) {
            self.prepare_finish(operation, NamespacePhase::Restoring).await?;
            return Ok(true);
        }
        self.creator
            .names
            .delete_mapping(encoded, bytes, mutation_identity(encoded, Some(bytes), &[]))
            .await?;
        Ok(false)
    }

    pub(super) async fn prepare_finish(
        &self,
        operation: &NamespaceOperation,
        phase: NamespacePhase,
    ) -> Result<(), CatalogError> {
        let mutation = operation.mutation.as_ref().ok_or(ValidationError::Record)?;
        let before = PayloadStore::new(self.creator.repository.store.clone())
            .get(&mutation.after)
            .await?;
        let key = authority_key(operation.context.catalog, operation.namespace);
        let StorageRecord::NamespaceAuthority(authority) = StorageRecord::decode(&key, &before)? else {
            return Err(ValidationError::Record.into());
        };
        if authority.lifecycle != NamespaceLifecycle::Dropping
            || authority.pending_operation != Some(operation.identity.operation)
        {
            return Err(ValidationError::Record.into());
        }
        let lifecycle = if phase == NamespacePhase::Restoring {
            NamespaceLifecycle::Ready
        } else {
            NamespaceLifecycle::Tombstone
        };
        let after = Self::transition(*authority, operation, lifecycle)?.encode()?;
        self.persist_mutation(operation, phase, &before, &after).await
    }
}
