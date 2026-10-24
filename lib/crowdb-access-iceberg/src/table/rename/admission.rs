use super::{
    mutation_identity, CasOutcome, CatalogError, NamespaceAuthority, NamespaceLifecycle, NamespaceRename,
    StorageRecord, TableRenames, ValidationError,
};
use crate::namespace::authority_key;

impl TableRenames {
    pub(super) async fn admit(
        &self,
        rename: &NamespaceRename,
        parents: Vec<NamespaceAuthority>,
    ) -> Result<(), CatalogError> {
        for parent in parents {
            self.occupy(rename, parent).await?;
        }
        Ok(())
    }

    async fn occupy(
        &self,
        rename: &NamespaceRename,
        mut parent: NamespaceAuthority,
    ) -> Result<(), CatalogError> {
        let key = authority_key(parent.catalog, parent.namespace);
        let encoded = key.encode()?;
        let identifier = parent.identifier.clone();
        let name_epoch = parent.name_epoch;
        for _ in 0..16 {
            if parent.lifecycle != NamespaceLifecycle::Ready
                || parent.pending_operation.is_some()
                || parent.identifier != identifier
                || parent.name_epoch != name_epoch
            {
                return Err(CatalogError::Conflict);
            }
            if let Some(existing) = &parent.rename {
                return if existing.as_ref() == rename {
                    Ok(())
                } else {
                    Err(CatalogError::Busy)
                };
            }
            let expected = StorageRecord::NamespaceAuthority(Box::new(parent.clone())).encode()?;
            let mut next = parent;
            next.mutation_revision = next
                .mutation_revision
                .checked_add(1)
                .ok_or(ValidationError::GenerationExhausted)?;
            next.rename = Some(Box::new(rename.clone()));
            let bytes = StorageRecord::NamespaceAuthority(Box::new(next)).encode()?;
            match self
                .store
                .compare_exchange(
                    &encoded,
                    Some(&expected),
                    &bytes,
                    mutation_identity(&encoded, Some(&expected), &bytes),
                )
                .await?
            {
                CasOutcome::Applied(_) => return Ok(()),
                CasOutcome::Conflict(Some(value)) => {
                    let StorageRecord::NamespaceAuthority(observed) =
                        StorageRecord::decode(&key, &value.bytes)?
                    else {
                        return Err(ValidationError::Record.into());
                    };
                    parent = *observed;
                }
                CasOutcome::Conflict(None) => return Err(CatalogError::Conflict),
            }
        }
        Err(CatalogError::Busy)
    }

    pub(super) async fn release(&self, rename: &NamespaceRename) -> Result<(), CatalogError> {
        for parent in rename.parents() {
            let key = authority_key(rename.source.catalog, parent);
            let encoded = key.encode()?;
            let mut value = self.store.get(&encoded).await?;
            let mut finished = false;
            for _ in 0..16 {
                let Some(current) = value else {
                    finished = true;
                    break;
                };
                let StorageRecord::NamespaceAuthority(mut authority) =
                    StorageRecord::decode(&key, &current.bytes)?
                else {
                    return Err(ValidationError::Record.into());
                };
                if authority.rename.as_deref() != Some(rename) {
                    finished = true;
                    break;
                }
                authority.rename = None;
                authority.mutation_revision = authority
                    .mutation_revision
                    .checked_add(1)
                    .ok_or(ValidationError::GenerationExhausted)?;
                let bytes = StorageRecord::NamespaceAuthority(authority).encode()?;
                match self
                    .store
                    .compare_exchange(
                        &encoded,
                        Some(&current.bytes),
                        &bytes,
                        mutation_identity(&encoded, Some(&current.bytes), &bytes),
                    )
                    .await?
                {
                    CasOutcome::Applied(_) => {
                        finished = true;
                        break;
                    }
                    CasOutcome::Conflict(observed) => value = observed,
                }
            }
            if !finished {
                return Err(CatalogError::Busy);
            }
        }
        Ok(())
    }
}
