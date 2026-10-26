use crate::{
    error::ValidationError,
    key::{NamespaceId, OperationId},
    table::{name_key, TableMapping, TableMappingState},
};

/// Bounded name-move occupancy shared by the affected namespace authorities.
/// Contains no request payload, response or execution phase.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceRename {
    pub source: TableMapping,
    pub destination_namespace: NamespaceId,
    pub destination_name: String,
    pub operation: OperationId,
    pub head_generation: u64,
    pub head_fence: u64,
}

impl NamespaceRename {
    /// # Errors
    /// Rejects malformed names, unchanged destinations and exhausted head fences.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.source.validate()?;
        name_key(
            self.source.catalog,
            self.destination_namespace,
            &self.destination_name,
        )?;
        if self.source.state != TableMappingState::Published
            || self.head_generation == 0
            || self.head_fence == 0
            || self.head_fence == u64::MAX
            || self.source.name_epoch == u64::MAX
            || (self.source.namespace == self.destination_namespace
                && self.source.name == self.destination_name)
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }

    pub(crate) fn destination(&self, state: TableMappingState) -> TableMapping {
        TableMapping {
            namespace: self.destination_namespace,
            name: self.destination_name.clone(),
            name_epoch: self.source.name_epoch + 1,
            operation: self.operation,
            state,
            ..self.source.clone()
        }
    }

    pub(crate) fn parents(&self) -> Vec<NamespaceId> {
        let mut parents = vec![self.source.namespace, self.destination_namespace];
        parents.sort_unstable_by_key(|id| *id.as_bytes());
        parents.dedup();
        parents
    }
}
