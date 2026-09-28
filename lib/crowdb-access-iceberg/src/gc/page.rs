use crate::{
    error::ValidationError,
    key::{CatalogId, CatalogScope, IcebergKey, OperationId},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcPage {
    pub catalog: CatalogId,
    pub task: OperationId,
    pub kind: u8,
    pub sequence: u64,
    pub entries: Vec<Vec<u8>>,
}

impl GcPage {
    #[must_use]
    pub fn key(&self) -> IcebergKey {
        let mut suffix = self.task.as_bytes().to_vec();
        suffix.push(self.kind);
        suffix.extend_from_slice(&self.sequence.to_be_bytes());
        IcebergKey::Catalog {
            catalog: self.catalog,
            scope: CatalogScope::GcPage,
            suffix,
        }
    }

    /// # Errors
    /// Rejects oversized, empty or unordered pages and foreign file keys.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.kind > 3
            || self.entries.is_empty()
            || self.entries.len() > 256
            || self.entries.iter().map(Vec::len).sum::<usize>() > 48 * 1024
            || self.entries.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(ValidationError::Record);
        }
        for entry in &self.entries {
            match IcebergKey::decode(entry)? {
                IcebergKey::Catalog { catalog, .. } if catalog == self.catalog => {}
                _ => return Err(ValidationError::IdentityMismatch),
            }
        }
        Ok(())
    }
}
