use crate::{
    error::ValidationError,
    key::{CatalogId, CatalogScope, IcebergKey, NameSuffix, NamespaceId, TableId},
};

#[must_use]
pub fn head_key(catalog: CatalogId, table: TableId) -> IcebergKey {
    IcebergKey::Catalog {
        catalog,
        scope: CatalogScope::TableHead,
        suffix: table.as_bytes().to_vec(),
    }
}

/// # Errors
/// Rejects unrepresentable names before encoding the namespace-qualified key.
pub fn name_key(
    catalog: CatalogId,
    namespace: NamespaceId,
    name: &str,
) -> Result<IcebergKey, ValidationError> {
    Ok(IcebergKey::Catalog {
        catalog,
        scope: CatalogScope::TableName,
        suffix: NameSuffix {
            parent: Some(namespace),
            name,
        }
        .encode()?,
    })
}
