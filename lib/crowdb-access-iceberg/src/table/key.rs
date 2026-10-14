use crate::{
    error::ValidationError,
    key::{CatalogId, CatalogScope, IcebergKey, NameSuffix, NamespaceId, TableId},
};

#[must_use]
pub fn storage_head_key(location: crate::file::TableLocation) -> crate::key::StorageKey {
    match location.storage_path() {
        Some(table) => crate::key::StorageKey::NativeTable(crate::key::TableStorageKey {
            table,
            scope: crate::key::TableStorageScope::Head,
            suffix: String::new(),
        }),
        None => crate::key::StorageKey::Legacy(head_key(location.catalog, location.table)),
    }
}

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
