use std::ops::Range;

use crate::error::ValidationError;
use crate::key::{CatalogId, CatalogScope, IcebergKey, NameSuffix, NamespaceId};

#[must_use]
pub fn authority_key(catalog: CatalogId, namespace: NamespaceId) -> IcebergKey {
    IcebergKey::Catalog {
        catalog,
        scope: CatalogScope::NamespaceAuthority,
        suffix: namespace.as_bytes().to_vec(),
    }
}

/// # Errors
/// Rejects names that cannot be represented in the ordered index.
pub fn name_key(
    catalog: CatalogId,
    parent: Option<NamespaceId>,
    name: &str,
) -> Result<IcebergKey, ValidationError> {
    Ok(IcebergKey::Catalog {
        catalog,
        scope: CatalogScope::NamespaceName,
        suffix: NameSuffix { parent, name }.encode()?,
    })
}

/// # Errors
/// Table ranges require a namespace parent; only child-index scopes are valid.
pub fn child_range(
    catalog: CatalogId,
    parent: Option<NamespaceId>,
    scope: CatalogScope,
) -> Result<Range<Vec<u8>>, ValidationError> {
    if !matches!(scope, CatalogScope::NamespaceName | CatalogScope::TableName)
        || (scope == CatalogScope::TableName && parent.is_none())
    {
        return Err(ValidationError::Key);
    }
    let mut start = IcebergKey::catalog_range(catalog).start;
    start.push(scope as u8);
    let parent_bytes = parent.as_ref().map_or(&[0; 16], NamespaceId::as_bytes);
    start.extend_from_slice(parent_bytes);
    let mut end = start.clone();
    while end.last() == Some(&u8::MAX) {
        end.pop();
    }
    let last = end.last_mut().ok_or(ValidationError::Key)?;
    *last += 1;
    Ok(start..end)
}
