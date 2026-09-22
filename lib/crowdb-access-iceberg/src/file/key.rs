use crate::key::{CatalogId, CatalogScope, FileId, IcebergKey};

use super::FileLocation;

#[must_use]
pub fn file_key(catalog: CatalogId, file: FileId) -> IcebergKey {
    IcebergKey::Catalog {
        catalog,
        scope: CatalogScope::File,
        suffix: file.as_bytes().to_vec(),
    }
}

#[must_use]
pub fn location_key(location: &FileLocation) -> IcebergKey {
    let mut suffix = location.table().table.as_bytes().to_vec();
    suffix.extend_from_slice(location.relative_key().as_bytes());
    IcebergKey::Catalog {
        catalog: location.table().catalog,
        scope: CatalogScope::FileLocation,
        suffix,
    }
}
