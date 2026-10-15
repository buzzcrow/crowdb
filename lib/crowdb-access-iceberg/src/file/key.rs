use crate::key::{
    CatalogId, CatalogScope, FileId, IcebergKey, StorageKey, TableStorageKey, TableStorageScope,
};

use super::FileLocation;

#[must_use]
pub fn canonical_file_key(location: &FileLocation) -> StorageKey {
    match location.table().storage_path() {
        Some(table) => StorageKey::NativeTable(TableStorageKey {
            table,
            scope: TableStorageScope::File,
            suffix: location.relative_key().to_owned(),
        }),
        None => StorageKey::Legacy(location_key(location)),
    }
}

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
