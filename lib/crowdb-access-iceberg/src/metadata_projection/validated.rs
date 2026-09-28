use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::{
    file::FileRecord,
    record::StorageRecord,
    table::{TableHead, TableMetadataDocument, TableMetadataLimits},
};

use super::{model::Root, ProjectionIdentity, ProjectionStore, MAX_PROJECTION_BYTES};

impl ProjectionStore {
    pub(crate) async fn put_document(
        &self,
        record: &FileRecord,
        document: &TableMetadataDocument,
        limits: TableMetadataLimits,
    ) -> bool {
        let head = document.selected_head();
        if record.file != head.metadata_file
            || record.location != head.metadata_location
            || record.digest != head.metadata_digest
            || !self.put(record, head.generation, document.canonical()).await
        {
            return false;
        }
        let identity = ProjectionIdentity::new(record, head.generation);
        let Some(key) = identity.key(0, 0) else {
            return false;
        };
        let Ok(Some(root)) = self.store.get(&key).await else {
            return false;
        };
        let Some(receipt) = receipt(head, limits, &root.bytes) else {
            return false;
        };
        self.put_page(identity, 0, 1, &receipt).await
    }

    pub(crate) async fn document_fields(
        &self,
        record: &FileRecord,
        head: &TableHead,
        limits: TableMetadataLimits,
    ) -> Option<BTreeMap<String, Vec<u8>>> {
        if record.length > MAX_PROJECTION_BYTES as u64 || record.length > limits.bytes as u64 {
            return None;
        }
        let identity = ProjectionIdentity::new(record, head.generation);
        let encoded = self.store.get(&identity.key(0, 0)?).await.ok()??;
        let root = Root::decode(&encoded.bytes, identity, record.length)?;
        let stored = self.store.get(&identity.key(0, 1)?).await.ok()??;
        if stored.bytes != receipt(head, limits, &encoded.bytes)? {
            return None;
        }
        let mut fields = BTreeMap::new();
        for (name, child) in &root.children {
            fields.insert(name.clone(), self.read_child(identity, child).await?);
        }
        Some(fields)
    }
}

fn receipt(head: &TableHead, limits: TableMetadataLimits, root: &[u8]) -> Option<Vec<u8>> {
    limits.validate().ok()?;
    let mut digest = Sha256::new();
    digest.update(b"crowdb-iceberg-validated-metadata-v1");
    digest.update(StorageRecord::TableHead(Box::new(head.clone())).encode().ok()?);
    for limit in [
        limits.bytes,
        limits.values,
        limits.depth,
        limits.string_bytes,
        limits.collection_entries,
    ] {
        digest.update(u64::try_from(limit).ok()?.to_be_bytes());
    }
    digest.update(root);
    Some(digest.finalize().to_vec())
}
