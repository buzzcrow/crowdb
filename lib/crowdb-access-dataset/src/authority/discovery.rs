// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{head::decode_head, VersionedRecord};
use crate::{
    inspection::{work::InspectionWork, InspectionError, InspectionPage},
    key, AuthorityError, DatasetAuthority, DatasetError, DatasetId, DatasetRecord, MetadataScanBudget,
    NamespacePath, ReadCancellation,
};
use bincode::Options;

impl DatasetAuthority {
    /// Discovers existing Dataset records directly, including records predating inspection indexes.
    /// Head names are observed independently for each Dataset; no Manifest is loaded.
    /// # Errors
    /// Returns invalid namespaces/cursors, corrupt identities, cancellation or aggregate limits.
    pub async fn inspect_datasets_page(
        &self,
        namespace: &NamespacePath,
        after: Option<Vec<u8>>,
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPage<DatasetRecord>, InspectionError> {
        budget.validate()?;
        if !namespace.is_default() {
            return Err(AuthorityError::Invalid(DatasetError::InvalidDefaultNamespace).into());
        }
        if budget.items > 100 || budget.bytes > 1024 * 1024 {
            return Err(InspectionError::Limit);
        }
        let mut prefix = key::DATASET_PREFIX.to_vec();
        let mut scope = namespace.encode_key("x").map_err(AuthorityError::from)?;
        scope.truncate(scope.len() - 3);
        prefix.extend(scope);
        let mut work = InspectionWork::new(self.store.as_ref(), cancel, budget)?;
        let cursor = crate::inspection::cursor::CursorScope::new(&prefix, b"datasets");
        let rows = work.scan(prefix, cursor.decode(after)?, budget.items).await?;
        let mut entries = Vec::new();
        for (stored_key, value) in rows.entries {
            let record: VersionedRecord<DatasetRecord> = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_limit(value.bytes.len() as u64)
                .reject_trailing_bytes()
                .deserialize(&value.bytes)
                .map_err(|_| InspectionError::Corrupt)?;
            if record.version != 1 {
                return Err(InspectionError::Corrupt);
            }
            let mut record = record.payload;
            record.validate().map_err(AuthorityError::from)?;
            DatasetId::from_bytes(record.id.as_bytes()).map_err(AuthorityError::from)?;
            if record.identity.namespace() != namespace
                || key::dataset(&record.identity).map_err(AuthorityError::from)? != stored_key
            {
                return Err(InspectionError::Corrupt);
            }
            let head = work
                .get(&key::head(&record.identity).map_err(AuthorityError::from)?, 8192)
                .await?;
            let head = head.map(|v| decode_head(&v.bytes)).transpose()?;
            record.latest = head.as_ref().and_then(|h| h.latest);
            record.stable = head.and_then(|h| h.stable);
            entries.push(record);
        }
        Ok(InspectionPage {
            entries,
            after: cursor.encode(rows.after)?,
        })
    }
}
