// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{key, work::InspectionWork, InspectionError, InspectionPage};
use crate::{
    AuthorityError, DatasetAuthority, DatasetIdentity, DatasetUploads, FieldLocator, MetadataScanBudget,
    OperationId, ReadCancellation, SnapshotId,
};
use std::time::Duration;

pub(crate) struct ManagedUpload {
    pub id: OperationId,
    pub length: u64,
    pub md5: [u8; 16],
}
impl DatasetAuthority {
    pub(crate) async fn composition_candidate_uploads_page(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        after: Option<Vec<u8>>,
        cancel: &ReadCancellation,
    ) -> Result<InspectionPage<ManagedUpload>, InspectionError> {
        let mut work = InspectionWork::new(
            self.store.as_ref(),
            cancel,
            MetadataScanBudget {
                items: 20,
                bytes: 1024 * 1024,
                timeout: Duration::from_secs(2),
            },
        )?;
        self.ready_inspection_projection(identity, snapshot, &mut work)
            .await?;
        let prefix = key::scope(identity, snapshot, b'V').map_err(AuthorityError::from)?;
        let page = work.scan(prefix, after, 20).await?;
        let mut entries = Vec::new();
        for (_, value) in page.entries {
            let locator: FieldLocator =
                serde_json::from_slice(&value.bytes).map_err(|_| InspectionError::Corrupt)?;
            if let FieldLocator::Chunk {
                location,
                length,
                md5,
            } = locator
            {
                if let Ok(id) = DatasetUploads::upload_id(identity, &location) {
                    entries.push(ManagedUpload { id, length, md5 });
                }
            }
        }
        Ok(InspectionPage {
            entries,
            after: page.after,
        })
    }
}
