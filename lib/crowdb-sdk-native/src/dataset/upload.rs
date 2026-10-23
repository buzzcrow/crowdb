// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{now, DatasetClient};
use crate::{
    buffer::{BufferLease, FrozenObjectBuffer},
    Error, OperationContext, Result,
};
use crowdb_access_dataset::{DatasetUploads, OperationId, UploadPart, UploadRecord};
use md5::{Digest, Md5};

impl DatasetClient {
    /// # Errors
    /// Returns invalid size/digest/expiry, divergent identity or storage failure.
    pub async fn create_upload(
        &self,
        context: OperationContext,
        id: OperationId,
        length: u64,
        part_size: u32,
        md5: [u8; 16],
        expires_at: u64,
    ) -> Result<UploadRecord> {
        let location = DatasetUploads::location(&self.identity, id)?;
        self.control(context, |scope| async move {
            Ok(scope
                .uploads
                .create(&location, length, part_size, md5, expires_at)
                .await?)
        })
        .await
    }

    /// Keeps the admitted caller owner until the durable part write completes.
    /// # Errors
    /// Rejects foreign owners, invalid parts or ambiguous storage outcomes.
    pub async fn upload_part(
        &self,
        context: OperationContext,
        id: OperationId,
        index: u32,
        data: BufferLease,
    ) -> Result<UploadPart> {
        if !data.belongs_to(&self.runtime.resources) {
            return Err(Error::BufferRange);
        }
        let location = DatasetUploads::location(&self.identity, id)?;
        self.control(context, |scope| async move {
            Ok(scope
                .uploads
                .put_part(&location, index, data.into_bytes(), now())
                .await?)
        })
        .await
    }

    /// Writes payload prepared in final frame slots, preserving its registered owner.
    /// # Errors
    /// Rejects foreign owners, invalid sizes, conflict or failed durability.
    pub async fn upload_framed_part(
        &self,
        context: OperationContext,
        id: OperationId,
        index: u32,
        data: FrozenObjectBuffer,
    ) -> Result<UploadPart> {
        if !data.belongs_to(&self.runtime.resources) {
            return Err(Error::BufferRange);
        }
        let location = DatasetUploads::location(&self.identity, id)?;
        self.control(context, |scope| async move {
            let mut digest = Md5::new();
            for payload in data.payload_views() {
                digest.update(&payload);
            }
            Ok(scope
                .uploads
                .put_framed_part(
                    &location,
                    index,
                    Box::new(data.owner),
                    digest.finalize().into(),
                    now(),
                )
                .await?)
        })
        .await
    }

    /// # Errors
    /// Rejects incomplete/corrupt parts, expired uploads or uncertain storage.
    pub async fn complete_upload(&self, context: OperationContext, id: OperationId) -> Result<UploadRecord> {
        let location = DatasetUploads::location(&self.identity, id)?;
        self.control(context, |scope| async move {
            Ok(scope.uploads.complete(&location, now()).await?)
        })
        .await
    }
}
