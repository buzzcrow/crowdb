// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use md5::{Digest, Md5};

use super::{DatasetUploads, UploadError, UploadPart, UploadRecord, VerifiedUploadPart};

impl DatasetUploads {
    pub(super) async fn verify_completion(
        &self,
        upload: &UploadRecord,
        parts: &[UploadPart],
    ) -> Result<(), UploadError> {
        if let [part] = parts {
            if part.length != upload.length || part.md5 != upload.md5 {
                return Err(UploadError::Integrity);
            }
            self.verify_streamed_part(part, None).await?;
            return Ok(());
        }
        let mut hash = Md5::new();
        let mut length = 0_u64;
        for part in parts {
            self.verify_streamed_part(part, Some(&mut hash)).await?;
            length = length.checked_add(part.length).ok_or(UploadError::Integrity)?;
        }
        if length != upload.length || <[u8; 16]>::from(hash.finalize()) != upload.md5 {
            return Err(UploadError::Integrity);
        }
        Ok(())
    }

    async fn verify_streamed_part(
        &self,
        part: &UploadPart,
        mut whole_digest: Option<&mut Md5>,
    ) -> Result<(), UploadError> {
        let stream = self.payloads.read_stream(&part.locations).await?;
        let mut stream = VerifiedUploadPart::new(part, stream);
        while let Some(payload) = stream.next().await? {
            if let Some(hash) = whole_digest.as_deref_mut() {
                hash.update(payload.as_ref());
            }
        }
        Ok(())
    }
}
