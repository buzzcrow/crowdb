// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use md5::{Digest, Md5};

use super::{DatasetUploads, UploadError, UploadPart, UploadRecord};

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
            self.verified_part(part).await?;
            return Ok(());
        }
        let mut hash = Md5::new();
        let mut length = 0_u64;
        for part in parts {
            let bytes = self.verified_part(part).await?;
            hash.update(&bytes);
            length = length.checked_add(part.length).ok_or(UploadError::Integrity)?;
        }
        if length != upload.length || <[u8; 16]>::from(hash.finalize()) != upload.md5 {
            return Err(UploadError::Integrity);
        }
        Ok(())
    }
}
