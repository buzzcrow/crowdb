// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{MultipartUpload, PartMetadata};
use crate::s3::S3Client;
use crate::{Error, OperationContext, Result};
use crowdb_access_s3::execution::map_multipart_error;
use crowdb_access_s3::S3ErrorCode;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct PartPage {
    pub parts: Vec<PartMetadata>,
    pub next_part_number_marker: Option<u16>,
}

pub struct ListUploads {
    pub prefix: Vec<u8>,
    pub key_marker: Option<Vec<u8>>,
    pub upload_marker: Option<[u8; 16]>,
    pub limit: usize,
}

impl Default for ListUploads {
    fn default() -> Self {
        Self {
            prefix: Vec::new(),
            key_marker: None,
            upload_marker: None,
            limit: 1000,
        }
    }
}

pub struct UploadsPage {
    pub uploads: Vec<MultipartUpload>,
    pub next: Option<(Vec<u8>, [u8; 16])>,
}

impl S3Client {
    /// # Errors
    /// Returns invalid paging, foreign/closed session or bounded metadata failure.
    pub async fn list_parts(
        &self,
        context: OperationContext,
        upload: &MultipartUpload,
        after_number: u16,
        limit: usize,
    ) -> Result<PartPage> {
        self.validate_upload(upload)?;
        if !(1..=1000).contains(&limit) || after_number > 10_000 {
            return Err(Error::S3(S3ErrorCode::InvalidRequest));
        }
        let request = self.runtime.resources.admit(context)?;
        let page = request
            .run(async {
                let current = self
                    .execution
                    .refresh_multipart(&upload.session, false)
                    .await
                    .map_err(|error| super::map_error(error, request.context().request_id.get()))?;
                self.execution
                    .multipart_repository()
                    .list_parts(&current, after_number, limit)
                    .await
                    .map_err(|error| Error::S3(map_multipart_error(&error)))
            })
            .await?;
        Ok(PartPage {
            parts: page.parts.into_iter().map(Into::into).collect(),
            next_part_number_marker: page.next_part_number_marker,
        })
    }

    /// # Errors
    /// Returns invalid markers, namespace failure or exhausted bounded scan budget.
    pub async fn list_multipart_uploads(
        &self,
        context: OperationContext,
        bucket: &[u8],
        options: &ListUploads,
    ) -> Result<UploadsPage> {
        if !(1..=1000).contains(&options.limit)
            || options.upload_marker.is_some() && options.key_marker.is_none()
        {
            return Err(Error::S3(S3ErrorCode::InvalidRequest));
        }
        let request = self.runtime.resources.admit(context)?;
        let page = request
            .run(async {
                let bucket = self.execution.resolve_bucket(bucket).await.map_err(Error::S3)?;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |duration| {
                        u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
                    });
                self.execution
                    .multipart_repository()
                    .list_uploads(
                        bucket,
                        &options.prefix,
                        options.key_marker.as_deref(),
                        options.upload_marker.as_ref(),
                        options.limit,
                        now,
                    )
                    .await
                    .map_err(|error| Error::S3(map_multipart_error(&error)))
            })
            .await?;
        Ok(UploadsPage {
            uploads: page
                .uploads
                .into_iter()
                .map(|session| self.bind_upload(session))
                .collect(),
            next: page.next,
        })
    }
}
