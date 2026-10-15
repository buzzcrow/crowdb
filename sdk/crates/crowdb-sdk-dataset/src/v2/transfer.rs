// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ControlClient, CreateUpload, Decimal, OpaqueId, UploadStatus};
use crate::{Error, UploadState};
use bytes::Bytes;
use md5::{Digest, Md5};
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};

impl ControlClient {
    /// Uploads a caller-owned stream from its beginning. The caller supplies the
    /// exact length and digest, and must reopen/reselect the same source on retry.
    /// Existing parts are verified against replayed bytes; no source is modified.
    /// Memory is bounded to one part using the shared SDK buffer admission.
    /// # Errors
    /// Rejects changed/truncated sources, invalid bounds, ownership or transport failures.
    pub async fn upload_reader<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
        request: &CreateUpload,
    ) -> Result<UploadStatus, Error> {
        validate(request)?;
        let status = self.create_upload(request).await?;
        if status.state == UploadState::Aborted {
            return Err(Error::Invalid("upload aborted".into()));
        }
        if status.id != request.id
            || status.length != request.length
            || status.part_size != request.part_size
            || status.md5 != request.md5
        {
            return Err(Error::Integrity);
        }
        let existing: std::collections::BTreeMap<_, _> =
            status.parts.iter().map(|part| (part.index, part)).collect();
        if existing.len() != status.parts.len() {
            return Err(Error::Integrity);
        }
        let mut remaining = request.length.0;
        let mut index = 0;
        let mut digest = Md5::new();
        while remaining > 0 {
            let size =
                usize::try_from(remaining.min(u64::from(request.part_size))).map_err(|_| Error::Limit)?;
            let _credit = self.inner.reserve_transfer(size)?;
            let mut data = vec![0; size];
            reader.read_exact(&mut data).await?;
            digest.update(&data);
            let md5 = hex::encode(Md5::digest(&data));
            if let Some(part) = existing.get(&index) {
                if part.length.0 != size as u64 || part.md5 != md5 {
                    return Err(Error::Integrity);
                }
            } else {
                let part = self.upload_part(&request.id, index, Bytes::from(data)).await?;
                if part.index != index || part.length.0 != size as u64 || part.md5 != md5 {
                    return Err(Error::Integrity);
                }
            }
            remaining -= size as u64;
            index += 1;
        }
        let mut extra = [0];
        if reader.read(&mut extra).await? != 0 || hex::encode(digest.finalize()) != request.md5 {
            return Err(Error::Integrity);
        }
        self.complete_upload(&request.id).await
    }

    /// Opens a local file, computes its digest using bounded buffers, then reuses
    /// the stream upload path. Retain the ID to resume an interrupted transfer.
    /// # Errors
    /// Returns file I/O, changed-source, capability, capacity or service errors.
    pub async fn upload_file(&self, path: impl AsRef<Path>, id: &OpaqueId) -> Result<UploadStatus, Error> {
        self.require("uploads").await?;
        let mut file = tokio::fs::File::open(path).await?;
        let length = file.metadata().await?.len();
        let part_size = 1024 * 1024;
        if length == 0 || length.div_ceil(part_size) > 10000 {
            return Err(Error::Limit);
        }
        let credit = self.inner.reserve_transfer(1024 * 1024)?;
        let mut buffer = vec![0; 1024 * 1024];
        let mut digest = Md5::new();
        let mut remaining = length;
        while remaining > 0 {
            let size = usize::try_from(remaining.min(part_size)).map_err(|_| Error::Limit)?;
            file.read_exact(&mut buffer[..size]).await?;
            digest.update(&buffer[..size]);
            remaining -= size as u64;
        }
        let mut extra = [0];
        if file.read(&mut extra).await? != 0 {
            return Err(Error::Integrity);
        }
        drop(buffer);
        drop(credit);
        file.rewind().await?;
        self.upload_reader(
            &mut file,
            &CreateUpload {
                id: id.clone(),
                length: Decimal(length),
                part_size: 1024 * 1024,
                md5: hex::encode(digest.finalize()),
            },
        )
        .await
    }
}
fn validate(request: &CreateUpload) -> Result<(), Error> {
    if request.length.0 == 0
        || request.part_size == 0
        || request.part_size > 4 * 1024 * 1024
        || request.length.0.div_ceil(u64::from(request.part_size)) > 10000
    {
        return Err(Error::Limit);
    }
    if request.md5.len() != 32
        || !request
            .md5
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Invalid("upload digest".into()));
    }
    Ok(())
}
