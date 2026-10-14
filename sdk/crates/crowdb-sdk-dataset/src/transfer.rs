// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    wire::{BatchEvent, FieldState},
    BatchRequest, DatasetClient, Error, Part, Upload, UploadRequest, UploadState,
};
use bytes::Bytes;
use md5::{Digest, Md5};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
const PART_SIZE: usize = 1024 * 1024;
#[derive(Clone, Copy, Debug)]
pub struct UploadOptions {
    pub part_size: usize,
    pub concurrency: usize,
}
impl Default for UploadOptions {
    fn default() -> Self {
        Self {
            part_size: PART_SIZE,
            concurrency: 4,
        }
    }
}

impl DatasetClient {
    /// Reserves SDK-owned native transfer buffers before allocation.
    /// # Errors
    /// Returns capacity or input-window limits.
    pub fn reserve_transfer(&self, size: usize) -> Result<crowdb_sdk_core::BufferPermit, Error> {
        self.http.reserve_buffer(size)
    }
    /// # Errors
    /// Returns invalid size, conflict, capacity, service or transport failures.
    pub async fn upload_part(&self, id: &str, index: u32, bytes: Bytes) -> Result<Part, Error> {
        if bytes.is_empty() || bytes.len() > 4 * PART_SIZE {
            return Err(Error::Limit);
        }
        let response = self
            .http
            .request_idempotent(
                "PUT",
                &self.path(&["uploads", id, "parts", &index.to_string()]),
                bytes,
                "application/octet-stream",
            )
            .await
            .map_err(|e| e.with_identity(id))?;
        Ok(serde_json::from_slice(&response)?)
    }
    /// Uploads a seekable file with a digest pass and bounded transfer pass.
    /// Reuse the logical ID after interruption; accepted parts are not allocated again.
    /// # Errors
    /// Reports local I/O, integrity, expiry, capacity and service failures.
    pub async fn upload_file(&self, path: impl AsRef<Path>, id: &str) -> Result<Upload, Error> {
        self.upload_file_with_options(path, id, UploadOptions::default())
            .await
    }
    /// # Errors
    /// Rejects invalid windows, local file changes, integrity or service failures.
    pub async fn upload_file_with_options(
        &self,
        path: impl AsRef<Path>,
        id: &str,
        options: UploadOptions,
    ) -> Result<Upload, Error> {
        if options.part_size == 0
            || options.part_size > 4 * PART_SIZE
            || options.concurrency == 0
            || options.concurrency > 64
        {
            return Err(Error::Invalid("upload window".into()));
        }
        let mut file = tokio::fs::File::open(path).await?;
        let length = file.metadata().await?.len();
        if length == 0 || length.div_ceil(options.part_size as u64) > 10_000 {
            return Err(Error::Limit);
        }
        let mut hash = Md5::new();
        let digest_credit = self.http.reserve_buffer(PART_SIZE)?;
        let mut buffer = vec![0; PART_SIZE];
        loop {
            let count = file.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            hash.update(&buffer[..count]);
        }
        drop(buffer);
        drop(digest_credit);
        let status = self
            .create_upload(&UploadRequest {
                id: id.into(),
                length,
                part_size: u32::try_from(options.part_size).map_err(|_| Error::Limit)?,
                md5: hex::encode(hash.finalize()),
            })
            .await?;
        if matches!(status.state, UploadState::Complete | UploadState::Claimed) {
            return Ok(status);
        }
        if status.state != UploadState::Open {
            return Err(Error::Invalid("upload aborted".into()));
        }
        let existing: std::collections::BTreeMap<_, _> = status.parts.iter().map(|p| (p.index, p)).collect();
        file.rewind().await?;
        let mut remaining = length;
        let mut index = 0;
        let mut tasks = tokio::task::JoinSet::new();
        while remaining > 0 {
            let size = usize::try_from(remaining.min(options.part_size as u64)).map_err(|_| Error::Limit)?;
            let credit = self.http.reserve_buffer(size)?;
            let mut bytes = vec![0; size];
            file.read_exact(&mut bytes).await?;
            let md5 = hex::encode(Md5::digest(&bytes));
            if let Some(part) = existing.get(&index) {
                if part.md5 != md5 || part.length != size as u64 {
                    return Err(Error::Integrity);
                }
            } else {
                if tasks.len() == options.concurrency {
                    tasks
                        .join_next()
                        .await
                        .ok_or_else(|| Error::Invalid("missing upload task".into()))?
                        .map_err(|_| Error::Invalid("upload task cancelled".into()))??;
                }
                let client = self.clone();
                let id = id.to_owned();
                let bytes = Bytes::from(bytes);
                tasks.spawn(async move {
                    let _credit = credit;
                    let receipt = client.upload_part(&id, index, bytes).await?;
                    if receipt.index != index || receipt.length != size as u64 || receipt.md5 != md5 {
                        return Err(Error::Integrity);
                    }
                    Ok::<_, Error>(())
                });
            }
            remaining -= size as u64;
            index += 1;
        }
        while let Some(result) = tasks.join_next().await {
            result.map_err(|_| Error::Invalid("upload task cancelled".into()))??;
        }
        self.complete_upload(id).await
    }
    /// Writes one field through a single batch stream. On error the caller-owned
    /// destination may contain a verified prefix and must be discarded.
    /// # Errors
    /// Reports not-found, corrupt/truncated streams, deadline or local I/O failures.
    pub async fn download_field<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        snapshot: &str,
        sample: &str,
        field: &str,
        writer: &mut W,
    ) -> Result<(), Error> {
        let request = BatchRequest {
            snapshot: snapshot.into(),
            sample_ids: vec![sample.into()],
            fields: vec![field.into()],
        };
        let mut stream = self.stream_batch(&request).await?;
        let mut valid = false;
        while let Some(event) = stream.next_event().await? {
            match event {
                BatchEvent::Header(header) => {
                    if header.snapshot != snapshot
                        || header.fields.len() != 1
                        || header.fields[0].sample != sample
                        || header.fields[0].field != field
                        || !matches!(header.fields[0].state, FieldState::Value)
                    {
                        return Err(Error::Invalid("field not found".into()));
                    }
                }
                BatchEvent::Data(bytes) => writer.write_all(&bytes).await?,
                BatchEvent::FieldEnd => {}
                BatchEvent::End => valid = true,
            }
        }
        if !valid {
            return Err(Error::Integrity);
        }
        writer.flush().await?;
        Ok(())
    }
    /// Publishes a destination path only after complete field verification.
    /// # Errors
    /// Reports transfer/integrity/local I/O failures, preserving the previous path.
    pub async fn download_to_path(
        &self,
        snapshot: &str,
        sample: &str,
        field: &str,
        path: impl AsRef<Path>,
    ) -> Result<(), Error> {
        let path = path.as_ref();
        let temporary = path.with_file_name(format!(".crowdb-{}.tmp", uuid::Uuid::new_v4()));
        let mut cleanup = TemporaryPath(Some(temporary.clone()));
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .await?;
        let result = self.download_field(snapshot, sample, field, &mut file).await;
        match result {
            Ok(()) => {
                file.sync_all().await?;
                drop(file);
                tokio::fs::rename(&temporary, path).await?;
                cleanup.0 = None;
                Ok(())
            }
            Err(error) => {
                drop(file);
                let _ = tokio::fs::remove_file(&temporary).await;
                Err(error)
            }
        }
    }
}

struct TemporaryPath(Option<std::path::PathBuf>);
impl Drop for TemporaryPath {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = tokio::fs::remove_file(path).await;
                });
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}
