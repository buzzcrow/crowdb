use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crowdb_access_iceberg::file::{
    FileBlockStore, FileIdentity, FileIoError, FileTree, FileTreeWriter, NATIVE_FILE_BLOCK_BYTES,
};
use http_body_util::BodyExt;
use hyper::body::{Body, Bytes};

#[derive(Clone, Copy, Debug)]
pub struct FileUploadConstraints {
    pub max_bytes: u64,
    pub content_length: Option<u64>,
    pub sha256: Option<[u8; 32]>,
}

impl FileUploadConstraints {
    fn validate(self) -> Result<(), FileUploadError> {
        if self.max_bytes == 0
            || self.max_bytes > u64::MAX / 8
            || self.content_length.is_some_and(|length| length > self.max_bytes)
        {
            return Err(FileUploadError::Bounds);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FileUploadError {
    #[error(transparent)]
    Storage(#[from] FileIoError),
    #[error("file upload capacity exhausted")]
    Busy,
    #[error("file upload byte bounds exceeded")]
    Bounds,
    #[error("file upload body read failed")]
    Body,
    #[error("file upload length differs from declared content length")]
    Length,
    #[error("file upload digest differs from signed payload digest")]
    Digest,
    #[error("file upload trailers are not supported")]
    Trailers,
}

pub struct FileUploadBudget {
    active: Arc<AtomicUsize>,
    limit: usize,
}

impl FileUploadBudget {
    /// # Errors
    /// Rejects empty or unbounded concurrent upload limits.
    pub fn new(limit: usize) -> Result<Self, FileUploadError> {
        if limit == 0 || limit > 64 {
            return Err(FileUploadError::Bounds);
        }
        Ok(Self {
            active: Arc::new(AtomicUsize::new(0)),
            limit,
        })
    }

    #[must_use]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    /// Stages bytes only; the caller must authorize intersected limits and seal before publication.
    /// Writes each received frame in bounded slices and never polls ahead of a pending storage write.
    /// # Errors
    /// Rejects exhausted admission, byte bounds, body failures and length/digest mismatches.
    /// Cancellation or failure retains orphan blocks without publishing any authority.
    pub async fn receive<Input: Body<Data = Bytes> + Unpin>(
        &self,
        mut body: Input,
        store: Arc<dyn FileBlockStore>,
        owner: FileIdentity,
        constraints: FileUploadConstraints,
    ) -> Result<FileTree, FileUploadError> {
        constraints.validate()?;
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.limit).then_some(active + 1)
            })
            .map_err(|_| FileUploadError::Busy)?;
        let _permit = Permit(self.active.clone());
        let mut writer = FileTreeWriter::new(store, owner, NATIVE_FILE_BLOCK_BYTES)?;
        while let Some(frame) = body.frame().await {
            let bytes = frame
                .map_err(|_| FileUploadError::Body)?
                .into_data()
                .map_err(|_| FileUploadError::Trailers)?;
            let length = writer
                .length()
                .checked_add(bytes.len() as u64)
                .ok_or(FileUploadError::Bounds)?;
            if length > constraints.max_bytes {
                return Err(FileUploadError::Bounds);
            }
            if constraints
                .content_length
                .is_some_and(|declared| length > declared)
            {
                return Err(FileUploadError::Length);
            }
            for chunk in bytes.chunks(64 * 1024) {
                writer.push(chunk).await?;
            }
        }
        if constraints
            .content_length
            .is_some_and(|declared| declared != writer.length())
        {
            return Err(FileUploadError::Length);
        }
        let tree = writer.finish().await?;
        if constraints.sha256.is_some_and(|digest| digest != tree.digest) {
            return Err(FileUploadError::Digest);
        }
        Ok(tree)
    }
}

struct Permit(Arc<AtomicUsize>);

impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
