use std::sync::Arc;

use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    ByteRange, FileBlockStore, FileGrant, FileGrantError, FileIdentity, FileLocation, FileOperation,
    FileRecord, FileTree, MultipartAdmissionRecord, MultipartLimits, MultipartPhase, MultipartSession,
};
use hyper::body::{Body, Bytes};

use super::file_body::{FileBodyError, FileReadBody, FileResponseBudget};
use super::file_request::{FileRequest, MultipartRequest};
use super::file_upload::{FileUploadBudget, FileUploadConstraints, FileUploadError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileServiceLimits {
    pub max_request_bytes: u64,
    pub max_file_bytes: u64,
    pub max_part_bytes: u64,
    pub max_staged_bytes: u64,
}

impl FileServiceLimits {
    fn validate(self) -> Result<(), FileAdmissionError> {
        if self.max_request_bytes == 0
            || self.max_file_bytes == 0
            || self.max_part_bytes == 0
            || self.max_staged_bytes == 0
            || self.max_request_bytes > u64::MAX / 8
            || self.max_file_bytes > u64::MAX / 8
            || self.max_part_bytes > u64::MAX / 8
            || self.max_staged_bytes > u64::MAX / 8
        {
            return Err(FileAdmissionError::Bounds);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FileAdmissionError {
    #[error(transparent)]
    Grant(#[from] FileGrantError),
    #[error("native file request is outside the authorized session")]
    Scope,
    #[error("native file request exceeds service or session limits")]
    Bounds,
    #[error("native file admission state is invalid or exhausted")]
    State,
    #[error(transparent)]
    Upload(#[from] FileUploadError),
    #[error(transparent)]
    Read(#[from] FileBodyError),
}

pub struct FileTransferAdmission {
    context: CatalogContext,
    principal: [u8; 32],
    location: FileLocation,
    operation: FileOperation,
    request_bytes: u64,
    file_bytes: u64,
    staged_bytes: u64,
}

impl FileTransferAdmission {
    #[must_use]
    pub const fn context(&self) -> CatalogContext {
        self.context
    }

    #[must_use]
    pub const fn principal(&self) -> [u8; 32] {
        self.principal
    }

    /// Returns limits for a newly created durable multipart session.
    /// # Errors
    /// Rejects non-create requests or an exhausted byte intersection.
    pub fn multipart_limits(&self) -> Result<MultipartLimits, FileAdmissionError> {
        if self.operation != FileOperation::CreateMultipart {
            return Err(FileAdmissionError::Scope);
        }
        let limits = MultipartLimits {
            max_parts: 10_000,
            max_part_bytes: self.request_bytes.min(self.file_bytes),
            max_file_bytes: self.file_bytes,
            max_staged_bytes: self.staged_bytes,
            ttl_ms: 24 * 60 * 60 * 1000,
        };
        limits.validate().map_err(|_| FileAdmissionError::Bounds)?;
        Ok(limits)
    }

    /// Intersects verified credentials with service and durable session bounds.
    /// # Errors
    /// Rejects wrong table, principal, operation, upload, expiry or missing credit.
    pub fn authorize(
        grant: &FileGrant,
        request: &FileRequest,
        service: FileServiceLimits,
        session: Option<&MultipartSession>,
        now_ms: u64,
    ) -> Result<Self, FileAdmissionError> {
        service.validate()?;
        if now_ms < grant.issued_ms || now_ms >= grant.expires_ms {
            return Err(FileAdmissionError::Grant(FileGrantError::Expired));
        }
        if !consistent(request) {
            return Err(FileAdmissionError::Scope);
        }
        grant.authorize(request.operation, &request.location, 0, 0)?;
        let mut request_bytes = grant.max_request_bytes.min(service.max_request_bytes);
        let mut file_bytes = grant.max_file_bytes.min(service.max_file_bytes);
        let mut staged_bytes = service.max_staged_bytes;
        match &request.multipart {
            None | Some(MultipartRequest::Create) if session.is_some() => {
                return Err(FileAdmissionError::Scope);
            }
            None => {}
            Some(MultipartRequest::Create) => {
                request_bytes = request_bytes.min(service.max_part_bytes);
            }
            Some(multipart) => {
                let session = session.ok_or(FileAdmissionError::Scope)?;
                session.validate().map_err(|_| FileAdmissionError::State)?;
                let upload_id = match multipart {
                    MultipartRequest::Upload { upload_id, .. }
                    | MultipartRequest::List { upload_id, .. }
                    | MultipartRequest::Complete { upload_id }
                    | MultipartRequest::Abort { upload_id } => upload_id,
                    MultipartRequest::Create => return Err(FileAdmissionError::State),
                };
                if session.context != grant.context
                    || session.principal != grant.principal
                    || session.location != request.location
                    || upload_id != &session.upload.to_string()
                {
                    return Err(FileAdmissionError::Scope);
                }
                if now_ms < session.created_ms || now_ms >= session.expires_ms {
                    return Err(FileAdmissionError::State);
                }
                if session.credit.map_or(true, |credit| credit.released)
                    && !(request.operation == FileOperation::CompleteMultipart
                        && session.phase == MultipartPhase::Published)
                {
                    return Err(FileAdmissionError::State);
                }
                file_bytes = file_bytes.min(session.limits.max_file_bytes);
                staged_bytes = staged_bytes.min(session.limits.max_staged_bytes);
                if let MultipartRequest::Upload { part_number, .. } = multipart {
                    if session.phase != MultipartPhase::Open || *part_number > session.limits.max_parts {
                        return Err(FileAdmissionError::State);
                    }
                    request_bytes = request_bytes
                        .min(service.max_part_bytes)
                        .min(session.limits.max_part_bytes);
                }
            }
        }
        Ok(Self {
            context: grant.context,
            principal: grant.principal,
            location: request.location.clone(),
            operation: request.operation,
            request_bytes,
            file_bytes,
            staged_bytes,
        })
    }

    /// Preflights a new multipart reservation; the caller must still run durable
    /// `MultipartAdmission::reserve` before exposing the upload ID.
    /// # Errors
    /// Rejects incompatible requested limits or exhausted admission snapshots.
    pub fn check_create(
        &self,
        session: &MultipartSession,
        policy: &MultipartAdmissionRecord,
    ) -> Result<(), FileAdmissionError> {
        if self.operation != FileOperation::CreateMultipart
            || session.validate().is_err()
            || policy.validate().is_err()
            || session.phase != MultipartPhase::Open
            || session.revision != 1
            || session.part_count != 0
            || session.pending.is_some()
            || session.credit.is_some()
            || session.context != self.context
            || session.principal != self.principal
            || session.location != self.location
            || policy.context != self.context
            || policy.pending.is_some()
        {
            return Err(FileAdmissionError::State);
        }
        if session.limits.max_part_bytes > self.request_bytes.min(self.file_bytes)
            || session.limits.max_file_bytes > self.file_bytes
            || session.limits.max_staged_bytes > self.staged_bytes
            || policy.sessions >= policy.limits.max_sessions
            || policy
                .reserved_bytes
                .checked_add(session.limits.max_staged_bytes)
                .map_or(true, |total| total > policy.limits.max_reserved_bytes)
        {
            return Err(FileAdmissionError::Bounds);
        }
        Ok(())
    }

    /// Checks actual transferred bytes against all intersected limits.
    /// # Errors
    /// Rejects a stream exceeding either the request or complete-file ceiling.
    pub fn check_bytes(&self, request_bytes: u64, file_bytes: u64) -> Result<(), FileAdmissionError> {
        if request_bytes > self.request_bytes || file_bytes > self.file_bytes {
            return Err(FileAdmissionError::Bounds);
        }
        Ok(())
    }

    pub(super) const fn request_byte_limit(&self) -> u64 {
        self.request_bytes
    }

    /// Receives a bounded immutable PUT or multipart part without publishing it.
    /// # Errors
    /// Rejects declared/actual size, digest, owner or storage failures.
    pub async fn receive<Input: Body<Data = Bytes> + Unpin>(
        &self,
        budget: &FileUploadBudget,
        body: Input,
        store: Arc<dyn FileBlockStore>,
        owner: FileIdentity,
        content_length: Option<u64>,
        sha256: Option<[u8; 32]>,
    ) -> Result<FileTree, FileAdmissionError> {
        if !matches!(self.operation, FileOperation::Put | FileOperation::UploadPart)
            || owner.table != self.location.table()
        {
            return Err(FileAdmissionError::Scope);
        }
        if let Some(length) = content_length {
            self.check_bytes(length, length)?;
        }
        let tree = budget
            .receive(
                body,
                store,
                owner,
                FileUploadConstraints {
                    max_bytes: self.request_bytes.min(self.file_bytes),
                    content_length,
                    sha256,
                },
            )
            .await?;
        self.check_bytes(tree.length, tree.length)?;
        Ok(tree)
    }

    /// Opens an authorized range only after checking the complete file and response bytes.
    /// # Errors
    /// Rejects another location, excessive range/file bytes or reader admission failure.
    pub fn read_body(
        &self,
        budget: &FileResponseBudget,
        store: Arc<dyn FileBlockStore>,
        record: FileRecord,
        range: Option<ByteRange>,
    ) -> Result<FileReadBody, FileAdmissionError> {
        if self.operation != FileOperation::Get || record.location != self.location {
            return Err(FileAdmissionError::Scope);
        }
        let bytes = range.map_or(record.length, |range| range.end.saturating_sub(range.start));
        self.check_bytes(bytes, record.length)?;
        Ok(budget.body(store, record, range)?)
    }
}

fn consistent(request: &FileRequest) -> bool {
    matches!(
        (request.operation, request.multipart.as_ref()),
        (
            FileOperation::Head | FileOperation::Get | FileOperation::Put,
            None
        ) | (FileOperation::CreateMultipart, Some(MultipartRequest::Create))
            | (FileOperation::UploadPart, Some(MultipartRequest::Upload { .. }))
            | (FileOperation::ListParts, Some(MultipartRequest::List { .. }))
            | (
                FileOperation::CompleteMultipart,
                Some(MultipartRequest::Complete { .. })
            )
            | (
                FileOperation::AbortMultipart,
                Some(MultipartRequest::Abort { .. })
            )
    )
}
