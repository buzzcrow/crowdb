use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use crowdb_access_iceberg::catalog::{CatalogContext, CatalogError};
use crowdb_access_iceberg::file::{
    FileBlockStore, FileIdentity, FileOperation, FileReader, FileSealError, FileSealer, FileTree,
    MultipartAdmissionLimits, MultipartPart, MultipartPhase, MultipartSession, MultipartWorkError,
};
use crowdb_access_iceberg::key::{FileId, OperationId};
use crowdb_access_s3::auth::StreamingPayloadVerifier;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::http::header::HeaderValue;
use hyper::{Request, Response};
use md5::Md5;
use sha2::{Digest, Sha256};

use super::{admission_error, catalog_error, FileHttp, FileS3ErrorCode, FileTransferAdmission};
use crate::iceberg::body::IcebergBody;
use crate::iceberg::file_request::{FileRequest, MultipartRequest};
use crate::iceberg::file_response::MultipartResponses;
use crate::iceberg::{FileEncodingError, FileUploadBody};

impl FileHttp {
    pub(super) async fn load_session(
        &self,
        context: CatalogContext,
        request: &FileRequest,
    ) -> Result<Option<MultipartSession>, FileS3ErrorCode> {
        let upload_id = match &request.multipart {
            None | Some(MultipartRequest::Create) => return Ok(None),
            Some(
                MultipartRequest::Upload { upload_id, .. }
                | MultipartRequest::List { upload_id, .. }
                | MultipartRequest::Complete { upload_id }
                | MultipartRequest::Abort { upload_id },
            ) => upload_id,
        };
        let upload = upload_id
            .parse::<OperationId>()
            .map_err(|_| FileS3ErrorCode::NoSuchUpload)?;
        self.multipart
            .load(context, upload)
            .await
            .map_err(catalog_error)?
            .ok_or(FileS3ErrorCode::NoSuchUpload)
            .map(Some)
    }

    pub(super) async fn multipart_request(
        self: &Arc<Self>,
        file: &FileRequest,
        request: Request<Incoming>,
        session: Option<MultipartSession>,
        admission: &FileTransferAdmission,
        now_ms: u64,
        streaming: Option<StreamingPayloadVerifier>,
    ) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        match (&file.multipart, file.operation) {
            (Some(MultipartRequest::Create), FileOperation::CreateMultipart) => {
                self.create(file, admission, now_ms).await
            }
            (Some(MultipartRequest::Upload { part_number, .. }), FileOperation::UploadPart) => {
                self.upload(
                    session.ok_or(FileS3ErrorCode::NoSuchUpload)?,
                    *part_number,
                    request,
                    admission,
                    now_ms,
                    streaming,
                )
                .await
            }
            (
                Some(MultipartRequest::List {
                    marker, max_parts, ..
                }),
                FileOperation::ListParts,
            ) => {
                let session = session.ok_or(FileS3ErrorCode::NoSuchUpload)?;
                let page = self
                    .lister
                    .list(&session, *marker, *max_parts, now_ms)
                    .await
                    .map_err(catalog_error)?;
                MultipartResponses::list_parts(&session, &page, *marker, *max_parts)
                    .map(|response| response.map(IcebergBody::new))
                    .map_err(|_| FileS3ErrorCode::InternalError)
            }
            (Some(MultipartRequest::Abort { .. }), FileOperation::AbortMultipart) => {
                self.abort(session.ok_or(FileS3ErrorCode::NoSuchUpload)?).await
            }
            (Some(MultipartRequest::Complete { .. }), FileOperation::CompleteMultipart) => {
                self.complete(session.ok_or(FileS3ErrorCode::NoSuchUpload)?, request, now_ms)
                    .await
            }
            _ => Err(FileS3ErrorCode::InvalidRequest),
        }
    }

    async fn create(
        &self,
        request: &FileRequest,
        admission: &FileTransferAdmission,
        now_ms: u64,
    ) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        let limits = admission.multipart_limits().map_err(admission_error)?;
        let session = MultipartSession {
            context: admission.context(),
            upload: OperationId::random(),
            owner: FileIdentity {
                table: request.location.table(),
                file: FileId::random(),
            },
            location: request.location.clone(),
            principal: admission.principal(),
            revision: 1,
            created_ms: now_ms,
            expires_ms: now_ms
                .checked_add(limits.ttl_ms)
                .ok_or(FileS3ErrorCode::InternalError)?,
            limits,
            phase: MultipartPhase::Open,
            part_count: 0,
            staged_bytes: 0,
            completion: None,
            published: None,
            pending: None,
            credit: None,
        };
        let policy = self
            .admission
            .initialize(
                session.context,
                MultipartAdmissionLimits {
                    max_sessions: 1024,
                    max_reserved_bytes: 64 * 1024 * 1024 * 1024 * 1024,
                },
            )
            .await
            .map_err(catalog_error)?;
        admission
            .check_create(&session, &policy)
            .map_err(admission_error)?;
        if !self
            .admission
            .reserve(&policy, &session, now_ms)
            .await
            .map_err(catalog_error)?
        {
            return Err(FileS3ErrorCode::SlowDown);
        }
        let durable = self
            .multipart
            .load(session.context, session.upload)
            .await
            .map_err(catalog_error)?
            .ok_or(FileS3ErrorCode::InternalError)?;
        MultipartResponses::create(&durable)
            .map(|response| response.map(IcebergBody::new))
            .map_err(|_| FileS3ErrorCode::InternalError)
    }

    async fn upload(
        &self,
        session: MultipartSession,
        part_number: u16,
        request: Request<Incoming>,
        admission: &FileTransferAdmission,
        now_ms: u64,
        streaming: Option<StreamingPayloadVerifier>,
    ) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        let digest = if streaming.is_some() {
            None
        } else {
            signed_digest(request.headers().get("x-amz-content-sha256"))?
        };
        let content_md5 = request.headers().get("content-md5").cloned();
        let (parts, body) = request.into_parts();
        let mut body = FileUploadBody::new(body, &parts.headers, streaming, admission.request_byte_limit())
            .map_err(encoding_error)?;
        let length = body.decoded_length();
        let owner = FileIdentity {
            table: session.owner.table,
            file: FileId::random(),
        };
        let tree = admission
            .receive(
                &self.uploads,
                &mut body,
                self.blocks.clone(),
                owner,
                length,
                digest,
            )
            .await
            .map_err(|error| {
                body.failure()
                    .map_or_else(|| admission_error(error), encoding_error)
            })?;
        verify_md5(self.blocks.clone(), owner, tree.clone(), content_md5.as_ref()).await?;
        let before = self
            .multipart
            .part(&session, part_number)
            .await
            .map_err(catalog_error)?;
        let part = MultipartPart {
            upload: session.upload,
            number: part_number,
            revision: before.map_or(1, |part| part.revision.checked_add(1).unwrap_or(0)),
            modified_ms: now_ms,
            owner,
            tree,
        };
        if !self
            .multipart
            .reserve_part(&session, &part, now_ms)
            .await
            .map_err(catalog_error)?
        {
            return Err(FileS3ErrorCode::SlowDown);
        }
        let pending = self
            .multipart
            .load(session.context, session.upload)
            .await
            .map_err(catalog_error)?
            .ok_or(FileS3ErrorCode::InternalError)?;
        if !self
            .multipart
            .settle_part(&pending)
            .await
            .map_err(catalog_error)?
        {
            return Err(FileS3ErrorCode::SlowDown);
        }
        let settled = self
            .multipart
            .load(session.context, session.upload)
            .await
            .map_err(catalog_error)?
            .ok_or(FileS3ErrorCode::InternalError)?;
        let part = self
            .multipart
            .part(&settled, part_number)
            .await
            .map_err(catalog_error)?
            .ok_or(FileS3ErrorCode::InternalError)?;
        MultipartResponses::upload_part(&part)
            .map(|response| response.map(IcebergBody::new))
            .map_err(|_| FileS3ErrorCode::InternalError)
    }

    async fn abort(&self, session: MultipartSession) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        if !self.multipart.abort(&session).await.map_err(catalog_error)? {
            return Err(FileS3ErrorCode::SlowDown);
        }
        let terminal = self
            .multipart
            .load(session.context, session.upload)
            .await
            .map_err(catalog_error)?
            .ok_or(FileS3ErrorCode::InternalError)?;
        let policy = self
            .admission
            .load(session.context)
            .await
            .map_err(catalog_error)?
            .ok_or(FileS3ErrorCode::InternalError)?;
        if !self
            .admission
            .release(&policy, &terminal)
            .await
            .map_err(catalog_error)?
        {
            return Err(FileS3ErrorCode::SlowDown);
        }
        Ok(MultipartResponses::abort().map(IcebergBody::new))
    }

    async fn complete(
        self: &Arc<Self>,
        mut session: MultipartSession,
        request: Request<Incoming>,
        now_ms: u64,
    ) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        let host = request
            .headers()
            .get(hyper::header::HOST)
            .and_then(|value| value.to_str().ok())
            .filter(|host| !host.is_empty() && host.len() <= 256 && !host.contains('/'))
            .ok_or(FileS3ErrorCode::InvalidRequest)?
            .to_owned();
        let url = format!("http://{host}{}", request.uri().path());
        let signed = signed_digest(request.headers().get("x-amz-content-sha256"))?;
        let bytes = read_complete_body(request.into_body()).await?;
        if signed.is_some_and(|digest| digest != <[u8; 32]>::from(Sha256::digest(&bytes))) {
            return Err(FileS3ErrorCode::BadDigest);
        }
        let requested =
            crate::iceberg::CompleteSelection::parse(&bytes).map_err(|_| FileS3ErrorCode::InvalidPart)?;
        let selection = requested
            .resolve(&self.multipart, &session)
            .await
            .map_err(|error| match error {
                crate::iceberg::CompleteResolveError::InvalidPart => FileS3ErrorCode::InvalidPart,
                crate::iceberg::CompleteResolveError::EntityTooSmall => FileS3ErrorCode::EntityTooSmall,
                crate::iceberg::CompleteResolveError::Catalog(error) => catalog_error(error),
            })?;
        if session.phase == MultipartPhase::Open {
            if !self
                .multipart
                .freeze_completion(&session, &selection, now_ms)
                .await
                .map_err(catalog_error)?
            {
                return Err(FileS3ErrorCode::SlowDown);
            }
            session = self.current(&session).await?;
        }
        let expected: [u8; 32] = Sha256::digest(selection.encode()).into();
        let service = Arc::clone(self);
        let resource = session.location.object_key();
        let body = crate::iceberg::FileCompleteBody::new(
            async move {
                service
                    .drive_complete(session, expected, now_ms, &url)
                    .await
                    .map(Response::into_body)
            },
            &resource,
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(300),
        )
        .map_err(|_| FileS3ErrorCode::InternalError)?;
        let mut response = Response::new(IcebergBody::complete(body));
        response.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        );
        Ok(response)
    }

    async fn drive_complete(
        &self,
        mut session: MultipartSession,
        expected: [u8; 32],
        now_ms: u64,
        url: &str,
    ) -> Result<Response<Vec<u8>>, FileS3ErrorCode> {
        if session
            .completion
            .as_ref()
            .map_or(true, |completion| completion.selection.digest != expected)
        {
            return Err(FileS3ErrorCode::InvalidPart);
        }
        loop {
            match session.phase {
                MultipartPhase::Completing => {
                    let completion = session
                        .completion
                        .as_ref()
                        .ok_or(FileS3ErrorCode::InternalError)?;
                    if completion.progress.next_part < completion.selected_parts {
                        self.multipart
                            .advance_completion(
                                &session,
                                self.blocks.clone(),
                                crate::iceberg::file_admission::MULTIPART_COPY_BYTES,
                                crowdb_access_iceberg::file::NATIVE_FILE_BLOCK_BYTES,
                            )
                            .await
                            .map_err(work_error)?;
                    } else {
                        let tree = self
                            .multipart
                            .assembled_tree(
                                &session,
                                self.blocks.clone(),
                                crowdb_access_iceberg::file::NATIVE_FILE_BLOCK_BYTES,
                            )
                            .await
                            .map_err(work_error)?;
                        let sealed = FileSealer::new(self.blocks.clone(), self.limits.max_file_bytes)
                            .map_err(seal_error)?
                            .seal_uploaded(session.owner, session.location.clone(), tree.clone())
                            .await
                            .map_err(seal_error)?;
                        self.multipart
                            .prepare_publication(&session, &tree, &sealed, now_ms)
                            .await
                            .map_err(catalog_error)?;
                    }
                }
                MultipartPhase::Publishing | MultipartPhase::Published => {
                    let Some(record) = self.multipart.publish(&session).await.map_err(catalog_error)? else {
                        session = self.current(&session).await?;
                        continue;
                    };
                    session = self.current(&session).await?;
                    self.release_terminal(&session).await;
                    return MultipartResponses::complete(&session, &record, url)
                        .map_err(|_| FileS3ErrorCode::InternalError);
                }
                _ => return Err(FileS3ErrorCode::Conflict),
            }
            session = self.current(&session).await?;
        }
    }

    async fn current(&self, session: &MultipartSession) -> Result<MultipartSession, FileS3ErrorCode> {
        self.multipart
            .load(session.context, session.upload)
            .await
            .map_err(catalog_error)?
            .ok_or(FileS3ErrorCode::NoSuchUpload)
    }

    async fn release_terminal(&self, session: &MultipartSession) {
        if session.credit.is_some_and(|credit| credit.released) {
            return;
        }
        let result = async {
            let policy = self
                .admission
                .load(session.context)
                .await?
                .ok_or(CatalogError::Uninitialized)?;
            self.admission.release(&policy, session).await
        }
        .await;
        match result {
            Ok(true) => {}
            Ok(false) => {
                tracing::debug!(upload = %session.upload, "terminal credit release deferred to recovery");
            }
            Err(error) => {
                tracing::warn!(upload = %session.upload, %error, "terminal credit release deferred to recovery");
            }
        }
    }
}

async fn read_complete_body(mut body: Incoming) -> Result<Vec<u8>, FileS3ErrorCode> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let data = frame
            .map_err(|_| FileS3ErrorCode::InvalidRequest)?
            .into_data()
            .map_err(|_| FileS3ErrorCode::InvalidRequest)?;
        if bytes
            .len()
            .checked_add(data.len())
            .map_or(true, |length| length > 2 * 1024 * 1024)
        {
            return Err(FileS3ErrorCode::EntityTooLarge);
        }
        bytes.extend_from_slice(&data);
    }
    Ok(bytes)
}

fn work_error(error: MultipartWorkError) -> FileS3ErrorCode {
    match error {
        MultipartWorkError::Catalog(error) => catalog_error(error),
        MultipartWorkError::File(_) | MultipartWorkError::Invalid(_) => FileS3ErrorCode::InternalError,
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn seal_error(error: FileSealError) -> FileS3ErrorCode {
    match error {
        FileSealError::Bounds => FileS3ErrorCode::EntityTooLarge,
        FileSealError::Storage(_) => FileS3ErrorCode::InternalError,
        FileSealError::Invalid(_)
        | FileSealError::Json(_)
        | FileSealError::Avro(_)
        | FileSealError::Format(_)
        | FileSealError::Puffin(_) => FileS3ErrorCode::InvalidRequest,
    }
}

pub(super) fn encoding_error(error: FileEncodingError) -> FileS3ErrorCode {
    match error {
        FileEncodingError::Length => FileS3ErrorCode::EntityTooLarge,
        FileEncodingError::Checksum => FileS3ErrorCode::BadDigest,
        FileEncodingError::Signature => FileS3ErrorCode::AccessDenied,
        FileEncodingError::Framing | FileEncodingError::Transport => FileS3ErrorCode::InvalidRequest,
    }
}

pub(super) fn signed_digest(value: Option<&HeaderValue>) -> Result<Option<[u8; 32]>, FileS3ErrorCode> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| FileS3ErrorCode::InvalidRequest)?;
    if value == "UNSIGNED-PAYLOAD" {
        return Ok(None);
    }
    if value.len() != 64 {
        return Err(FileS3ErrorCode::InvalidRequest);
    }
    let mut digest = [0; 32];
    for (target, pair) in digest.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let hex = |byte| match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        };
        *target = (hex(pair[0]).ok_or(FileS3ErrorCode::InvalidRequest)? << 4)
            | hex(pair[1]).ok_or(FileS3ErrorCode::InvalidRequest)?;
    }
    Ok(Some(digest))
}

pub(super) async fn verify_md5(
    blocks: Arc<dyn FileBlockStore>,
    owner: FileIdentity,
    tree: FileTree,
    header: Option<&HeaderValue>,
) -> Result<(), FileS3ErrorCode> {
    let Some(header) = header else {
        return Ok(());
    };
    let decoded = STANDARD
        .decode(header.as_bytes())
        .map_err(|_| FileS3ErrorCode::InvalidRequest)?;
    let expected: [u8; 16] = decoded.try_into().map_err(|_| FileS3ErrorCode::InvalidRequest)?;
    let mut reader = FileReader::from_tree(blocks, owner, tree, None, 64 * 1024)
        .map_err(|_| FileS3ErrorCode::InternalError)?;
    let mut digest = Md5::new();
    while let Some(bytes) = reader.next().await.map_err(|_| FileS3ErrorCode::InternalError)? {
        digest.update(&bytes);
    }
    if <[u8; 16]>::from(digest.finalize()) != expected {
        return Err(FileS3ErrorCode::BadDigest);
    }
    Ok(())
}
