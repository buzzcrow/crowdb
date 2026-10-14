use crowdb_access_iceberg::{
    file::{FileIdentity, NativeInjectionRepository, NativeInjectionRequest},
    key::FileId,
};
use hyper::{
    body::Incoming,
    header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG},
    Request, Response,
};

use super::{
    admission_error, catalog_error, etag, multipart, now_ms, set_header, FileHttp, FileRequest,
    FileS3ErrorCode, FileTransferAdmission, IcebergBody, UploadBody,
};

impl FileHttp {
    pub(super) async fn inject(
        &self,
        file_request: &FileRequest,
        request: Request<Incoming>,
        admission: &FileTransferAdmission,
        current_ms: u64,
        locations: &crowdb_access_iceberg::file::DirectFileLocations,
    ) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        let digest = multipart::signed_digest(request.headers().get("x-amz-content-sha256"))?
            .ok_or(FileS3ErrorCode::InvalidRequest)?;
        let stem = file_request
            .location
            .relative_key()
            .rsplit('/')
            .next()
            .and_then(|filename| filename.strip_suffix(".parquet"))
            .ok_or(FileS3ErrorCode::InvalidRequest)?
            .to_owned();
        let injection = NativeInjectionRequest {
            context: admission.context(),
            table: file_request.location.table(),
            identity: file_request.injection.ok_or(FileS3ErrorCode::InvalidRequest)?,
            principal: admission.principal(),
            stem,
            digest,
        };
        let repository = NativeInjectionRepository::new(self.store.clone());
        crowdb_access_iceberg::file::DirectFileLocations::validate_stem(&injection.stem)
            .map_err(|_| FileS3ErrorCode::InvalidRequest)?;
        let file = if let Some(file) = repository
            .replay(&injection, current_ms)
            .await
            .map_err(catalog_error)?
        {
            file
        } else {
            let (parts, incoming) = request.into_parts();
            let mut body = UploadBody::new(incoming, &parts.headers, None, admission.request_byte_limit())
                .map_err(multipart::encoding_error)?;
            let owner = FileIdentity {
                table: injection.table,
                file: FileId::random(),
            };
            let length = body.decoded_length();
            let tree = admission
                .receive(
                    &self.uploads,
                    &mut body,
                    self.blocks.clone(),
                    owner,
                    length,
                    Some(digest),
                )
                .await
                .map_err(|error| {
                    body.failure()
                        .map_or_else(|| admission_error(error), multipart::encoding_error)
                })?;
            crowdb_access_iceberg::file::FileSealer::new(self.blocks.clone(), self.limits.max_file_bytes)
                .map_err(|_| FileS3ErrorCode::InternalError)?
                .seal_uploaded(owner, file_request.location.clone(), tree.clone())
                .await
                .map_err(multipart::seal_error)?;
            repository
                .finish(&injection, owner, tree, locations, now_ms()?)
                .await
                .map_err(catalog_error)?
        };
        let bytes = serde_json::to_vec(&serde_json::json!({"location": file.location.to_string()}))
            .map_err(|_| FileS3ErrorCode::InternalError)?;
        let length = bytes.len();
        let mut response = Response::new(IcebergBody::new(bytes));
        set_header(&mut response, CONTENT_LENGTH, &length.to_string())?;
        set_header(&mut response, CONTENT_TYPE, "application/json")?;
        set_header(&mut response, ETAG, &etag(&file))?;
        Ok(response)
    }
}
