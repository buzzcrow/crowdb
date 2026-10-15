use std::time::Duration;

use crowdb_access_iceberg::catalog::{CatalogLifecycle, CatalogRepository, RootState};
use crowdb_access_iceberg::file::{FileOperation, TableLocation};
use crowdb_access_s3::auth::RawAuthRequest;
use hyper::header::CONTENT_LENGTH;
use hyper::{body::Incoming, Request, Response};

use super::{
    authenticate_file_transfer, catalog_error, now_ms, set_header, FileHttp, FileS3ErrorCode, IcebergBody,
};
use crate::iceberg::FileBucketRequest;

impl FileHttp {
    pub(super) async fn bucket_probe(
        &self,
        catalog: &CatalogRepository,
        request: &Request<Incoming>,
        probe: &FileBucketRequest,
        request_timeout: Duration,
    ) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        let (root, authority) = catalog.status().await.map_err(catalog_error)?;
        if root.state != RootState::Ready
            || authority.lifecycle != CatalogLifecycle::Ready
            || request_timeout.is_zero()
            || request_timeout > Duration::from_millis(authority.admission_bounds.request_ms)
        {
            return Err(FileS3ErrorCode::SlowDown);
        }
        let (grant, streaming) = authenticate_file_transfer(
            &self.issuer,
            root.context,
            RawAuthRequest::from_parts(request.method(), request.uri(), request.headers()),
            &self.region,
            now_ms()?,
        )
        .map_err(|_| FileS3ErrorCode::AccessDenied)?;
        if streaming.is_some() || !grant.operations.allows(FileOperation::Head) {
            return Err(FileS3ErrorCode::AccessDenied);
        }
        let table = TableLocation {
            catalog: grant.context.catalog,
            table: grant.table,
            storage_namespace: grant.storage_namespace,
        };
        let (bucket, location) = match probe {
            FileBucketRequest::Head(bucket) => (bucket, false),
            FileBucketRequest::Location(bucket) => (bucket, true),
        };
        if *bucket != table.bucket() {
            return Err(FileS3ErrorCode::AccessDenied);
        }
        let bytes = if location {
            format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>", self.region).into_bytes()
        } else {
            Vec::new()
        };
        let length = bytes.len();
        let mut response = Response::new(IcebergBody::new(bytes));
        set_header(&mut response, CONTENT_LENGTH, &length.to_string())?;
        response.headers_mut().insert(
            "x-amz-bucket-region",
            hyper::header::HeaderValue::from_str(&self.region).map_err(|_| FileS3ErrorCode::InternalError)?,
        );
        Ok(response)
    }
}
