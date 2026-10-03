use crowdb_access_iceberg::catalog::{CatalogLifecycle, CatalogRepository, RootState};
use crowdb_access_iceberg::file::FileListRequest;
use crowdb_access_s3::auth::RawAuthRequest;
use hyper::{body::Incoming, Request, Response};
use std::time::Duration;

use super::{catalog_error, now_ms, FileHttp};
use crate::iceberg::body::IcebergBody;
use crate::iceberg::file_auth::authenticate_file_request;
use crate::iceberg::file_response::FileS3ErrorCode;

impl FileHttp {
    pub(super) async fn list_files(
        &self,
        catalog: &CatalogRepository,
        request: &Request<Incoming>,
        listing: &FileListRequest,
        timeout: Duration,
    ) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        let (root, authority) = catalog.status().await.map_err(catalog_error)?;
        if root.state != RootState::Ready
            || authority.lifecycle != CatalogLifecycle::Ready
            || timeout.is_zero()
            || timeout > Duration::from_millis(authority.admission_bounds.request_ms)
        {
            return Err(FileS3ErrorCode::SlowDown);
        }
        let now = now_ms()?;
        let grant = authenticate_file_request(
            &self.issuer,
            root.context,
            RawAuthRequest::from_parts(request.method(), request.uri(), request.headers()),
            &self.region,
            now,
        )
        .map_err(|_| FileS3ErrorCode::AccessDenied)?;
        if request.headers().contains_key("transfer-encoding")
            || request
                .headers()
                .get("content-length")
                .is_some_and(|value| value.as_bytes() != b"0")
        {
            return Err(FileS3ErrorCode::InvalidRequest);
        }
        let page = self
            .repository
            .list(&grant, listing, &self.list_tokens, now)
            .await
            .map_err(|error| {
                if matches!(error, crowdb_access_iceberg::catalog::CatalogError::Invalid(_)) {
                    FileS3ErrorCode::InvalidRequest
                } else {
                    catalog_error(error)
                }
            })?;
        let xml = crate::iceberg::file_list_response::list_files(listing, &page);
        if xml.len() > 2 * 1024 * 1024 {
            return Err(FileS3ErrorCode::InternalError);
        }
        Response::builder()
            .header("content-type", "application/xml")
            .header("content-length", xml.len())
            .body(IcebergBody::new(xml.into_bytes()))
            .map_err(|_| FileS3ErrorCode::InternalError)
    }
}
