use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::catalog::{CatalogLifecycle, CatalogRepository, RootState};
use crowdb_access_iceberg::file::{FileLocation, FileOperation, FileReader, TableLocation};
use crowdb_access_iceberg::gc::{
    avro_links, metadata_links, AvroMarkCursor, AvroMarkLimits, GcLimits, ReachableFile, ReachableKind,
};
use crowdb_access_iceberg::record::StorageRecord;
use crowdb_access_iceberg::table::head_key;
use crowdb_access_s3::auth::RawAuthRequest;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use md5::{Digest, Md5};
use serde::Deserialize;
use sha2::Sha256;
use std::time::Duration;

use super::{catalog_error, now_ms, FileHttp, FileS3ErrorCode};
use crate::iceberg::body::IcebergBody;
use crate::iceberg::file_auth::authenticate_file_transfer;

#[derive(Deserialize)]
#[serde(rename = "Delete")]
struct DeleteRequest {
    #[serde(rename = "Object")]
    objects: Vec<DeleteEntry>,
    #[serde(rename = "Quiet", default)]
    quiet: bool,
}

#[derive(Deserialize)]
struct DeleteEntry {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId")]
    version_id: Option<String>,
}

impl FileHttp {
    #[allow(clippy::too_many_lines)]
    pub(super) async fn delete_objects(
        &self,
        catalog: &CatalogRepository,
        request: Request<Incoming>,
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
        let now = now_ms()?;
        let (grant, streaming) = authenticate_file_transfer(
            &self.issuer,
            root.context,
            RawAuthRequest::from_parts(request.method(), request.uri(), request.headers()),
            &self.region,
            now,
        )
        .map_err(|_| FileS3ErrorCode::AccessDenied)?;
        if streaming.is_some() || !grant.operations.allows(FileOperation::DeleteObjects) {
            return Err(FileS3ErrorCode::AccessDenied);
        }
        let bucket = TableLocation {
            catalog: root.context.catalog,
            table: grant.table,
        }
        .bucket();
        if request.uri().path() != format!("/{bucket}") {
            return Err(FileS3ErrorCode::AccessDenied);
        }
        let expected_md5 = request
            .headers()
            .get("content-md5")
            .and_then(|value| value.to_str().ok())
            .ok_or(FileS3ErrorCode::InvalidRequest)?
            .to_owned();
        let signed_digest = request
            .headers()
            .get("x-amz-content-sha256")
            .and_then(|value| value.to_str().ok())
            .ok_or(FileS3ErrorCode::InvalidRequest)?
            .to_owned();
        let mut incoming = request.into_body();
        let mut body = Vec::new();
        while let Some(frame) = incoming.frame().await {
            let frame = frame.map_err(|_| FileS3ErrorCode::InvalidRequest)?;
            if let Ok(bytes) = frame.into_data() {
                if body.len().saturating_add(bytes.len()) > 2 * 1024 * 1024 {
                    return Err(FileS3ErrorCode::EntityTooLarge);
                }
                body.extend_from_slice(&bytes);
            }
        }
        if expected_md5 != STANDARD.encode(Md5::digest(&body)) {
            return Err(FileS3ErrorCode::BadDigest);
        }
        if signed_digest != "UNSIGNED-PAYLOAD" && signed_digest != format!("{:x}", Sha256::digest(&body)) {
            return Err(FileS3ErrorCode::BadDigest);
        }
        let parsed: DeleteRequest =
            quick_xml::de::from_reader(body.as_slice()).map_err(|_| FileS3ErrorCode::InvalidRequest)?;
        if parsed.objects.is_empty() || parsed.objects.len() > 1000 {
            return Err(FileS3ErrorCode::InvalidRequest);
        }
        let table = TableLocation {
            catalog: root.context.catalog,
            table: grant.table,
        };
        let references = self.retained_references(root.context, table).await;
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        for entry in parsed.objects {
            let key = quick_xml::escape::escape(&entry.key);
            let location = FileLocation::from_object_key(&bucket, &entry.key)
                .ok()
                .filter(|location| location.table() == table);
            let result = if entry.version_id.is_some() {
                Err(("InvalidArgument", "Versioned deletion is not supported"))
            } else if let Some(location) = location {
                match self
                    .delete_location(root.context, &location, now, references.as_ref().ok())
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(FileS3ErrorCode::Conflict) => {
                        Err(("AccessDenied", "A retained snapshot references this object"))
                    }
                    Err(_) => Err(("ServiceUnavailable", "Object deletion is uncertain")),
                }
            } else {
                Err(("AccessDenied", "Object is outside the cleanup credential scope"))
            };
            match result {
                Ok(()) if !parsed.quiet => {
                    xml.push_str("<Deleted><Key>");
                    xml.push_str(&key);
                    xml.push_str("</Key></Deleted>");
                }
                Ok(()) => {}
                Err((code, message)) => {
                    xml.push_str("<Error><Key>");
                    xml.push_str(&key);
                    xml.push_str("</Key><Code>");
                    xml.push_str(code);
                    xml.push_str("</Code><Message>");
                    xml.push_str(message);
                    xml.push_str("</Message></Error>");
                }
            }
        }
        xml.push_str("</DeleteResult>");
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/xml")
            .body(IcebergBody::new(xml.into_bytes()))
            .map_err(|_| FileS3ErrorCode::InternalError)
    }

    pub(super) async fn delete_object(
        &self,
        location: &FileLocation,
        context: CatalogContext,
        now_ms: u64,
    ) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        let references = self.retained_references(context, location.table()).await?;
        self.delete_location(context, location, now_ms, Some(&references))
            .await?;
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(IcebergBody::new(Vec::new()))
            .map_err(|_| FileS3ErrorCode::InternalError)
    }

    async fn delete_location(
        &self,
        context: CatalogContext,
        location: &FileLocation,
        now_ms: u64,
        references: Option<&HashSet<FileLocation>>,
    ) -> Result<(), FileS3ErrorCode> {
        if let Some(file) = self
            .repository
            .load(context, location)
            .await
            .map_err(catalog_error)?
        {
            let references = references.ok_or(FileS3ErrorCode::SlowDown)?;
            if references.contains(location) {
                return Err(FileS3ErrorCode::Conflict);
            }
            let deleted = self
                .repository
                .mark_deleted(context, &file, now_ms)
                .await
                .map_err(catalog_error)?;
            self.gc
                .claim_deleted_file(context, &deleted, now_ms, GcLimits::default())
                .await
                .map_err(catalog_error)?;
        } else if let Some(deleted) = self
            .repository
            .deleted(context, location)
            .await
            .map_err(catalog_error)?
        {
            self.gc
                .claim_deleted_file(context, &deleted, now_ms, GcLimits::default())
                .await
                .map_err(catalog_error)?;
        }
        Ok(())
    }

    pub(super) async fn retained_references(
        &self,
        context: CatalogContext,
        table: TableLocation,
    ) -> Result<HashSet<FileLocation>, FileS3ErrorCode> {
        let key = head_key(context.catalog, table.table);
        let Some(value) = self
            .store
            .get(&key.encode().map_err(|_| FileS3ErrorCode::InternalError)?)
            .await
            .map_err(|_| FileS3ErrorCode::SlowDown)?
        else {
            return Ok(HashSet::new());
        };
        let StorageRecord::TableHead(head) =
            StorageRecord::decode(&key, &value.bytes).map_err(|_| FileS3ErrorCode::SlowDown)?
        else {
            return Err(FileS3ErrorCode::SlowDown);
        };
        if head.table != table.table || head.catalog != context.catalog {
            return Err(FileS3ErrorCode::SlowDown);
        }
        let mut queue = VecDeque::from([ReachableFile {
            location: head.metadata_location.clone(),
            kind: ReachableKind::Metadata,
        }]);
        let mut seen = HashSet::new();
        let limits = crate::iceberg::table_limits::commits();
        while let Some(link) = queue.pop_front() {
            if !seen.insert(link.location.clone()) {
                continue;
            }
            if seen.len() > 100_000 || queue.len() > 100_000 {
                return Err(FileS3ErrorCode::SlowDown);
            }
            if link.kind == ReachableKind::File {
                continue;
            }
            let record = self
                .repository
                .load(context, &link.location)
                .await
                .map_err(|_| FileS3ErrorCode::SlowDown)?
                .ok_or(FileS3ErrorCode::SlowDown)?;
            if link.kind == ReachableKind::Metadata {
                let mut reader = FileReader::new(self.blocks.clone(), record, None, 64 * 1024)
                    .map_err(|_| FileS3ErrorCode::SlowDown)?;
                let mut bytes = Vec::new();
                while let Some(frame) = reader.next().await.map_err(|_| FileS3ErrorCode::SlowDown)? {
                    if bytes.len().saturating_add(frame.len()) > limits.preparation.evaluation.metadata.bytes
                    {
                        return Err(FileS3ErrorCode::SlowDown);
                    }
                    bytes.extend(frame);
                }
                queue.extend(
                    metadata_links(&bytes, &link.location, limits.preparation.evaluation.metadata)
                        .map_err(|_| FileS3ErrorCode::SlowDown)?,
                );
            } else {
                let mut cursor = AvroMarkCursor::default();
                let mut complete = false;
                for _ in 0..100_000 {
                    let page = avro_links(
                        Arc::clone(&self.blocks),
                        &record,
                        link.kind,
                        &cursor,
                        AvroMarkLimits {
                            framing: limits.prior.manifests.framing,
                            datum: limits.prior.manifests.datum,
                            decoded_bytes: limits.prior.manifests.decoded_bytes,
                            page_items: 128,
                        },
                    )
                    .await
                    .map_err(|_| FileS3ErrorCode::SlowDown)?;
                    queue.extend(page.links);
                    if page.complete {
                        complete = true;
                        break;
                    }
                    if page.next == cursor {
                        return Err(FileS3ErrorCode::SlowDown);
                    }
                    cursor = page.next;
                }
                if !complete {
                    return Err(FileS3ErrorCode::SlowDown);
                }
            }
        }
        Ok(seen)
    }
}
