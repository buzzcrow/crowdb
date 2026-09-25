use std::collections::BTreeMap;
use std::sync::{atomic::AtomicUsize, Arc};

use crowdb_access_iceberg::{
    catalog::{CatalogContext, CatalogError},
    error::ValidationError,
    key::NameSuffix,
    namespace::NamespaceIdentifier,
    table::{SnapshotLoadingMode, TableListLimits, TableLister, TableLoad, TableLoader},
    wire::IcebergErrorResponse,
};
use crowdb_access_iceberg::{file::FileBlockStore, namespace::NamespaceStore, table::TableMetadataLimits};
use hyper::{header, Method, Request, Response};
use sha2::{Digest, Sha256};

use super::{
    body::{IcebergBody, SpoolPermit},
    http::{bad_request, decode_query, response, service_unavailable},
    namespace_read::decode_path,
};

const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

pub(super) struct TableHttp {
    loader: TableLoader,
    lister: TableLister,
    spools: Arc<AtomicUsize>,
    pub(super) file_config: Option<super::table_credentials::TableFileConfig>,
}

impl TableHttp {
    pub(super) fn new<Store: NamespaceStore + 'static>(
        store: Arc<Store>,
        blocks: Arc<dyn FileBlockStore>,
        secret: &[u8; 32],
    ) -> Result<Self, ValidationError> {
        Ok(Self {
            loader: TableLoader::new(
                store.clone(),
                blocks,
                TableMetadataLimits {
                    bytes: 2 * 1024 * 1024,
                    values: 200_000,
                    depth: 64,
                    string_bytes: 1024 * 1024,
                    collection_entries: 10_000,
                },
            ),
            lister: TableLister::new(store, secret)?,
            spools: Arc::new(AtomicUsize::new(0)),
            file_config: None,
        })
    }

    pub(super) async fn read(
        &self,
        context: CatalogContext,
        request: &Request<hyper::body::Incoming>,
    ) -> Result<Response<IcebergBody>, IcebergErrorResponse> {
        if request.method() != Method::GET && request.method() != Method::HEAD {
            return Err(unsupported());
        }
        let suffix = request
            .uri()
            .path()
            .strip_prefix("/v1/namespaces/")
            .ok_or_else(bad_request)?;
        let mut parts = suffix.split('/');
        let namespace = NamespaceIdentifier::from_rest(&decode_path(parts.next().ok_or_else(bad_request)?)?)
            .map_err(|_| bad_request())?;
        if parts.next() != Some("tables") {
            return Err(bad_request());
        }
        let name = parts.next().map(decode_path).transpose()?;
        if parts.next().is_some() {
            return Err(unsupported());
        }
        let mut parameters = parameters(request.uri().query())?;
        let permit = SpoolPermit::acquire(&self.spools).ok_or_else(service_unavailable)?;
        let mut result = if let Some(name) = name {
            NameSuffix {
                parent: None,
                name: &name,
            }
            .encode()
            .map_err(|_| bad_request())?;
            if request.method() == Method::HEAD {
                if !parameters.is_empty() {
                    return Err(bad_request());
                }
                if !self
                    .loader
                    .exists(context, &namespace, &name)
                    .await
                    .map_err(|_| service_unavailable())?
                {
                    return Err(missing_table());
                }
                response(204, Vec::new())
            } else {
                self.load(context, &namespace, &name, &mut parameters, request.headers())
                    .await?
            }
        } else {
            if request.method() != Method::GET {
                return Err(unsupported());
            }
            self.list(context, &namespace, &mut parameters).await?
        };
        let body = std::mem::replace(result.body_mut(), IcebergBody::new(Vec::new()));
        *result.body_mut() = body.with_spool_permit(permit);
        Ok(result)
    }

    async fn load(
        &self,
        context: CatalogContext,
        namespace: &NamespaceIdentifier,
        name: &str,
        parameters: &mut BTreeMap<String, String>,
        headers: &hyper::HeaderMap,
    ) -> Result<Response<IcebergBody>, IcebergErrorResponse> {
        let mode = match parameters.remove("snapshots").as_deref() {
            None | Some("all") => SnapshotLoadingMode::All,
            Some("refs") => SnapshotLoadingMode::Refs,
            _ => return Err(bad_request()),
        };
        if !parameters.is_empty() {
            return Err(bad_request());
        }
        let condition = condition(headers)?;
        let loaded = self
            .loader
            .load(
                context,
                namespace,
                name,
                mode,
                if self.file_config.is_some() {
                    None
                } else {
                    condition.as_deref()
                },
            )
            .await
            .map_err(|_| service_unavailable())?;
        let (mut result, etag) = match loaded {
            TableLoad::Missing => return Err(missing_table()),
            TableLoad::NotModified { etag } => (response(304, Vec::new()), etag),
            TableLoad::Loaded { head, etag, metadata } => {
                super::metrics::record_selected_version(head.format_version);
                let location = serde_json::to_vec(&head.metadata_location.to_string())
                    .map_err(|_| service_unavailable())?;
                let mut bytes = b"{\"metadata-location\":".to_vec();
                append(&mut bytes, &location)?;
                append(&mut bytes, b",\"metadata\":")?;
                append(&mut bytes, &metadata)?;
                append(&mut bytes, b"}")?;
                let etag = if let Some(config) = &self.file_config {
                    config.append(&mut bytes, namespace, name, head.table)?;
                    let mut digest = Sha256::new();
                    digest.update(etag.as_bytes());
                    digest.update(&bytes);
                    format!("\"{:x}\"", digest.finalize())
                } else {
                    etag
                };
                let unchanged = condition.as_deref().is_some_and(|header| {
                    header.split(',').any(|tag| {
                        let tag = tag.trim();
                        tag == "*" || tag.strip_prefix("W/").unwrap_or(tag) == etag
                    })
                });
                (
                    if unchanged {
                        response(304, Vec::new())
                    } else {
                        response(200, bytes)
                    },
                    etag,
                )
            }
        };
        result
            .headers_mut()
            .insert(header::ETAG, etag.parse().map_err(|_| service_unavailable())?);
        Ok(result)
    }

    async fn list(
        &self,
        context: CatalogContext,
        namespace: &NamespaceIdentifier,
        parameters: &mut BTreeMap<String, String>,
    ) -> Result<Response<IcebergBody>, IcebergErrorResponse> {
        let page_size = parameters
            .remove("pageSize")
            .map(|value| value.parse::<usize>())
            .transpose()
            .map_err(|_| bad_request())?
            .unwrap_or(100);
        let token = parameters.remove("pageToken");
        if !parameters.is_empty() || !(1..=100).contains(&page_size) {
            return Err(bad_request());
        }
        let page = self
            .lister
            .list(
                context,
                namespace,
                TableListLimits {
                    page_size,
                    scanned: 4096,
                    names_bytes: 256 * 1024,
                },
                token.as_deref(),
            )
            .await
            .map_err(|error| list_error(&error))?
            .ok_or_else(|| {
                IcebergErrorResponse::new(404, "NoSuchNamespaceException", "Namespace does not exist")
            })?;
        let mut bytes = b"{\"identifiers\":[".to_vec();
        for (index, name) in page.names.iter().enumerate() {
            if index > 0 {
                append(&mut bytes, b",")?;
            }
            let identifier =
                serde_json::to_vec(&serde_json::json!({"namespace":namespace.components(),"name":name}))
                    .map_err(|_| service_unavailable())?;
            append(&mut bytes, &identifier)?;
        }
        append(&mut bytes, b"],\"next-page-token\":")?;
        append(
            &mut bytes,
            &serde_json::to_vec(&page.next_page_token).map_err(|_| service_unavailable())?,
        )?;
        append(&mut bytes, b"}")?;
        Ok(response(200, bytes))
    }
}

fn parameters(query: Option<&str>) -> Result<BTreeMap<String, String>, IcebergErrorResponse> {
    let mut parameters = BTreeMap::new();
    for pair in query
        .unwrap_or_default()
        .split('&')
        .filter(|value| !value.is_empty())
    {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        if parameters
            .insert(decode_query(name)?, decode_query(value)?)
            .is_some()
        {
            return Err(bad_request());
        }
    }
    Ok(parameters)
}

fn condition(headers: &hyper::HeaderMap) -> Result<Option<String>, IcebergErrorResponse> {
    let mut result = String::new();
    for value in headers.get_all(header::IF_NONE_MATCH) {
        let value = value.to_str().map_err(|_| bad_request())?;
        if result.len() + value.len() + 1 > 8192 {
            return Err(bad_request());
        }
        if !result.is_empty() {
            result.push(',');
        }
        result.push_str(value);
    }
    Ok((!result.is_empty()).then_some(result))
}

fn append(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), IcebergErrorResponse> {
    if value.len() > MAX_RESPONSE_BYTES.saturating_sub(bytes.len()) {
        return Err(service_unavailable());
    }
    bytes.extend_from_slice(value);
    Ok(())
}

pub(super) fn missing_table() -> IcebergErrorResponse {
    IcebergErrorResponse::new(404, "NoSuchTableException", "Table does not exist")
}

pub(super) fn unsupported() -> IcebergErrorResponse {
    IcebergErrorResponse::new(
        406,
        "UnsupportedOperationException",
        "This table endpoint is not enabled",
    )
}

fn list_error(error: &CatalogError) -> IcebergErrorResponse {
    match error {
        CatalogError::Invalid(
            ValidationError::Key | ValidationError::KeyTooLarge | ValidationError::IdentityMismatch,
        ) => bad_request(),
        _ => service_unavailable(),
    }
}
