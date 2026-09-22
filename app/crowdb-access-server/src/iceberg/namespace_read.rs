use std::sync::{atomic::AtomicUsize, Arc};

use crowdb_access_iceberg::catalog::{CatalogContext, CatalogError};
use crowdb_access_iceberg::namespace::{
    NamespaceIdentifier, NamespaceLister, NamespaceRepository, NamespaceStore,
};
use crowdb_access_iceberg::wire::IcebergErrorResponse;
use hyper::{Method, Response, Uri};

use super::body::{IcebergBody, SpoolPermit};
use super::http::{bad_request, decode_query, response, service_unavailable};

pub(super) struct NamespaceHttp {
    repository: NamespaceRepository,
    lister: NamespaceLister,
    spools: Arc<AtomicUsize>,
}

impl NamespaceHttp {
    pub(super) fn new<Store: NamespaceStore + 'static>(
        store: Arc<Store>,
        secret: &[u8; 32],
    ) -> Result<Self, crowdb_access_iceberg::error::ValidationError> {
        Ok(Self {
            repository: NamespaceRepository::new(store.clone()),
            lister: NamespaceLister::new(store, secret)?,
            spools: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub(super) async fn read(
        &self,
        context: CatalogContext,
        method: &Method,
        uri: &Uri,
    ) -> Result<Response<IcebergBody>, IcebergErrorResponse> {
        if method == Method::GET && uri.path() == "/v1/namespaces" {
            return self.list(context, uri.query()).await;
        }
        if method != Method::GET && method != Method::HEAD {
            return Err(unsupported());
        }
        let encoded = uri
            .path()
            .strip_prefix("/v1/namespaces/")
            .ok_or_else(unsupported)?;
        if encoded.is_empty() || encoded.contains('/') || uri.query().is_some() {
            return Err(bad_request());
        }
        let identifier = NamespaceIdentifier::from_rest(&decode_path(encoded)?).map_err(|_| bad_request())?;
        let authority = self
            .repository
            .load(context, &identifier)
            .await
            .map_err(|error| storage_error(&error))?
            .ok_or_else(not_found)?;
        let bytes = if method == Method::HEAD {
            Vec::new()
        } else {
            serde_json::to_vec(&serde_json::json!({"namespace": authority.identifier.components(), "properties": authority.properties.entries()})).map_err(|_| service_unavailable())?
        };
        Ok(response(if method == Method::HEAD { 204 } else { 200 }, bytes))
    }

    async fn list(
        &self,
        context: CatalogContext,
        query: Option<&str>,
    ) -> Result<Response<IcebergBody>, IcebergErrorResponse> {
        let (parent, limit, token) = parameters(query)?;
        if let Some(token) = token {
            let page = self
                .lister
                .page(context, parent.as_ref(), limit, &token)
                .await
                .map_err(|error| storage_error(&error))?
                .ok_or_else(not_found)?;
            let namespaces: Vec<_> = page
                .namespaces
                .iter()
                .map(NamespaceIdentifier::components)
                .collect();
            let bytes = serde_json::to_vec(
                &serde_json::json!({"namespaces": namespaces, "next-page-token": page.next_page_token}),
            )
            .map_err(|_| service_unavailable())?;
            if bytes.len() > 2 * 1024 * 1024 {
                return Err(service_unavailable());
            }
            return Ok(response(200, bytes));
        }
        let permit = SpoolPermit::acquire(&self.spools).ok_or_else(service_unavailable)?;
        let mut bytes = b"{\"namespaces\":[".to_vec();
        let mut token = String::new();
        let mut items = 0;
        let mut scanned = 0;
        loop {
            let page = self
                .lister
                .page(context, parent.as_ref(), 100, &token)
                .await
                .map_err(|error| storage_error(&error))?
                .ok_or_else(not_found)?;
            scanned += page.scanned;
            if scanned > 4096 {
                return Err(service_unavailable());
            }
            for identifier in page.namespaces {
                let encoded =
                    serde_json::to_vec(identifier.components()).map_err(|_| service_unavailable())?;
                items += 1;
                if items > 1024 || bytes.len() + encoded.len() + 64 > 2 * 1024 * 1024 {
                    return Err(service_unavailable());
                }
                if items > 1 {
                    bytes.push(b',');
                }
                bytes.extend_from_slice(&encoded);
            }
            match page.next_page_token {
                Some(next) => token = next,
                None => break,
            }
        }
        bytes.extend_from_slice(b"],\"next-page-token\":null}");
        let mut result = response(200, Vec::new());
        *result.body_mut() = IcebergBody::with_permit(bytes, permit);
        Ok(result)
    }
}

fn parameters(
    query: Option<&str>,
) -> Result<(Option<NamespaceIdentifier>, usize, Option<String>), IcebergErrorResponse> {
    let mut values = std::collections::BTreeMap::new();
    for pair in query
        .unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
    {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let name = decode_query(name)?;
        if !matches!(name.as_str(), "parent" | "pageSize" | "pageToken")
            || values.insert(name, decode_query(value)?).is_some()
        {
            return Err(bad_request());
        }
    }
    let parent = values
        .remove("parent")
        .filter(|value| !value.is_empty())
        .map(|value| NamespaceIdentifier::from_rest(&value))
        .transpose()
        .map_err(|_| bad_request())?;
    let limit = values
        .remove("pageSize")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| bad_request())?
        .unwrap_or(100);
    if limit == 0 || limit > 100 {
        return Err(bad_request());
    }
    Ok((parent, limit, values.remove("pageToken")))
}

fn decode_path(value: &str) -> Result<String, IcebergErrorResponse> {
    decode_query(&value.replace('+', "%2B"))
}

fn not_found() -> IcebergErrorResponse {
    IcebergErrorResponse::new(404, "NoSuchNamespaceException", "Namespace does not exist")
}
fn unsupported() -> IcebergErrorResponse {
    IcebergErrorResponse::new(
        406,
        "UnsupportedOperationException",
        "This endpoint is not implemented",
    )
}
fn storage_error(error: &CatalogError) -> IcebergErrorResponse {
    match error {
        CatalogError::Invalid(
            crowdb_access_iceberg::error::ValidationError::Key
            | crowdb_access_iceberg::error::ValidationError::KeyTooLarge
            | crowdb_access_iceberg::error::ValidationError::IdentityMismatch
            | crowdb_access_iceberg::error::ValidationError::Text,
        ) => bad_request(),
        _ => service_unavailable(),
    }
}
