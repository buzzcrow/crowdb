use std::collections::BTreeMap;

use crowdb_access_iceberg::namespace::{NamespaceIdentifier, NamespaceProperties, PropertyChanges};
use crowdb_access_iceberg::wire::IcebergErrorResponse;
use hyper::{Method, Uri};
use serde::Deserialize;

use super::http::bad_request;
use super::namespace_read::decode_path;

pub(super) enum NamespaceMutation {
    Create(NamespaceIdentifier, NamespaceProperties),
    Update(NamespaceIdentifier, PropertyChanges),
    Drop(NamespaceIdentifier),
}

#[derive(Deserialize)]
struct CreateBody {
    namespace: Vec<String>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct UpdateBody {
    #[serde(default)]
    removals: Vec<String>,
    #[serde(default)]
    updates: BTreeMap<String, String>,
}

pub(super) fn route(method: &Method, uri: &Uri) -> Option<&'static str> {
    if method == Method::POST && uri.path() == "/v1/namespaces" {
        return Some("POST /namespaces");
    }
    let tail = uri.path().strip_prefix("/v1/namespaces/")?;
    if method == Method::POST && tail.ends_with("/properties") {
        return Some("POST /namespaces/{namespace}/properties");
    }
    (method == Method::DELETE).then_some("DELETE /namespaces/{namespace}")
}

pub(super) fn parse(route: &str, uri: &Uri, bytes: &[u8]) -> Result<NamespaceMutation, IcebergErrorResponse> {
    if uri.query().is_some() {
        return Err(bad_request());
    }
    if route == "POST /namespaces" {
        let body: CreateBody = serde_json::from_slice(bytes).map_err(|_| bad_request())?;
        let identifier = NamespaceIdentifier::new(body.namespace).map_err(|_| bad_request())?;
        let properties = NamespaceProperties::new(body.properties).map_err(|_| bad_request())?;
        return Ok(NamespaceMutation::Create(identifier, properties));
    }
    let tail = uri
        .path()
        .strip_prefix("/v1/namespaces/")
        .ok_or_else(bad_request)?;
    let encoded = if route.starts_with("POST") {
        tail.strip_suffix("/properties").ok_or_else(bad_request)?
    } else {
        tail
    };
    if encoded.is_empty() || encoded.contains('/') {
        return Err(bad_request());
    }
    let identifier = NamespaceIdentifier::from_rest(&decode_path(encoded)?).map_err(|_| bad_request())?;
    if route.starts_with("DELETE") {
        if !bytes.is_empty() {
            return Err(bad_request());
        }
        return Ok(NamespaceMutation::Drop(identifier));
    }
    let body: UpdateBody = serde_json::from_slice(bytes).map_err(|_| bad_request())?;
    let changes = PropertyChanges {
        removals: body.removals,
        updates: body.updates,
    };
    changes
        .validate()
        .map_err(|error| super::namespace_write::mutation_error(&error.into()))?;
    Ok(NamespaceMutation::Update(identifier, changes))
}
