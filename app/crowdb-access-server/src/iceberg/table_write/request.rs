use super::super::{http::bad_request, namespace_read::decode_path};
use crowdb_access_iceberg::{key::NameSuffix, namespace::NamespaceIdentifier, wire::IcebergErrorResponse};

pub(in crate::iceberg) struct Target {
    pub namespace: NamespaceIdentifier,
    pub name: Option<String>,
}

pub(in crate::iceberg) fn parse(uri: &hyper::Uri) -> Result<Target, IcebergErrorResponse> {
    if uri.query().is_some() {
        return Err(bad_request());
    }
    let suffix = uri
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
        return Err(super::super::table_read::unsupported());
    }
    if let Some(name) = &name {
        NameSuffix { parent: None, name }
            .encode()
            .map_err(|_| bad_request())?;
    }
    Ok(Target { namespace, name })
}
