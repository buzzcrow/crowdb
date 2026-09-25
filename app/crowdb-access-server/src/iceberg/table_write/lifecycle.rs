use super::{
    super::http::{bad_request, decode_query, service_unavailable},
    TableWrites,
};
use crowdb_access_iceberg::{
    catalog::{Capabilities, FormatAction},
    commit::TableCommitOutcome,
    key::NameSuffix,
    namespace::NamespaceIdentifier,
    operation::RetryRecord,
    table::{TableLifecycleAction, TableLifecycleRequest},
    wire::IcebergErrorResponse,
};
use hyper::{Method, Uri};

#[derive(serde::Deserialize)]
struct Identifier {
    namespace: Vec<String>,
    name: String,
}

#[derive(serde::Deserialize)]
struct Rename {
    source: Identifier,
    destination: Identifier,
}

impl Identifier {
    fn validate(self) -> Result<(NamespaceIdentifier, String), IcebergErrorResponse> {
        NameSuffix {
            parent: None,
            name: &self.name,
        }
        .encode()
        .map_err(|_| bad_request())?;
        Ok((
            NamespaceIdentifier::new(self.namespace).map_err(|_| bad_request())?,
            self.name,
        ))
    }
}

impl TableWrites {
    pub(super) async fn mutate_lifecycle(
        &self,
        record: &RetryRecord,
        capabilities: Capabilities,
        resuming: bool,
        method: &Method,
        uri: &Uri,
        bytes: &[u8],
    ) -> Result<TableCommitOutcome, IcebergErrorResponse> {
        let (namespace, name, action) = if uri.path() == "/v1/tables/rename" {
            if *method != Method::POST {
                return Err(super::super::table_read::unsupported());
            }
            if uri.query().is_some() {
                return Err(bad_request());
            }
            let rename: Rename = serde_json::from_slice(bytes).map_err(|_| bad_request())?;
            let (namespace, name) = rename.source.validate()?;
            let (destination, target) = rename.destination.validate()?;
            (
                namespace,
                name,
                TableLifecycleAction::Rename {
                    namespace: destination,
                    name: target,
                },
            )
        } else {
            if *method != Method::DELETE || !bytes.is_empty() {
                return Err(bad_request());
            }
            let purge_requested = purge(uri)?;
            let path: Uri = uri.path().parse().map_err(|_| bad_request())?;
            let target = super::request::parse(&path)?;
            (
                target.namespace,
                target.name.ok_or_else(super::super::table_read::unsupported)?,
                TableLifecycleAction::Drop { purge_requested },
            )
        };
        let existing = if resuming {
            self.lifecycles
                .load(record.context, record.identity.operation)
                .await
                .map_err(|_| service_unavailable())?
        } else {
            None
        };
        let version = if let Some(operation) = existing {
            operation.before.format_version
        } else {
            let parent = self
                .namespaces
                .load(record.context, &namespace)
                .await
                .map_err(|_| service_unavailable())?
                .ok_or_else(super::super::table_read::missing_table)?;
            self.tables
                .select(record.context, parent.namespace, &name)
                .await
                .map_err(|_| service_unavailable())?
                .ok_or_else(super::super::table_read::missing_table)?
                .head
                .format_version
        };
        if !capabilities.supports(version, FormatAction::Write) {
            return Err(super::super::table_read::unsupported());
        }
        self.lifecycles.execute(&TableLifecycleRequest { context: record.context, identity: record.identity,
            principal: record.principal.clone(), namespace, name, action }).await.map_err(|error| {
                tracing::error!(%error, "table lifecycle remains recoverable; retry with the same request key");
                service_unavailable()
            })
    }
}

fn purge(uri: &Uri) -> Result<bool, IcebergErrorResponse> {
    let Some(query) = uri.query() else {
        return Ok(false);
    };
    let (name, value) = query.split_once('=').ok_or_else(bad_request)?;
    if decode_query(name)? != "purgeRequested" {
        return Err(bad_request());
    }
    match decode_query(value)?.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(bad_request()),
    }
}
