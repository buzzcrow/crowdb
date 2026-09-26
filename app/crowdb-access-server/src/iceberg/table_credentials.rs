use std::{collections::BTreeMap, sync::Arc};

use crowdb_access_iceberg::{
    catalog::{CatalogContext, CatalogLifecycle, CatalogRepository, CatalogStore, FormatAction, RootState},
    commit::{TableCreateJournal, TableCreatePhase},
    file::FileGrantIssuer,
    key::{OperationId, TableId},
    namespace::{NamespaceIdentifier, NamespaceRepository, NamespaceStore},
    table::TableRepository,
    wire::{
        FileDelegationLimits, FileDelegationTarget, IcebergErrorResponse, LoadCredentialsResponse, Principal,
    },
};
use hyper::{Request, Response};

use super::{
    body::IcebergBody,
    http::{bad_request, decode_query, response, service_unavailable},
    namespace_write::now_ms,
    table_read::{missing_table, unsupported},
};

pub(super) struct TableCredentials {
    store: Arc<dyn CatalogStore>,
    namespaces: NamespaceRepository,
    tables: TableRepository,
    issuer: FileGrantIssuer,
}

#[derive(Clone)]
pub(super) struct TableFileConfig {
    endpoint: String,
}

impl TableFileConfig {
    pub(super) fn append(
        &self,
        bytes: &mut Vec<u8>,
        namespace: &NamespaceIdentifier,
        name: &str,
        table: TableId,
    ) -> Result<(), IcebergErrorResponse> {
        let config = serde_json::to_vec(&self.properties(namespace, name, table))
            .map_err(|_| service_unavailable())?;
        if bytes.last() != Some(&b'}')
            || bytes.len() + config.len() + 16 > crowdb_access_iceberg::operation::MAX_PAYLOAD_BYTES
        {
            return Err(service_unavailable());
        }
        bytes.pop();
        bytes.extend_from_slice(b",\"config\":");
        bytes.extend_from_slice(&config);
        bytes.push(b'}');
        Ok(())
    }
    pub(super) fn new(mut endpoint: String) -> Result<Self, crowdb_access_iceberg::error::ValidationError> {
        let uri: hyper::Uri = endpoint
            .parse()
            .map_err(|_| crowdb_access_iceberg::error::ValidationError::Text)?;
        if !matches!(uri.scheme_str(), Some("http" | "https"))
            || uri.authority().is_none()
            || uri
                .authority()
                .is_some_and(|authority| authority.as_str().contains('@'))
            || uri.path() != "/"
            || uri.query().is_some()
            || endpoint.len() > 2048
        {
            return Err(crowdb_access_iceberg::error::ValidationError::Text);
        }
        endpoint.truncate(endpoint.trim_end_matches('/').len());
        Ok(Self { endpoint })
    }

    pub(super) fn properties(
        &self,
        namespace: &NamespaceIdentifier,
        name: &str,
        table: TableId,
    ) -> BTreeMap<&'static str, String> {
        let namespace = namespace.components().join("\u{1f}");
        let namespace = percent_encoding::utf8_percent_encode(&namespace, percent_encoding::NON_ALPHANUMERIC);
        let name = percent_encoding::utf8_percent_encode(name, percent_encoding::NON_ALPHANUMERIC);
        BTreeMap::from([
            ("s3.endpoint", self.endpoint.clone()),
            ("s3.path-style-access", "true".into()),
            ("client.region", "us-east-1".into()),
            (
                "client.refresh-credentials-endpoint",
                format!("/v1/namespaces/{namespace}/tables/{name}/credentials?table-id={table}"),
            ),
        ])
    }
}

impl TableCredentials {
    pub(super) fn new<Store: NamespaceStore + 'static>(
        store: Arc<Store>,
        secret: [u8; 32],
    ) -> Result<Self, crowdb_access_iceberg::file::FileGrantError> {
        Ok(Self {
            store: store.clone(),
            namespaces: NamespaceRepository::new(store.clone()),
            tables: TableRepository::new(store),
            issuer: FileGrantIssuer::new(secret, 900_000)?,
        })
    }

    pub(super) async fn load(
        &self,
        repository: &CatalogRepository,
        context: CatalogContext,
        principal: Principal,
        request: &Request<hyper::body::Incoming>,
    ) -> Result<Response<IcebergBody>, IcebergErrorResponse> {
        if request.method() != hyper::Method::GET {
            return Err(super::table_read::unsupported());
        }
        let path = request
            .uri()
            .path()
            .strip_suffix("/credentials")
            .ok_or_else(bad_request)?;
        let target = super::table_write::request::parse(&path.parse().map_err(|_| bad_request())?)?;
        let name = target.name.ok_or_else(bad_request)?;
        let selector = selector(request.uri().query())?;
        let now = now_ms()?;
        let parent = self
            .namespaces
            .load(context, &target.namespace)
            .await
            .map_err(|_| service_unavailable())?
            .ok_or_else(missing_table)?;
        let selected = self
            .tables
            .select(context, parent.namespace, &name)
            .await
            .map_err(|_| service_unavailable())?;
        let mut ttl_ms = 900_000;
        let (table, version, staged) = if let Some(selected) =
            selected.filter(|selected| selector.map_or(true, |table| table == selected.head.table))
        {
            self.tables
                .ensure_current(context, &selected)
                .await
                .map_err(|_| service_unavailable())?;
            (selected.head.table, selected.head.format_version, false)
        } else {
            let table = selector.ok_or_else(missing_table)?;
            let identity = OperationId::from_bytes(table.as_bytes()).map_err(|_| bad_request())?;
            let journal = TableCreateJournal::new(self.store.clone());
            let operation = journal
                .load(context, identity)
                .await
                .map_err(|_| service_unavailable())?
                .ok_or_else(missing_table)?;
            let stage = operation.stage.as_ref().ok_or_else(missing_table)?;
            if !principal.namespace_write
                || operation.principal != principal.name
                || operation.namespace != target.namespace
                || operation.candidate.namespace != parent.namespace
                || operation.candidate.name != name
                || operation.candidate.table != table
                || operation.phase != TableCreatePhase::Staged
            {
                return Err(missing_table());
            }
            let expires = u64::try_from(stage.expires_ms).map_err(|_| service_unavailable())?;
            ttl_ms = ttl_ms.min(
                expires
                    .checked_sub(now)
                    .filter(|remaining| *remaining > 0)
                    .ok_or_else(missing_table)?,
            );
            if journal
                .load(context, identity)
                .await
                .map_err(|_| service_unavailable())?
                .as_ref()
                != Some(&operation)
            {
                return Err(service_unavailable());
            }
            (table, operation.candidate.format_version, true)
        };
        self.issue_response(
            repository,
            context,
            principal,
            FileDelegationTarget {
                table,
                format_version: version,
                staged,
            },
            ttl_ms,
            now,
        )
        .await
    }

    async fn issue_response(
        &self,
        repository: &CatalogRepository,
        context: CatalogContext,
        principal: Principal,
        target: FileDelegationTarget,
        ttl_ms: u64,
        now: u64,
    ) -> Result<Response<IcebergBody>, IcebergErrorResponse> {
        let (root, authority) = repository.status().await.map_err(|_| service_unavailable())?;
        if root.context != context
            || root.state != RootState::Ready
            || authority.lifecycle != CatalogLifecycle::Ready
        {
            return Err(service_unavailable());
        }
        if !authority
            .capabilities
            .supports(target.format_version, FormatAction::Read)
        {
            return Err(unsupported());
        }
        let credentials = FileDelegationLimits {
            ttl_ms,
            max_request_bytes: 1024 * 1024 * 1024,
            max_file_bytes: 1024 * 1024 * 1024 * 1024,
        }
        .issue(&self.issuer, principal, context, &authority, target, now)
        .map_err(|_| service_unavailable())?;
        let pins = crowdb_access_iceberg::gc::ReaderPins::new(self.store.clone());
        let expires_ms = pins
            .request_expiry(context, credentials.grant().expires_ms)
            .await
            .map_err(|_| service_unavailable())?;
        pins.protect_files(context, target.table, principal.name, expires_ms, now)
            .await
            .map_err(|_| service_unavailable())?;
        Ok(response(
            200,
            serde_json::to_vec(&LoadCredentialsResponse::from(credentials))
                .map_err(|_| service_unavailable())?,
        ))
    }
}

fn selector(query: Option<&str>) -> Result<Option<TableId>, IcebergErrorResponse> {
    let mut selector = None;
    for pair in query
        .unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
    {
        let (name, value) = pair.split_once('=').ok_or_else(bad_request)?;
        if decode_query(name)? != "table-id" || selector.is_some() {
            return Err(bad_request());
        }
        selector = Some(decode_query(value)?.parse().map_err(|_| bad_request())?);
    }
    Ok(selector)
}
