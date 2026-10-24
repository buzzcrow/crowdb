use std::sync::{atomic::AtomicUsize, Arc};

use crowdb_access_iceberg::{
    catalog::{CatalogAuthority, CatalogContext, CatalogStore},
    commit::{CommitProofLimits, StagedCommitLimits, TableCreator},
    file::FileBlockStore,
    namespace::{NamespaceRepository, NamespaceStore},
    operation::{PayloadStore, RequestIdentity},
    table::TableRepository,
    wire::{IcebergErrorResponse, Principal, RequestKey},
};
use hyper::{body::Incoming, Method, Request, Response, Uri};
use sha2::{Digest, Sha256};

use super::{
    body::{IcebergBody, SpoolPermit},
    http::{bad_request, response, service_unavailable},
    namespace_write::{now_ms, read_body},
    table_limits,
};

mod lifecycle;
mod mutation;
pub(super) mod request;

pub(super) struct TableMutation {
    context: CatalogContext,
    identity: RequestIdentity,
    principal: String,
    digest: [u8; 32],
}

pub(super) struct TableWrites {
    store: Arc<dyn CatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
    creator: TableCreator,
    namespaces: NamespaceRepository,
    tables: TableRepository,
    lifecycles: crowdb_access_iceberg::table::TableLifecycles,
    payloads: PayloadStore,
    limits: CommitProofLimits,
    active: Arc<AtomicUsize>,
    pub(super) file_config: Option<super::table_credentials::TableFileConfig>,
}

impl TableWrites {
    pub(super) fn new<Store: NamespaceStore + 'static>(
        store: Arc<Store>,
        blocks: Arc<dyn FileBlockStore>,
    ) -> Self {
        let limits = table_limits::commits();
        Self {
            lifecycles: crowdb_access_iceberg::table::TableLifecycles::new(store.clone()),
            store: store.clone(),
            blocks: blocks.clone(),
            creator: TableCreator::new(store.clone(), blocks)
                .with_response_reserve(table_limits::RESPONSE_RESERVE)
                .with_staged_limits(StagedCommitLimits {
                    evaluation: limits.preparation.evaluation,
                    snapshots: limits.snapshots,
                    auxiliary: limits.auxiliary,
                }),
            namespaces: NamespaceRepository::new(store.clone()),
            tables: TableRepository::new(store.clone()),
            payloads: PayloadStore::new(store),
            limits,
            active: Arc::new(AtomicUsize::new(0)),
            file_config: None,
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(super) async fn execute(
        &self,
        context: CatalogContext,
        authority: &CatalogAuthority,
        principal: Principal,
        request: Request<Incoming>,
    ) -> Result<Response<IcebergBody>, IcebergErrorResponse> {
        if request.method() != Method::POST && request.method() != Method::DELETE {
            return Err(super::table_read::unsupported());
        }
        if !principal.namespace_write {
            return Err(IcebergErrorResponse::new(
                403,
                "ForbiddenException",
                "Table write privilege is required",
            ));
        }
        let _permit = SpoolPermit::acquire(&self.active, 2 * 1024 * 1024).ok_or_else(service_unavailable)?;
        let now = now_ms()?;
        if request.headers().get_all("idempotency-key").iter().count() > 1 {
            return Err(bad_request());
        }
        let header = request
            .headers()
            .get("idempotency-key")
            .map(|value| value.to_str())
            .transpose()
            .map_err(|_| bad_request())?;
        let key = RequestKey::parse(header, now).map_err(|_| bad_request())?;
        let uri = request.uri().clone();
        let method = request.method().clone();
        let request_host = request
            .headers()
            .get(hyper::header::HOST)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let bytes = read_body(request.into_body()).await?;
        let capabilities = authority.capabilities;
        let configuration_target = self.configuration_target(&uri, &method, &bytes);
        let route = if method == Method::DELETE {
            "DELETE table"
        } else {
            "POST table"
        };
        let mut digest = Sha256::new();
        for value in [route.as_bytes(), uri.to_string().as_bytes(), bytes.as_slice()] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
        }
        let record = TableMutation {
            identity: key.identity(),
            principal: principal.name.into(),
            digest: digest.finalize().into(),
            context,
        };
        if method == Method::POST && uri.path() != "/v1/tables/rename" {
            if let Some(body) = self
                .try_direct_update(&record, capabilities, &uri, &bytes)
                .await?
            {
                let body = self.decorate_response(
                    200,
                    body,
                    configuration_target,
                    context,
                    authority,
                    principal,
                    request_host.as_deref(),
                )?;
                return Ok(response(200, body));
            }
        }
        if method == Method::DELETE || uri.path() == "/v1/tables/rename" {
            let outcome = self
                .mutate_lifecycle(&record, capabilities, &method, &uri, &bytes)
                .await?;
            return Ok(response(outcome.status, outcome.body));
        }
        let target = request::parse(&uri);
        let result = match target {
            Ok(target) => Box::pin(self.mutate(&record, capabilities, target, bytes, now)).await,
            Err(error) => Err(error),
        };
        let (status, body) = self.outcome_response(result).await?;
        let body = self.decorate_response(
            status,
            body,
            configuration_target,
            context,
            authority,
            principal,
            request_host.as_deref(),
        )?;
        Ok(response(status, body))
    }

    async fn outcome_response(
        &self,
        result: Result<crowdb_access_iceberg::commit::TableCommitOutcome, IcebergErrorResponse>,
    ) -> Result<(u16, Vec<u8>), IcebergErrorResponse> {
        let (status, body) = match result {
            Ok(outcome) => (
                outcome.status,
                self.payloads
                    .get(&outcome.body)
                    .await
                    .map_err(|_| service_unavailable())?,
            ),
            Err(error) if error.error.code < 500 => (
                error.error.code,
                serde_json::to_vec(&error).map_err(|_| service_unavailable())?,
            ),
            Err(error) => return Err(error),
        };
        Ok((status, body))
    }

    #[allow(clippy::too_many_arguments)]
    fn decorate_response(
        &self,
        status: u16,
        mut body: Vec<u8>,
        configuration_target: Option<(
            crowdb_access_iceberg::namespace::NamespaceIdentifier,
            String,
            bool,
        )>,
        context: CatalogContext,
        authority: &CatalogAuthority,
        principal: Principal,
        request_host: Option<&str>,
    ) -> Result<Vec<u8>, IcebergErrorResponse> {
        if status == 200 {
            if let (Some(config), Some((namespace, name, staged))) = (&self.file_config, configuration_target)
            {
                #[derive(serde::Deserialize)]
                struct Metadata {
                    location: String,
                    #[serde(rename = "format-version")]
                    format_version: u8,
                }
                #[derive(serde::Deserialize)]
                struct Envelope {
                    metadata: Metadata,
                }
                let envelope: Envelope = serde_json::from_slice(&body).map_err(|_| service_unavailable())?;
                let table: crowdb_access_iceberg::file::TableLocation =
                    format!("{}/", envelope.metadata.location.trim_end_matches('/'))
                        .parse()
                        .map_err(|_| service_unavailable())?;
                if table.catalog != context.catalog {
                    return Err(service_unavailable());
                }
                config.append(
                    &mut body,
                    super::table_credentials::TableFileTarget {
                        storage_namespace: table.storage_namespace,
                        namespace: &namespace,
                        name: &name,
                        table: table.table,
                        version: envelope.metadata.format_version,
                        staged,
                    },
                    super::table_credentials::TableFileAccess {
                        context,
                        authority,
                        principal,
                    },
                    request_host,
                )?;
            }
        }
        Ok(body)
    }

    fn configuration_target(
        &self,
        uri: &Uri,
        method: &Method,
        bytes: &[u8],
    ) -> Option<(
        crowdb_access_iceberg::namespace::NamespaceIdentifier,
        String,
        bool,
    )> {
        if *method == Method::DELETE || uri.path() == "/v1/tables/rename" {
            return None;
        }
        let target = request::parse(uri).ok()?;
        let (name, staged) = if let Some(name) = target.name {
            (name, false)
        } else {
            let create = crowdb_access_iceberg::commit::CreateTableRequest::decode(
                bytes,
                self.limits.preparation.request.json,
            )
            .ok()?;
            (create.name().to_owned(), create.stage_create())
        };
        Some((target.namespace, name, staged))
    }
}
