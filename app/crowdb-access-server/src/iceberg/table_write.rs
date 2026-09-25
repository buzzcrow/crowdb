use std::sync::{atomic::AtomicUsize, Arc};

use crowdb_access_iceberg::{
    catalog::{CatalogContext, CatalogStore},
    commit::{CommitProofLimits, StagedCommitLimits, TableCreator},
    file::FileBlockStore,
    namespace::{NamespaceRepository, NamespaceStore},
    operation::{PayloadStore, RetryAdmission, RetryLedger, RetryRecord},
    table::TableRepository,
    wire::{IcebergErrorResponse, Principal, RequestKey},
};
use hyper::{body::Incoming, Method, Request, Response};
use sha2::{Digest, Sha256};

use super::{
    body::{IcebergBody, SpoolPermit},
    http::{bad_request, response, service_unavailable},
    namespace_write::{mutation_error, now_ms, read_body},
    table_limits,
};

mod lifecycle;
mod mutation;
pub(super) mod request;

pub(super) struct TableWrites {
    store: Arc<dyn CatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
    creator: TableCreator,
    namespaces: NamespaceRepository,
    tables: TableRepository,
    lifecycles: crowdb_access_iceberg::table::TableLifecycles,
    ledger: RetryLedger,
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
            ledger: RetryLedger::new(store.clone()),
            payloads: PayloadStore::new(store),
            limits,
            active: Arc::new(AtomicUsize::new(0)),
            file_config: None,
        }
    }

    pub(super) async fn execute(
        &self,
        context: CatalogContext,
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
        let _permit = SpoolPermit::acquire(&self.active).ok_or_else(service_unavailable)?;
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
        let bytes = read_body(request.into_body()).await?;
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
        let mut record = RetryRecord {
            identity: key.identity(),
            principal: principal.name.into(),
            route: route.into(),
            digest: digest.finalize().into(),
            context,
            retained_until_ms: 0,
            status: 0,
            body: Vec::new(),
        };
        let admission = self.admit(&mut record, key, now).await?;
        let record = match admission {
            RetryAdmission::Replay(record) => {
                super::metrics::record_retry(3);
                return Ok(response(record.status, record.body));
            }
            RetryAdmission::New(record) => {
                super::metrics::record_retry(1);
                record
            }
            RetryAdmission::Resume(record) => {
                super::metrics::record_retry(2);
                record
            }
        };
        if method == Method::DELETE || uri.path() == "/v1/tables/rename" {
            let result = self.mutate_lifecycle(&record, &method, &uri, &bytes).await;
            let (status, body) = self.outcome_response(result, None, context).await?;
            self.ledger
                .finish(record, status, body.clone(), now_ms()?)
                .await
                .map_err(|error| mutation_error(&error))?;
            return Ok(response(status, body));
        }
        let target = request::parse(&uri);
        let configuration_target = target.as_ref().ok().and_then(|target| {
            let name = target.name.clone().or_else(|| {
                crowdb_access_iceberg::commit::CreateTableRequest::decode(
                    &bytes,
                    self.limits.preparation.request.json,
                )
                .ok()
                .map(|request| request.name().to_owned())
            })?;
            Some((target.namespace.clone(), name))
        });
        let result = match target {
            Ok(target) => self.mutate(&record, target, bytes, now).await,
            Err(error) => Err(error),
        };
        let (status, body) = self
            .outcome_response(result, configuration_target, context)
            .await?;
        self.ledger
            .finish(record, status, body.clone(), now_ms()?)
            .await
            .map_err(|error| mutation_error(&error))?;
        Ok(response(status, body))
    }

    async fn outcome_response(
        &self,
        result: Result<crowdb_access_iceberg::commit::TableCommitOutcome, IcebergErrorResponse>,
        configuration_target: Option<(crowdb_access_iceberg::namespace::NamespaceIdentifier, String)>,
        context: CatalogContext,
    ) -> Result<(u16, Vec<u8>), IcebergErrorResponse> {
        let (status, mut body) = match result {
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
        if status == 200 {
            if let (Some(config), Some((namespace, name))) = (&self.file_config, configuration_target) {
                #[derive(serde::Deserialize)]
                struct Metadata {
                    location: String,
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
                config.append(&mut body, &namespace, &name, table.table)?;
            }
        }
        Ok((status, body))
    }

    async fn admit(
        &self,
        record: &mut RetryRecord,
        key: RequestKey,
        now: u64,
    ) -> Result<RetryAdmission, IcebergErrorResponse> {
        for _ in 0..8 {
            match self.ledger.begin(record.clone(), now).await {
                Err(crowdb_access_iceberg::catalog::CatalogError::Busy)
                    if matches!(key, RequestKey::Internal(_)) =>
                {
                    record.identity = RequestKey::parse(None, now)
                        .map_err(|_| bad_request())?
                        .identity();
                }
                result => return result.map_err(|error| mutation_error(&error)),
            }
        }
        Err(service_unavailable())
    }
}
