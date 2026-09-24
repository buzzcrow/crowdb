use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_access_iceberg::catalog::{CatalogContext, CatalogError};
use crowdb_access_iceberg::error::ValidationError;
use crowdb_access_iceberg::namespace::{
    NamespaceCreateRequest, NamespaceCreator, NamespaceDropRequest, NamespaceDropper,
    NamespacePropertyRequest, NamespaceRepository, NamespaceStore,
};
use crowdb_access_iceberg::operation::{PayloadStore, RetryAdmission, RetryLedger, RetryRecord};
use crowdb_access_iceberg::wire::{IcebergErrorResponse, Principal, RequestKey};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Request, Response};
use sha2::{Digest, Sha256};

use super::body::IcebergBody;
use super::http::{bad_request, response, service_unavailable};
use super::namespace_request::{self, NamespaceMutation};

pub(super) struct NamespaceWrites {
    creator: NamespaceCreator,
    dropper: NamespaceDropper,
    repository: NamespaceRepository,
    ledger: RetryLedger,
    payloads: PayloadStore,
}

impl NamespaceWrites {
    pub(super) fn new<Store: NamespaceStore + 'static>(store: Arc<Store>) -> Self {
        Self {
            creator: NamespaceCreator::new(store.clone()),
            dropper: NamespaceDropper::new(store.clone()),
            repository: NamespaceRepository::new(store.clone()),
            ledger: RetryLedger::new(store.clone()),
            payloads: PayloadStore::new(store),
        }
    }

    pub(super) async fn execute(
        &self,
        context: CatalogContext,
        principal: Principal,
        request: Request<Incoming>,
    ) -> Result<Response<IcebergBody>, IcebergErrorResponse> {
        let route = namespace_request::route(request.method(), request.uri()).ok_or_else(|| {
            IcebergErrorResponse::new(
                406,
                "UnsupportedOperationException",
                "This endpoint is not implemented",
            )
        })?;
        if !principal.namespace_write {
            return Err(IcebergErrorResponse::new(
                403,
                "ForbiddenException",
                "Namespace write privilege is required",
            ));
        }
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
        let request_key = RequestKey::parse(header, now).map_err(|_| bad_request())?;
        let uri = request.uri().clone();
        let bytes = read_body(request.into_body()).await?;
        let mut digest = Sha256::new();
        for value in [route.as_bytes(), uri.to_string().as_bytes(), bytes.as_slice()] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
        }
        let retry = RetryRecord {
            identity: request_key.identity(),
            principal: principal.name.into(),
            route: route.into(),
            digest: digest.finalize().into(),
            context,
            retained_until_ms: 0,
            status: 0,
            body: Vec::new(),
        };
        let retry = match self.admit(retry, request_key, now).await? {
            RetryAdmission::Replay(record) => return Ok(response(record.status, record.body)),
            RetryAdmission::New(record) | RetryAdmission::Resume(record) => record,
        };
        let result = match namespace_request::parse(route, &uri, &bytes) {
            Ok(mutation) => self.mutate(&retry, mutation).await,
            Err(error) => Err(error),
        };
        let (status, body) = match result {
            Ok(result) => result,
            Err(error) if error.error.code < 500 => (
                error.error.code,
                serde_json::to_vec(&error).map_err(|_| service_unavailable())?,
            ),
            Err(error) => return Err(error),
        };
        self.ledger
            .finish(retry, status, body.clone(), now_ms()?)
            .await
            .map_err(|error| mutation_error(&error))?;
        Ok(response(status, body))
    }

    async fn admit(
        &self,
        mut retry: RetryRecord,
        request_key: RequestKey,
        now: u64,
    ) -> Result<RetryAdmission, IcebergErrorResponse> {
        for _ in 0..8 {
            match self.ledger.begin(retry.clone(), now).await {
                Err(CatalogError::Busy) if matches!(request_key, RequestKey::Internal(_)) => {
                    retry.identity = RequestKey::parse(None, now)
                        .map_err(|_| bad_request())?
                        .identity();
                }
                result => return result.map_err(|error| mutation_error(&error)),
            }
        }
        Err(service_unavailable())
    }

    async fn mutate(
        &self,
        retry: &RetryRecord,
        mutation: NamespaceMutation,
    ) -> Result<(u16, Vec<u8>), IcebergErrorResponse> {
        let outcome = match mutation {
            NamespaceMutation::Create(identifier, properties) => Some(
                self.creator
                    .create(&NamespaceCreateRequest {
                        context: retry.context,
                        identity: retry.identity,
                        principal: retry.principal.clone(),
                        identifier,
                        properties,
                    })
                    .await
                    .map_err(|error| mutation_error(&error))?,
            ),
            NamespaceMutation::Update(identifier, changes) => self
                .repository
                .update_properties(&NamespacePropertyRequest {
                    context: retry.context,
                    identity: retry.identity,
                    principal: retry.principal.clone(),
                    identifier,
                    changes,
                })
                .await
                .map_err(|error| mutation_error(&error))?,
            NamespaceMutation::Drop(identifier) => self
                .dropper
                .drop_namespace(&NamespaceDropRequest {
                    context: retry.context,
                    identity: retry.identity,
                    principal: retry.principal.clone(),
                    identifier,
                })
                .await
                .map_err(|error| mutation_error(&error))?,
        }
        .ok_or_else(|| {
            IcebergErrorResponse::new(404, "NoSuchNamespaceException", "Namespace does not exist")
        })?;
        let bytes = self
            .payloads
            .get(&outcome.body)
            .await
            .map_err(|_| service_unavailable())?;
        Ok((outcome.status, bytes))
    }
}

pub(super) async fn read_body(mut body: Incoming) -> Result<Vec<u8>, IcebergErrorResponse> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| bad_request())?;
        if let Ok(data) = frame.into_data() {
            if bytes.len() + data.len() > 2 * 1024 * 1024 {
                return Err(bad_request());
            }
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes)
}

pub(super) fn now_ms() -> Result<u64, IcebergErrorResponse> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| service_unavailable())?;
    u64::try_from(elapsed.as_millis()).map_err(|_| service_unavailable())
}

pub(super) fn mutation_error(error: &CatalogError) -> IcebergErrorResponse {
    match error {
        CatalogError::Invalid(ValidationError::PropertyOverlap) => IcebergErrorResponse::new(
            422,
            "UnprocessableEntityException",
            "Property removals and updates overlap",
        ),
        CatalogError::Invalid(
            ValidationError::Text
            | ValidationError::KeyTooLarge
            | ValidationError::RecordTooLarge
            | ValidationError::GenerationExhausted
            | ValidationError::Deadline
            | ValidationError::Identity,
        ) => bad_request(),
        CatalogError::Conflict => IcebergErrorResponse::new(
            409,
            "CommitFailedException",
            "Request identity or catalog context conflicts",
        ),
        _ => {
            tracing::error!(%error, "namespace mutation remains recoverable; retry with the same request key");
            service_unavailable()
        }
    }
}
