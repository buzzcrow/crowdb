use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_access_iceberg::catalog::{CatalogContext, CatalogError};
use crowdb_access_iceberg::error::ValidationError;
use crowdb_access_iceberg::namespace::{
    NamespaceCreateRequest, NamespaceCreator, NamespaceDropRequest, NamespaceDropper,
    NamespacePropertyRequest, NamespaceRepository, NamespaceStore,
};
use crowdb_access_iceberg::operation::{PayloadStore, RequestIdentity};
use crowdb_access_iceberg::wire::{IcebergErrorResponse, Principal, RequestKey};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Request, Response};

use super::body::IcebergBody;
use super::http::{bad_request, response, service_unavailable};
use super::namespace_request::{self, NamespaceMutation};

struct NamespaceMutationRequest {
    context: CatalogContext,
    identity: RequestIdentity,
    principal: String,
}

pub(super) struct NamespaceWrites {
    creator: NamespaceCreator,
    dropper: NamespaceDropper,
    repository: NamespaceRepository,
    payloads: PayloadStore,
}

impl NamespaceWrites {
    pub(super) fn new<Store: NamespaceStore + 'static>(store: Arc<Store>) -> Self {
        Self {
            creator: NamespaceCreator::new(store.clone()),
            dropper: NamespaceDropper::new(store.clone()),
            repository: NamespaceRepository::new(store.clone()),
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
        if route == "POST /namespaces/{namespace}/properties" {
            let NamespaceMutation::Update(identifier, changes) =
                namespace_request::parse(route, &uri, &bytes)?
            else {
                return Err(bad_request());
            };
            let (status, body) = self.update_properties(context, identifier, changes).await?;
            return Ok(response(status, body));
        }
        let retry = NamespaceMutationRequest {
            identity: request_key.identity(),
            principal: principal.name.into(),
            context,
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
        Ok(response(status, body))
    }

    async fn mutate(
        &self,
        retry: &NamespaceMutationRequest,
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
            NamespaceMutation::Update(identifier, changes) => {
                return self.update_properties(retry.context, identifier, changes).await;
            }
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

    async fn update_properties(
        &self,
        context: CatalogContext,
        identifier: crowdb_access_iceberg::namespace::NamespaceIdentifier,
        changes: crowdb_access_iceberg::namespace::PropertyChanges,
    ) -> Result<(u16, Vec<u8>), IcebergErrorResponse> {
        let outcome = self
            .repository
            .update_properties(&NamespacePropertyRequest {
                context,
                identifier,
                changes,
            })
            .await
            .map_err(|error| mutation_error(&error))?
            .ok_or_else(|| {
                IcebergErrorResponse::new(404, "NoSuchNamespaceException", "Namespace does not exist")
            })?;
        let body = serde_json::to_vec(&serde_json::json!({
            "removed": outcome.removed,
            "updated": outcome.updated,
            "missing": outcome.missing,
        }))
        .map_err(|_| service_unavailable())?;
        Ok((200, body))
    }
}

pub(super) async fn read_body(mut body: Incoming) -> Result<Vec<u8>, IcebergErrorResponse> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| bad_request())?;
        if let Ok(data) = frame.into_data() {
            super::metrics::record_request_bytes(data.len());
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
