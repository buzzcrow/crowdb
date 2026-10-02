use crate::expand::Recursive;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::cluster::{GroupSummary, GroupView, StoreView};
use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops;
use crowdb_protocol::common::ReplicaValue;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::ErrorBody;
use crate::state::AppState;

type ApiError = (StatusCode, Json<ErrorBody>);

fn snapshot_error(mut error: ApiError) -> ApiError {
    if error.0 == StatusCode::BAD_GATEWAY {
        error.0 = StatusCode::SERVICE_UNAVAILABLE;
    }
    error
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn api_error(error: Error) -> ApiError {
    let status = match error {
        Error::NotFound { .. } => StatusCode::NOT_FOUND,
        Error::Conflict { .. } => StatusCode::CONFLICT,
        Error::Validation { .. } => StatusCode::BAD_REQUEST,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    (
        status,
        Json(ErrorBody {
            error: error.to_string(),
        }),
    )
}

fn protected_record() -> ApiError {
    (
        StatusCode::CONFLICT,
        Json(ErrorBody {
            error: "system store and group cannot be changed through the logical API".into(),
        }),
    )
}

#[derive(Deserialize)]
pub(crate) struct CreateStore {
    store_id: u64,
    #[serde(default)]
    nodes: Vec<u64>,
}

pub(crate) async fn list_stores(State(state): State<AppState>) -> Result<Json<Vec<StoreView>>, ApiError> {
    crate::mgmt::http_list_stores(State(state), Recursive::default())
        .await
        .map_err(snapshot_error)
}

pub(crate) async fn get_store(
    State(state): State<AppState>,
    Path(store_id): Path<u64>,
) -> Result<Json<StoreView>, ApiError> {
    crate::mgmt::http_get_store(State(state), Path(store_id), Recursive::default())
        .await
        .map_err(snapshot_error)
}

pub(crate) async fn add_store(
    State(state): State<AppState>,
    Json(body): Json<CreateStore>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if body.store_id == 0 {
        return Err(protected_record());
    }
    let context = state.op_context().await.map_err(api_error)?;
    let nodes = ops::kv_logical::add_store(&context, body.store_id, &body.nodes)
        .await
        .map_err(api_error)?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"store_id": body.store_id, "nodes": nodes})),
    ))
}

pub(crate) async fn remove_store(
    State(state): State<AppState>,
    Path(store_id): Path<u64>,
) -> Result<StatusCode, ApiError> {
    if store_id == 0 {
        return Err(protected_record());
    }
    let context = state.op_context().await.map_err(api_error)?;
    ops::kv_logical::remove_store(&context, store_id)
        .await
        .map_err(api_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub(crate) struct CreateGroup {
    group_id: u64,
    replica_id: u64,
    nodes: Vec<u64>,
}

pub(crate) async fn list_groups(
    State(state): State<AppState>,
    Path(store_id): Path<u64>,
) -> Result<Json<Vec<GroupSummary>>, ApiError> {
    crate::mgmt::http_list_groups(State(state), Path(store_id), Recursive::default())
        .await
        .map_err(snapshot_error)
}

pub(crate) async fn get_group(
    State(state): State<AppState>,
    Path(ids): Path<(u64, u64)>,
) -> Result<Json<GroupView>, ApiError> {
    crate::mgmt::http_get_group(State(state), Path(ids), Recursive::default())
        .await
        .map_err(snapshot_error)
}

pub(crate) async fn add_group(
    State(state): State<AppState>,
    Path(store_id): Path<u64>,
    Json(body): Json<CreateGroup>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if store_id == 0 && body.group_id == 0 {
        return Err(protected_record());
    }
    let context = state.op_context().await.map_err(api_error)?;
    ops::kv_logical::add_group(&context, store_id, body.group_id, body.replica_id, &body.nodes)
        .await
        .map_err(api_error)?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"store_id": store_id, "group_id": body.group_id, "nodes": body.nodes})),
    ))
}

pub(crate) async fn remove_group(
    State(state): State<AppState>,
    Path((store_id, group_id)): Path<(u64, u64)>,
) -> Result<StatusCode, ApiError> {
    if store_id == 0 && group_id == 0 {
        return Err(protected_record());
    }
    let context = state.op_context().await.map_err(api_error)?;
    ops::kv_logical::remove_group(&context, store_id, group_id)
        .await
        .map_err(api_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub(crate) struct CreateReplica {
    node_id: u64,
    replica_id: Option<u64>,
}

pub(crate) async fn list_replicas(
    State(state): State<AppState>,
    Path((store_id, group_id)): Path<(u64, u64)>,
) -> Result<Json<Vec<ReplicaValue>>, ApiError> {
    let context = state.op_context().await.map_err(api_error)?;
    ops::kv_logical::list_replicas(&context, store_id, group_id)
        .await
        .map(Json)
        .map_err(api_error)
}

pub(crate) async fn get_replica(
    State(state): State<AppState>,
    Path((store_id, group_id, replica_id)): Path<(u64, u64, u64)>,
) -> Result<Json<ReplicaValue>, ApiError> {
    let context = state.op_context().await.map_err(api_error)?;
    context
        .sysmd()
        .get_replica(store_id, group_id, replica_id)
        .await
        .map_err(|error| api_error(error.into()))?
        .map(Json)
        .ok_or_else(|| {
            api_error(Error::NotFound {
                kind: "replica".into(),
                id: format!("{store_id}/{group_id}/{replica_id}"),
            })
        })
}

pub(crate) async fn add_replica(
    State(state): State<AppState>,
    Path((store_id, group_id)): Path<(u64, u64)>,
    Json(body): Json<CreateReplica>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if store_id == 0 && group_id == 0 {
        return Err(protected_record());
    }
    let context = state.op_context().await.map_err(api_error)?;
    let replica_id =
        ops::kv_logical::add_replica(&context, store_id, group_id, body.node_id, body.replica_id)
            .await
            .map_err(api_error)?;
    Ok((
        StatusCode::CREATED,
        Json(
            json!({"store_id": store_id, "group_id": group_id, "replica_id": replica_id, "node_id": body.node_id}),
        ),
    ))
}

pub(crate) async fn remove_replica(
    State(state): State<AppState>,
    Path((store_id, group_id, replica_id)): Path<(u64, u64, u64)>,
) -> Result<StatusCode, ApiError> {
    if store_id == 0 && group_id == 0 {
        return Err(protected_record());
    }
    let context = state.op_context().await.map_err(api_error)?;
    ops::kv_logical::remove_replica(&context, store_id, group_id, replica_id)
        .await
        .map_err(api_error)?;
    Ok(StatusCode::NO_CONTENT)
}
