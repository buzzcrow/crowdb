//! Bounded, read-only inspection of references reachable from a selected table snapshot.

use std::collections::BTreeMap;

use crowdb_access_iceberg::{
    catalog::{CatalogAuthority, CatalogContext},
    file::{FileLocation, FileRecord, TableLocation},
    namespace::NamespaceIdentifier,
    table::{SnapshotLoadingMode, TableLoad},
    wire::IcebergErrorResponse,
};
use hyper::Response;
use serde_json::{json, Value};

use super::{
    body::IcebergBody,
    http::{bad_request, response},
    table_read::{missing_table, TableHttp},
};

mod cursor;
mod manifests;
mod pages;
mod parquet;
mod present;

const PAGE: usize = 100;
const MAX_SCAN: usize = 100_000;
type Result<T> = std::result::Result<T, IcebergErrorResponse>;

struct Selection<'a> {
    service: &'a TableHttp,
    context: CatalogContext,
    metadata: Value,
    snapshot: Value,
    offset: usize,
    cursor_token: Option<String>,
    generation: String,
    table: TableLocation,
}

impl TableHttp {
    pub(super) async fn inspect(
        &self,
        context: CatalogContext,
        authority: &CatalogAuthority,
        namespace: &NamespaceIdentifier,
        name: &str,
        mut query: BTreeMap<String, String>,
    ) -> Result<Response<IcebergBody>> {
        let generation = query.remove("metadata").ok_or_else(bad_request)?;
        let snapshot = query
            .remove("snapshot")
            .ok_or_else(bad_request)?
            .parse::<i64>()
            .map_err(|_| bad_request())?;
        let manifest = query.remove("manifest");
        let file = query.remove("file");
        let continuation = query.remove("offset").unwrap_or_else(|| "0".into());
        let cursor_token = continuation.starts_with("c.").then(|| continuation.clone());
        let offset = if cursor_token.is_some() {
            0
        } else {
            continuation.parse::<usize>().map_err(|_| bad_request())?
        };
        if !query.is_empty()
            || offset > MAX_SCAN - PAGE
            || file.is_some() && (manifest.is_none() || cursor_token.is_some())
        {
            return Err(bad_request());
        }
        let loaded = self
            .loader
            .load_with_capabilities(
                context,
                namespace,
                name,
                SnapshotLoadingMode::All,
                authority.capabilities,
            )
            .await
            .map_err(failed)?;
        let TableLoad::Loaded { head, metadata, .. } = loaded else {
            return Err(missing_table());
        };
        if head.metadata_location.to_string() != generation {
            return Err(stale());
        }
        let metadata: Value = serde_json::from_slice(&metadata).map_err(failed)?;
        let snapshot = metadata["snapshots"]
            .as_array()
            .and_then(|items| {
                items
                    .iter()
                    .find(|item| item["snapshot-id"].as_i64() == Some(snapshot))
            })
            .cloned()
            .ok_or_else(missing_reference)?;
        let selection = Selection {
            service: self,
            context,
            metadata,
            snapshot,
            offset,
            cursor_token,
            generation: generation.clone(),
            table: head.metadata_location.table(),
        };
        let mut data = selection.browse(manifest.as_deref(), file.as_deref()).await?;
        let current = self
            .loader
            .head(context, namespace, name)
            .await
            .map_err(failed)?
            .ok_or_else(missing_table)?;
        if current.generation != head.generation || current.metadata_location != head.metadata_location {
            return Err(stale());
        }
        if data.get("offset").is_none() {
            data["offset"] = json!(offset);
        }
        data["metadata_location"] = json!(generation);
        data["snapshot_id"] = json!(selection.snapshot["snapshot-id"].to_string());
        present::stringify_integers(&mut data);
        let bytes = serde_json::to_vec(&data).map_err(failed)?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(bounds());
        }
        Ok(response(200, bytes))
    }
}

impl Selection<'_> {
    async fn record(&self, location: &FileLocation) -> Result<FileRecord> {
        if location.table() != self.table {
            return Err(missing_reference());
        }
        self.service
            .files
            .load(self.context, location)
            .await
            .map_err(failed)?
            .ok_or_else(missing_reference)
    }
}

fn failed(error: impl std::fmt::Display) -> IcebergErrorResponse {
    tracing::debug!(%error, "metadata inspection failed");
    IcebergErrorResponse::new(
        422,
        "InspectionException",
        "Metadata is malformed, unsupported or exceeds parser limits",
    )
}
fn missing_reference() -> IcebergErrorResponse {
    IcebergErrorResponse::new(
        404,
        "NoSuchReferenceException",
        "Selected snapshot reference or file is unavailable",
    )
}
fn stale() -> IcebergErrorResponse {
    IcebergErrorResponse::new(
        409,
        "StaleMetadataException",
        "Table metadata changed; refresh the table before continuing inspection",
    )
}
fn bounds() -> IcebergErrorResponse {
    IcebergErrorResponse::new(
        413,
        "InspectionLimitException",
        "Inspection exceeds the bounded metadata budget",
    )
}
