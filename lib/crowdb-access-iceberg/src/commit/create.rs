use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    file::TableLocation,
    table::{
        decode_bounded_json, TableHead, TableLifecycle, TableMetadataDocument, TableMetadataError as Error,
        TableMetadataLimits,
    },
};

mod journal;
mod layout;
mod operation;
mod properties;
mod publisher;
mod schema;

pub use journal::TableCreateJournal;
pub use operation::{TableCreateOperation, TableCreatePhase, TableCreateStage, TableStageBinding};
pub use publisher::{StagedCommitLimits, StagedCommitRequest, TableCreationRequest, TableCreator};

#[derive(Debug)]
pub struct CreateTableRequest {
    fields: Value,
}

impl CreateTableRequest {
    /// # Errors
    /// Rejects malformed, duplicate-key or excessive requests before any durable intent is created.
    pub fn decode(bytes: &[u8], limits: TableMetadataLimits) -> Result<Self, Error> {
        let fields = decode_bounded_json(bytes, limits)?;
        let name = fields["name"].as_str().ok_or(Error::Field("name"))?;
        crate::key::NameSuffix { parent: None, name }
            .encode()
            .map_err(|_| Error::Field("name"))?;
        if !fields["schema"].is_object() {
            return Err(Error::Field("schema"));
        }
        for name in ["partition-spec", "write-order", "properties"] {
            if fields
                .get(name)
                .is_some_and(|value| !value.is_null() && !value.is_object())
            {
                return Err(Error::Field(name));
            }
        }
        if fields
            .get("stage-create")
            .is_some_and(|value| !value.is_boolean())
        {
            return Err(Error::Field("stage-create"));
        }
        if fields
            .get("location")
            .is_some_and(|value| !value.is_null() && !value.is_string())
        {
            return Err(Error::Field("location"));
        }
        Ok(Self { fields })
    }

    #[must_use]
    pub fn name(&self) -> &str {
        self.fields["name"].as_str().unwrap_or_default()
    }

    #[must_use]
    pub fn stage_create(&self) -> bool {
        self.fields["stage-create"].as_bool().unwrap_or(false)
    }
}

/// Pure initial metadata, not a namespace admission or publication proof.
#[derive(Debug)]
pub struct InitialTableMetadata {
    pub document: TableMetadataDocument,
    pub head: TableHead,
    pub stage_create: bool,
}

/// Constructs initial metadata using fresh SDK-compatible schema, partition and order IDs.
/// The caller retains the target identity and timestamp in its durable creation intent.
/// # Errors
/// Rejects invalid input, foreign locations, unsupported versions and resource exhaustion.
pub fn evaluate_table_creation(
    request: &CreateTableRequest,
    mut target: TableHead,
    timestamp_ms: i64,
    limits: TableMetadataLimits,
) -> Result<InitialTableMetadata, Error> {
    limits.validate()?;
    let encoded = super::evaluator::encode_bounded(&request.fields, limits.bytes)?;
    decode_bounded_json(encoded.get().as_bytes(), limits)?;
    validate_target(request, &target, timestamp_ms)?;
    let (version, properties) = properties::prepare(&request.fields, limits)?;
    let fresh = schema::prepare(&request.fields["schema"], version, limits)?;
    properties::validate_columns(&properties, &fresh.context)?;
    let (spec, order, last_partition) = layout::prepare(&request.fields, &fresh.ids, limits)?;
    let mut root = json!({
        "format-version": version, "table-uuid": target.table_uuid.map(|value| value.to_string()),
        "location": target.metadata_location.table().to_string().trim_end_matches('/'),
        "last-updated-ms": timestamp_ms, "last-column-id": fresh.last_id,
        "schemas": [fresh.value], "current-schema-id": 0,
        "partition-specs": [spec], "default-spec-id": 0,
        "last-partition-id": last_partition, "default-sort-order-id": order["order-id"],
        "sort-orders": [order], "properties": properties,
        "current-snapshot-id": -1, "snapshots": [], "snapshot-log": [], "metadata-log": [],
        "refs": {}, "statistics": [], "partition-statistics": []
    });
    if version == 1 {
        root["schema"] = root["schemas"][0].clone();
        root["partition-spec"] = root["partition-specs"][0]["fields"].clone();
    } else {
        root["last-sequence-number"] = json!(0);
    }
    if version == 3 {
        root["next-row-id"] = json!(0);
        root["current-snapshot-id"] = Value::Null;
    }
    let canonical = super::evaluator::encode_bounded(&root, limits.bytes)?;
    let canonical = canonical.get().as_bytes().to_vec();
    target.format_version = version;
    target.metadata_digest = Sha256::digest(&canonical).into();
    let document = TableMetadataDocument::parse(canonical, &target, limits)?;
    document.parquet_field_mapping(limits, limits.values)?;
    Ok(InitialTableMetadata {
        document,
        head: target,
        stage_create: request.stage_create(),
    })
}

fn validate_target(request: &CreateTableRequest, target: &TableHead, timestamp_ms: i64) -> Result<(), Error> {
    if target.name != request.name()
        || target.generation != 1
        || target.operation_fence != 1
        || target.name_epoch != 1
        || target.lifecycle != TableLifecycle::Ready
        || target.pending_operation.is_none()
        || target.table_uuid.is_none()
        || timestamp_ms < 0
    {
        return Err(Error::Binding);
    }
    if let Some(location) = request.fields["location"].as_str() {
        let location = format!("{}/", location.trim_end_matches('/'));
        if location
            .parse::<TableLocation>()
            .map_err(|_| Error::Field("location"))?
            != target.metadata_location.table()
        {
            return Err(Error::Field("location"));
        }
    }
    Ok(())
}
