use serde_json::Value;

use super::{array, id, TableMetadataError as Error, TableMetadataLimits};
use crate::manifest::{ManifestContext, ManifestContextError, ManifestVersion};

pub(super) fn validate(
    root: &Value,
    version: u8,
    limits: TableMetadataLimits,
) -> Result<ManifestContext, Error> {
    let version = match version {
        1 => ManifestVersion::V1,
        2 => ManifestVersion::V2,
        3 => ManifestVersion::V3,
        _ => return Err(Error::Field("format-version")),
    };
    let schemas = match root.get("schemas") {
        Some(value) => array(value, "schemas", limits)?,
        None => std::slice::from_ref(&root["schema"]),
    };
    let last_column = id(&root["last-column-id"], "last-column-id")?;
    let mut work = limits.values;
    let current_id = root
        .get("current-schema-id")
        .or_else(|| {
            root.get("schemas")
                .is_none()
                .then(|| root["schema"].get("schema-id"))
                .flatten()
        })
        .map_or(Ok(0), |value| id(value, "current-schema-id"))?;
    let mut current = None;
    for schema in schemas {
        let schema_id = schema
            .get("schema-id")
            .map_or(Ok(0), |value| id(value, "schema-id"))?;
        let encoded = serde_json::to_vec(schema)?;
        let context =
            ManifestContext::parse(version, schema_id, 0, &encoded, b"[]").map_err(|error| match error {
                ManifestContextError::Bounds => Error::Bounds,
                ManifestContextError::Invalid | ManifestContextError::Unsupported => Error::Field("schemas"),
            })?;
        if context.fields().any(|(field_id, _)| *field_id > last_column) {
            return Err(Error::Field("last-column-id"));
        }
        super::defaults::validate(schema, version, &mut work)?;
        if context.schema_id() == current_id {
            current = Some(context);
        }
    }
    current.ok_or(Error::Field("current-schema-id"))
}
