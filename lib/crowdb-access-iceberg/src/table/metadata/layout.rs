use std::collections::BTreeSet;

use serde_json::Value;

use super::{array, id, optional_array, text, TableMetadataError as Error, TableMetadataLimits};
use crate::manifest::{ManifestContext, ManifestVersion, PartitionTransform};

pub(super) fn validate(
    root: &Value,
    schema: &ManifestContext,
    limits: TableMetadataLimits,
) -> Result<(), Error> {
    if let Some(specs) = root.get("partition-specs") {
        for spec in array(specs, "partition-specs", limits)? {
            let current = spec["spec-id"] == root["default-spec-id"];
            partitions(&spec["fields"], root, schema, current, limits)?;
        }
    } else {
        partitions(&root["partition-spec"], root, schema, true, limits)?;
    }
    for order in optional_array(root, "sort-orders", limits)? {
        let order_id = id(&order["order-id"], "order-id")?;
        let fields = array(&order["fields"], "fields", limits)?;
        if (order_id == 0) != fields.is_empty() {
            return Err(Error::Field("order-id"));
        }
        for field in fields {
            if !matches!(text(&field["direction"], "direction")?, "asc" | "desc")
                || !matches!(
                    text(&field["null-order"], "null-order")?,
                    "nulls-first" | "nulls-last"
                )
            {
                return Err(Error::Field("sort-orders"));
            }
            transform(field, schema, order["order-id"] == root["default-sort-order-id"])?;
        }
    }
    Ok(())
}

fn partitions(
    fields: &Value,
    root: &Value,
    schema: &ManifestContext,
    current: bool,
    limits: TableMetadataLimits,
) -> Result<(), Error> {
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    let last = root
        .get("last-partition-id")
        .map(|value| id(value, "last-partition-id"))
        .transpose()?;
    for (index, field) in array(fields, "fields", limits)?.iter().enumerate() {
        let field_id = match field.get("field-id") {
            Some(value) => id(value, "field-id")?,
            None if schema.version() == ManifestVersion::V1 => {
                1000 + i32::try_from(index).map_err(|_| Error::Bounds)?
            }
            None => return Err(Error::Field("field-id")),
        };
        let name = text(&field["name"], "name")?;
        if field_id <= 0
            || !ids.insert(field_id)
            || name.is_empty()
            || !names.insert(name)
            || last.is_some_and(|last| field_id > last)
        {
            return Err(Error::Field("partition-specs"));
        }
        transform(field, schema, current)?;
    }
    Ok(())
}

fn transform(field: &Value, schema: &ManifestContext, current: bool) -> Result<(), Error> {
    let transform = PartitionTransform::parse(text(&field["transform"], "transform")?)
        .map_err(|_| Error::Field("transform"))?;
    let sources = match (field.get("source-id"), field.get("source-ids")) {
        (Some(value), None) => vec![id(value, "source-id")?],
        (None, Some(value)) if schema.version() == ManifestVersion::V3 => {
            let values = value
                .as_array()
                .filter(|values| values.len() >= 2 && values.len() <= 256)
                .ok_or(Error::Field("source-ids"))?;
            if !matches!(transform, PartitionTransform::Unknown(_)) {
                return Err(Error::Field("transform"));
            }
            values
                .iter()
                .map(|value| id(value, "source-ids"))
                .collect::<Result<Vec<_>, _>>()?
        }
        _ => return Err(Error::Field("source-id")),
    };
    for source in sources {
        if source <= 0 || source > 2_147_483_447 {
            return Err(Error::Field("source-id"));
        }
        if current {
            let Some(field) = schema.field(source) else {
                if transform == PartitionTransform::Void {
                    continue;
                }
                return Err(Error::Field("source-id"));
            };
            if field.repeated {
                return Err(Error::Field("source-id"));
            }
            let primitive = field.primitive.as_ref().ok_or(Error::Field("source-id"))?;
            transform
                .result(primitive)
                .map_err(|_| Error::Field("transform"))?;
        }
    }
    Ok(())
}
