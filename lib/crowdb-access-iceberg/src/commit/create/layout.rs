use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Value};

use crate::table::{TableMetadataError as Error, TableMetadataLimits};

pub(super) fn prepare(
    request: &Value,
    ids: &BTreeMap<i32, i32>,
    limits: TableMetadataLimits,
) -> Result<(Value, Value, i32), Error> {
    let mut spec = request
        .get("partition-spec")
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or_else(|| json!({"fields":[]}));
    if let Some(value) = spec.get("spec-id") {
        nonnegative(value, "spec-id")?;
    }
    spec["spec-id"] = json!(0);
    let fields = spec["fields"]
        .as_array_mut()
        .ok_or(Error::Field("partition-spec"))?;
    if fields.len() > limits.collection_entries {
        return Err(Error::Bounds);
    }
    let mut last_partition = 999_i32;
    let mut field_ids = BTreeSet::new();
    for field in fields {
        if let Some(value) = field.get("field-id") {
            let field_id = nonnegative(value, "field-id")?;
            if field_id == 0 || !field_ids.insert(field_id) {
                return Err(Error::Field("field-id"));
            }
        }
        source(field, ids)?;
        last_partition = last_partition.checked_add(1).ok_or(Error::Bounds)?;
        field["field-id"] = json!(last_partition);
    }
    let mut order = request
        .get("write-order")
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or_else(|| json!({"fields":[]}));
    let requested_order = order
        .get("order-id")
        .map(|value| nonnegative(value, "order-id"))
        .transpose()?;
    let fields = order["fields"]
        .as_array_mut()
        .ok_or(Error::Field("write-order"))?;
    if fields.len() > limits.collection_entries {
        return Err(Error::Bounds);
    }
    let order_id = i32::from(!fields.is_empty());
    if requested_order.is_some_and(|requested| (requested == 0) != fields.is_empty()) {
        return Err(Error::Field("order-id"));
    }
    for field in fields {
        source(field, ids)?;
    }
    order["order-id"] = json!(order_id);
    Ok((spec, order, last_partition))
}

fn nonnegative(value: &Value, name: &'static str) -> Result<i32, Error> {
    value
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value >= 0)
        .ok_or(Error::Field(name))
}

fn source(field: &mut Value, ids: &BTreeMap<i32, i32>) -> Result<(), Error> {
    let value = super::schema::mapped(&field["source-id"], ids)?;
    if field.get("source-ids").is_some() {
        return Err(Error::Field("source-ids"));
    }
    field["source-id"] = json!(value);
    Ok(())
}
