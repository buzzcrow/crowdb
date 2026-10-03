use crowdb_access_iceberg::manifest::{
    ManifestContext, ManifestListEntry, ManifestScalarEntry, PartitionValue,
};
use serde_json::{json, Value};

pub(super) fn stringify_integers(value: &mut Value) {
    match value {
        Value::Number(number) if number.is_i64() || number.is_u64() => {
            *value = Value::String(number.to_string());
        }
        Value::Array(items) => items.iter_mut().for_each(stringify_integers),
        Value::Object(fields) => fields.values_mut().for_each(stringify_integers),
        _ => {}
    }
}
pub(super) fn manifest(entry: &ManifestListEntry) -> Value {
    json!({"location":entry.location.to_string(),"size":entry.length,"content":format!("{:?}",entry.content),
        "partition_spec_id":entry.partition_spec_id,"snapshot_id":entry.added_snapshot_id,
        "sequence":entry.sequence,"min_sequence":entry.min_sequence,"file_counts":entry.file_counts,
        "row_counts":entry.row_counts,"first_row_id":entry.first_row_id,
        "partitions":entry.partitions.as_ref().map(|values|values.iter().map(|value|format!("{value:?}")).collect::<Vec<_>>())})
}
pub(super) fn entry(entry: &ManifestScalarEntry, context: &ManifestContext) -> Value {
    let m = &entry.file.metrics;
    let mut ids = std::collections::BTreeSet::new();
    for map in [
        &m.column_sizes,
        &m.value_counts,
        &m.null_value_counts,
        &m.nan_value_counts,
    ]
    .into_iter()
    .flatten()
    {
        ids.extend(map.keys().copied());
    }
    for map in [&m.lower_bounds, &m.upper_bounds].into_iter().flatten() {
        ids.extend(map.keys().copied());
    }
    let metrics: Vec<_> = ids.into_iter().map(|id| {
        let field = context.retained_field(id);
        let kind = field.and_then(|f|f.primitive.as_ref()).map(|v|format!("{v:?}"));
        let bound = |map: &Option<std::collections::BTreeMap<i32,Vec<u8>>>| map.as_ref().and_then(|v|v.get(&id)).map(|bytes| metric_bound(bytes,kind.as_deref()));
        json!({"id":id,"name":field.map(|f|f.name.as_str()),"column_size":m.column_sizes.as_ref().and_then(|v|v.get(&id)),"values":m.value_counts.as_ref().and_then(|v|v.get(&id)),"nulls":m.null_value_counts.as_ref().and_then(|v|v.get(&id)),"nans":m.nan_value_counts.as_ref().and_then(|v|v.get(&id)),"lower":bound(&m.lower_bounds),"upper":bound(&m.upper_bounds)})
    }).collect();
    json!({"location":entry.file.location.to_string(),"size":entry.file.length,"format":format!("{:?}",entry.file.format),
        "content":format!("{:?}",entry.entry.content),"status":format!("{:?}",entry.entry.status),"records":entry.entry.record_count,
        "snapshot_id":entry.inherited.snapshot_id,"data_sequence":entry.inherited.data_sequence,"file_sequence":entry.inherited.file_sequence,
        "sequence_inherited":entry.entry.data_sequence.is_none(),"first_row_id":entry.inherited.first_row_id,"equality_ids":entry.file.equality_ids,
        "sort_order_id":entry.file.sort_order_id,"referenced_data_file":entry.file.referenced_data_file.as_ref().map(ToString::to_string),
        "partition":entry.file.partition.as_ref().map(|items|items.iter().map(|(id,value)|json!({"id":id,"name":context.partitions().iter().find(|f|f.id==*id).map(|f|f.name.as_str()),"value":partition(value)})).collect::<Vec<_>>()),"metrics":metrics})
}
fn metric_bound(bytes: &[u8], kind: Option<&str>) -> String {
    match (kind, bytes.len()) {
        (Some("Int" | "Date"), 4) => i32::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some("Long" | "Time" | "Timestamp" | "Timestamptz"), 8) => {
            i64::from_le_bytes(bytes.try_into().unwrap()).to_string()
        }
        (Some("Float"), 4) => f32::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some("Double"), 8) => f64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some("String"), _) => String::from_utf8_lossy(bytes).into_owned(),
        _ => format!("hex:{}", hex::encode(bytes)),
    }
}
fn partition(value: &PartitionValue) -> Value {
    match value {
        PartitionValue::Null => Value::Null,
        PartitionValue::Boolean(v) => json!(v),
        PartitionValue::Int(v) => json!(v),
        PartitionValue::Long(v) => json!(v),
        PartitionValue::Float(v) => json!(f32::from_bits(*v)),
        PartitionValue::Double(v) => json!(f64::from_bits(*v)),
        PartitionValue::String(v) => json!(v),
        PartitionValue::Bytes(v) | PartitionValue::Opaque(v) => json!(format!("hex:{}", hex::encode(v))),
    }
}
