use std::collections::BTreeMap;

use serde_json::Value;

use crate::{
    manifest::{ManifestContext, ManifestVersion, PrimitiveType},
    table::TableMetadataError as Error,
};

pub(super) fn validate(
    prior: &Value,
    before: &ManifestContext,
    candidate: &Value,
    after: &ManifestContext,
    last_id: i32,
    specs: &Value,
) -> Result<(), Error> {
    let mut old_fields = BTreeMap::new();
    let mut new_fields = BTreeMap::new();
    fields(prior, &mut old_fields)?;
    fields(candidate, &mut new_fields)?;
    for (id, field) in after.fields() {
        let Some(old) = before.field(*id) else {
            if *id <= last_id {
                return Err(Error::Field("field-id-reuse"));
            }
            if let Some(value) = new_fields.get(id) {
                if field.required
                    && needs_default(field.parent, before, after, &new_fields)
                    && (value["initial-default"].is_null() || value["write-default"].is_null())
                {
                    return Err(Error::Field("default"));
                }
            }
            continue;
        };
        if old.parent != field.parent
            || old.repeated != field.repeated
            || (old.kind != field.kind && old.primitive != Some(PrimitiveType::Unknown))
        {
            return Err(Error::Field("field-parent"));
        }
        if old
            .parent
            .and_then(|id| before.field(id))
            .is_some_and(|parent| parent.kind != "struct")
            && old.name != field.name
        {
            return Err(Error::Field("collection-field-id"));
        }
        if !old.required && field.required {
            return Err(Error::Field("required"));
        }
        if !promotion(old.primitive.as_ref(), field.primitive.as_ref(), after.version()) {
            return Err(Error::Field("type-promotion"));
        }
        if old.primitive == Some(PrimitiveType::Date) && old.primitive != field.primitive {
            date_partition(*id, specs)?;
        }
        if let (Some(old), Some(new)) = (old_fields.get(id), new_fields.get(id)) {
            initial_default(old, new, after.version())?;
            collection_ids(&old["type"], &new["type"])?;
        }
    }
    Ok(())
}

fn needs_default(
    mut parent: Option<i32>,
    before: &ManifestContext,
    after: &ManifestContext,
    fields: &BTreeMap<i32, &Value>,
) -> bool {
    while let Some(id) = parent {
        if before.field(id).is_some() {
            return true;
        }
        let Some(field) = after.field(id) else {
            return true;
        };
        if matches!(field.kind, "list" | "map")
            || fields
                .get(&id)
                .is_some_and(|value| value["initial-default"].is_null())
        {
            return false;
        }
        parent = field.parent;
    }
    true
}

fn fields<'value>(schema: &'value Value, output: &mut BTreeMap<i32, &'value Value>) -> Result<(), Error> {
    match schema["type"].as_str() {
        Some("struct") => {
            for field in schema["fields"].as_array().ok_or(Error::Field("fields"))? {
                let id = super::integer(field, "id")?;
                output.insert(id, field);
                fields(&field["type"], output)?;
            }
        }
        Some("list") => fields(&schema["element"], output)?,
        Some("map") => {
            fields(&schema["key"], output)?;
            fields(&schema["value"], output)?;
        }
        _ => {}
    }
    Ok(())
}

fn collection_ids(old: &Value, new: &Value) -> Result<(), Error> {
    match old["type"].as_str() {
        Some("list") => {
            if old["element-id"] != new["element-id"] {
                return Err(Error::Field("element-id"));
            }
            collection_ids(&old["element"], &new["element"])?;
        }
        Some("map") => {
            if old["key-id"] != new["key-id"]
                || old["value-id"] != new["value-id"]
                || old["key"] != new["key"]
            {
                return Err(Error::Field("map-key"));
            }
            collection_ids(&old["value"], &new["value"])?;
        }
        _ => {}
    }
    Ok(())
}

fn promotion(old: Option<&PrimitiveType>, new: Option<&PrimitiveType>, version: ManifestVersion) -> bool {
    use PrimitiveType::{Date, Decimal, Double, Float, Int, Long, Timestamp, TimestampNs, Unknown};
    old == new
        || matches!((old, new), (Some(Int), Some(Long)) | (Some(Float), Some(Double)))
        || matches!((old, new), (Some(Decimal { precision, scale }), Some(Decimal { precision: next, scale: next_scale })) if next >= precision && next_scale == scale)
        || (version == ManifestVersion::V3
            && (old == Some(&Unknown) || matches!((old, new), (Some(Date), Some(Timestamp | TimestampNs)))))
}

fn date_partition(id: i32, specs: &Value) -> Result<(), Error> {
    for spec in specs.as_array().ok_or(Error::Field("partition-specs"))? {
        for field in spec["fields"].as_array().ok_or(Error::Field("fields"))? {
            let used = field["source-id"].as_i64() == Some(i64::from(id))
                || field["source-ids"]
                    .as_array()
                    .is_some_and(|ids| ids.contains(&Value::from(id)));
            if used
                && !matches!(
                    field["transform"].as_str(),
                    Some("year" | "month" | "day" | "void")
                )
            {
                return Err(Error::Field("partition-promotion"));
            }
        }
    }
    Ok(())
}

fn initial_default(old: &Value, new: &Value, version: ManifestVersion) -> Result<(), Error> {
    let before = &old["initial-default"];
    let after = &new["initial-default"];
    if before.is_null() && after.is_null() {
        return Ok(());
    }
    if before.is_null() || after.is_null() {
        return Err(Error::Field("initial-default"));
    }
    let equal = if old["type"] == "float" && new["type"] == "double" {
        before.to_string().parse::<f32>().ok().map(f64::from) == after.as_f64()
    } else if old["type"] == "date" && matches!(new["type"].as_str(), Some("timestamp" | "timestamp_ns")) {
        let date = before
            .as_str()
            .and_then(|text| chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").ok());
        let time = after
            .as_str()
            .and_then(|text| chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f").ok());
        date.and_then(|date| date.and_hms_opt(0, 0, 0)) == time
    } else {
        crate::table::schema_default_identity(&old["type"], before, version)?
            == crate::table::schema_default_identity(&new["type"], after, version)?
    };
    if !equal {
        return Err(Error::Field("initial-default"));
    }
    Ok(())
}
