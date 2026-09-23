use std::collections::BTreeSet;

use serde_json::Value;

use super::{array, id, json, optional_array, text, TableMetadataError as Error, TableMetadataLimits};

pub(super) fn validate(root: &Value, limits: TableMetadataLimits) -> Result<(), Error> {
    let Some(mapping) = root["properties"].get("schema.name-mapping.default") else {
        return Ok(());
    };
    let encoded = text(mapping, "schema.name-mapping.default")?;
    let value = json::parse(encoded.as_bytes(), limits)?;
    fields(&value, &mut BTreeSet::new(), limits)
}

fn fields(value: &Value, ids: &mut BTreeSet<i32>, limits: TableMetadataLimits) -> Result<(), Error> {
    let mut names = BTreeSet::new();
    for field in array(value, "name-mapping", limits)? {
        if !field.is_object() {
            return Err(Error::Field("name-mapping"));
        }
        let field_id = field.get("field-id").filter(|value| !value.is_null());
        if let Some(value) = field_id {
            let field_id = id(value, "field-id")?;
            if field_id == 0 || field_id > 2_147_483_447 || !ids.insert(field_id) {
                return Err(Error::Field("name-mapping"));
            }
        }
        let mut aliases = BTreeSet::new();
        for name in optional_array(field, "names", limits)? {
            let name = text(name, "names")?;
            if aliases.insert(name) && field_id.is_some() && !names.insert(name) {
                return Err(Error::Field("name-mapping"));
            }
        }
        if let Some(children) = field.get("fields") {
            fields(children, ids, limits)?;
        }
    }
    Ok(())
}
