use serde_json::{value::RawValue, Value};

use super::{integer, raw, MetadataObject, State};
use crate::{
    manifest::{ManifestContext, ManifestVersion},
    table::TableMetadataError as Error,
};

mod evolution;

impl State {
    pub(super) fn add_schema(&mut self, schema: &MetadataObject) -> Result<(), Error> {
        let payload = self.payload(schema)?;
        let mut object: raw::Object = serde_json::from_str(payload.get())?;
        object.insert("schema-id".into(), raw::encode(&0, self.raw.limit)?);
        let normalized = raw::encode(&object, self.raw.limit)?;
        let incoming: Value = serde_json::from_str(normalized.get())?;
        let context = self.schema_context(&incoming)?;
        let mut schemas = self.raw.array("schemas")?;
        let mut next_id = 0_i32;
        for existing in &schemas {
            let mut value: Value = serde_json::from_str(existing.get())?;
            let id = integer(&value, "schema-id")?;
            next_id = next_id.max(id.checked_add(1).ok_or(Error::Field("schema-id"))?);
            value["schema-id"] = Value::from(0);
            if same_schema(&value, &incoming) {
                self.last_schema = self.added_schemas.contains(&id).then_some(id);
                return Ok(());
            }
        }
        let current = self.current_schema()?;
        let prior: Value = serde_json::from_str(current.get())?;
        let prior_context = self.schema_context(&prior)?;
        let last: i32 = self.raw.get("last-column-id")?;
        let specs: Value = self.raw.get("partition-specs")?;
        evolution::validate(&prior, &prior_context, &incoming, &context, last, &specs)?;
        let highest = context.fields().map(|(id, _)| *id).max().unwrap_or(0).max(last);
        object.insert("schema-id".into(), raw::encode(&next_id, self.raw.limit)?);
        schemas.push(raw::encode(&object, self.raw.limit)?);
        self.raw.set("schemas", &schemas)?;
        self.raw.set("last-column-id", &highest)?;
        self.added_schemas.insert(next_id);
        self.last_schema = Some(next_id);
        Ok(())
    }

    pub(super) fn select_schema(&mut self, requested: i32) -> Result<(), Error> {
        let selected = if requested == -1 {
            self.last_schema.ok_or(Error::Field("schema-id"))?
        } else {
            requested
        };
        let schemas = self.raw.array("schemas")?;
        let found = schemas.iter().try_fold(false, |found, raw| {
            let value: Value = serde_json::from_str(raw.get())?;
            Ok::<_, Error>(found || integer(&value, "schema-id")? == selected)
        })?;
        if !found {
            return Err(Error::Field("schema-id"));
        }
        self.raw.set("current-schema-id", &selected)
    }

    pub(super) fn remove_schemas(&mut self, ids: &[i32]) -> Result<(), Error> {
        self.raw.charge(ids.len().saturating_mul(4))?;
        let ids: std::collections::BTreeSet<_> = ids.iter().copied().collect();
        if ids.contains(&self.raw.get::<i32>("current-schema-id")?) {
            return Err(Error::Field("current-schema-id"));
        }
        let mut retained = Vec::new();
        for raw in self.raw.array("schemas")? {
            let value: Value = serde_json::from_str(raw.get())?;
            if !ids.contains(&integer(&value, "schema-id")?) {
                retained.push(raw);
            }
        }
        self.raw.set("schemas", &retained)
    }

    pub(super) fn current_schema(&mut self) -> Result<Box<RawValue>, Error> {
        let selected: i32 = self.raw.get("current-schema-id")?;
        for raw in self.raw.array("schemas")? {
            let value: Value = serde_json::from_str(raw.get())?;
            if integer(&value, "schema-id")? == selected {
                return Ok(raw);
            }
        }
        Err(Error::Field("current-schema-id"))
    }

    pub(super) fn schema_context(&mut self, schema: &Value) -> Result<ManifestContext, Error> {
        let version = match self.raw.get::<u8>("format-version")? {
            1 => ManifestVersion::V1,
            2 => ManifestVersion::V2,
            3 => ManifestVersion::V3,
            _ => return Err(Error::Field("format-version")),
        };
        let encoded = raw::encode(schema, self.raw.limit)?;
        self.raw.charge(encoded.get().len())?;
        crate::table::validate_schema_definition(schema, version, self.limits.values)?;
        let context = ManifestContext::parse(
            version,
            integer(schema, "schema-id")?,
            0,
            encoded.get().as_bytes(),
            b"[]",
        )
        .map_err(|error| match error {
            crate::manifest::ManifestContextError::Bounds => Error::Bounds,
            _ => Error::Field("schemas"),
        })?;
        if version != ManifestVersion::V3
            && context
                .fields()
                .any(|(_, field)| field.initial_default == crate::manifest::SchemaDefault::NonNull)
        {
            return Err(Error::Field("initial-default"));
        }
        Ok(context)
    }
}

fn same_schema(first: &Value, second: &Value) -> bool {
    let identifiers = |value: &Value| {
        value["identifier-field-ids"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_i64)
            .collect::<std::collections::BTreeSet<_>>()
    };
    first["type"] == second["type"]
        && first["fields"] == second["fields"]
        && identifiers(first) == identifiers(second)
}
