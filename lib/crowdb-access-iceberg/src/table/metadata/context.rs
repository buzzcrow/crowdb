use serde_json::Value;

use super::{TableMetadataDocument, TableMetadataError as Error};
use crate::manifest::{ManifestContext, ManifestContextError, ManifestVersion};

impl TableMetadataDocument {
    /// Builds a manifest context exclusively from this selected metadata generation.
    /// Historical IDs must be retained here; missing history must be recovered from
    /// separately verified prior authority, never from untrusted manifest headers.
    /// The caller must still fence this document's generation before publication.
    /// # Errors
    /// Rejects absent IDs, incompatible schema/spec pairs and excessive history/work.
    pub fn manifest_context(
        &self,
        schema_id: i32,
        spec_id: i32,
        history: &[i32],
        work_limit: usize,
    ) -> Result<ManifestContext, Error> {
        if history.len() > 16 || work_limit == 0 || work_limit > 1_000_000 {
            return Err(Error::Bounds);
        }
        if schema_id < 0 || spec_id < 0 || history.iter().any(|identity| *identity < 0) {
            return Err(Error::Field("manifest-context"));
        }
        let mut work = work_limit;
        let schema = self.find_definition("schemas", "schema-id", "schema", schema_id, &mut work)?;
        let spec =
            self.find_definition("partition-specs", "spec-id", "partition-spec", spec_id, &mut work)?;
        let fields = if spec.is_array() { spec } else { &spec["fields"] };
        let context = self.parse_context(schema_id, spec_id, schema, fields, &mut work)?;
        let mut historical = Vec::new();
        for identity in history {
            let schema = self.find_definition("schemas", "schema-id", "schema", *identity, &mut work)?;
            historical.push(self.parse_context(
                *identity,
                0,
                schema,
                &Value::Array(Vec::new()),
                &mut work,
            )?);
        }
        context
            .with_schema_history(&historical)
            .map_err(|error| context_error(&error))
    }

    fn find_definition(
        &self,
        collection: &'static str,
        identity: &'static str,
        legacy: &'static str,
        selected: i32,
        work: &mut usize,
    ) -> Result<&Value, Error> {
        let values = match self.fields().get(collection) {
            Some(Value::Array(values)) => values.as_slice(),
            None => self
                .fields()
                .get(legacy)
                .map_or(&[] as &[Value], std::slice::from_ref),
            _ => return Err(Error::Field(collection)),
        };
        for value in values {
            charge(work)?;
            if value.get(identity).and_then(Value::as_i64).unwrap_or(0) == i64::from(selected) {
                return Ok(value);
            }
        }
        Err(Error::Field(identity))
    }

    fn parse_context(
        &self,
        schema_id: i32,
        spec_id: i32,
        schema: &Value,
        partitions: &Value,
        work: &mut usize,
    ) -> Result<ManifestContext, Error> {
        charge_value(schema, work)?;
        charge_value(partitions, work)?;
        let version = match self.selected_head().format_version {
            1 => ManifestVersion::V1,
            2 => ManifestVersion::V2,
            3 => ManifestVersion::V3,
            _ => return Err(Error::Field("format-version")),
        };
        ManifestContext::parse(
            version,
            schema_id,
            spec_id,
            &serde_json::to_vec(schema)?,
            &serde_json::to_vec(partitions)?,
        )
        .map_err(|error| context_error(&error))
    }
}

fn context_error(error: &ManifestContextError) -> Error {
    match error {
        ManifestContextError::Bounds => Error::Bounds,
        ManifestContextError::Invalid | ManifestContextError::Unsupported => Error::Field("manifest-context"),
    }
}

fn charge(work: &mut usize) -> Result<(), Error> {
    *work = work.checked_sub(1).ok_or(Error::Bounds)?;
    Ok(())
}

fn charge_value(value: &Value, work: &mut usize) -> Result<(), Error> {
    charge(work)?;
    match value {
        Value::Array(values) => {
            for value in values {
                charge_value(value, work)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                charge_value(value, work)?;
            }
        }
        _ => {}
    }
    Ok(())
}
