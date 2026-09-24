use std::collections::BTreeSet;

use serde_json::{json, Value};

use super::{integer, raw, MetadataObject, State};
use crate::table::TableMetadataError as Error;

impl State {
    pub(super) fn add_layout(&mut self, payload: &MetadataObject, partition: bool) -> Result<(), Error> {
        let raw = self.payload(payload)?;
        let mut object: raw::Object = serde_json::from_str(raw.get())?;
        if partition {
            let raw = object.get("fields").ok_or(Error::Field("fields"))?;
            let mut fields: Vec<raw::Object> = serde_json::from_str(raw.get())?;
            let mut assigned = 999_i32;
            for field in &mut fields {
                if let Some(raw) = field.get("field-id") {
                    let id: i32 = serde_json::from_str(raw.get())?;
                    assigned = assigned.max(id);
                } else {
                    assigned = assigned.checked_add(1).ok_or(Error::Field("field-id"))?;
                    field.insert("field-id".into(), raw::encode(&assigned, self.raw.limit)?);
                }
            }
            object.insert("fields".into(), raw::encode(&fields, self.raw.limit)?);
        }
        let definition: Value = serde_json::from_str(raw::encode(&object, self.raw.limit)?.get())?;
        let fields = definition["fields"].as_array().ok_or(Error::Field("fields"))?;
        self.validate_layout(
            &definition,
            partition,
            i32::from(!partition && !fields.is_empty()),
            i32::MAX,
        )?;
        let (collection, id_name, _) = names(partition);
        let mut definitions = self.raw.array(collection)?;
        let mut highest_id = if partition { -1_i32 } else { 0 };
        let mut prior = Vec::new();
        for definition in &definitions {
            let value: Value = serde_json::from_str(definition.get())?;
            let id = integer(&value, id_name)?;
            if equivalent(&value["fields"], fields, partition) {
                self.reused_layout(id, partition);
                return Ok(());
            }
            highest_id = highest_id.max(id);
            prior.push(value);
        }
        let next = if !partition && fields.is_empty() {
            0
        } else {
            highest_id.checked_add(1).ok_or(Error::Field(id_name))?
        };
        let last: i32 = self.raw.get("last-partition-id")?;
        let highest = if partition {
            self.partition_ids(fields, &prior, last)?
        } else {
            last
        };
        object.insert(id_name.into(), raw::encode(&next, self.raw.limit)?);
        definitions.push(raw::encode(&object, self.raw.limit)?);
        self.raw.set(collection, &definitions)?;
        if partition {
            self.raw.set("last-partition-id", &highest)?;
            self.last_spec = Some(next);
            self.added_specs.insert(next);
        } else {
            self.last_order = Some(next);
            self.added_orders.insert(next);
        }
        Ok(())
    }

    fn validate_layout(
        &mut self,
        definition: &Value,
        partition: bool,
        id: i32,
        last: i32,
    ) -> Result<(), Error> {
        let mut validated = definition.clone();
        validated[names(partition).1] = Value::from(id);
        let schema = self.current_schema()?;
        let context = self.schema_context(&serde_json::from_str(schema.get())?)?;
        let root = if partition {
            json!({"partition-specs":[validated],"default-spec-id":id,"last-partition-id":last})
        } else {
            json!({"partition-specs":[],"sort-orders":[validated],"default-sort-order-id":id})
        };
        crate::table::validate_layout_definitions(&root, &context, self.limits)
    }

    fn reused_layout(&mut self, id: i32, partition: bool) {
        if partition {
            self.last_spec = self
                .last_spec
                .filter(|prior| self.added_specs.contains(prior))
                .map(|_| id);
        } else {
            self.last_order = self
                .last_order
                .filter(|prior| self.added_orders.contains(prior))
                .map(|_| id);
        }
    }

    pub(super) fn select_layout(&mut self, id: i32, partition: bool) -> Result<(), Error> {
        let (collection, id_name, selected_name) = names(partition);
        let id = if id == -1 {
            (if partition {
                self.last_spec
            } else {
                self.last_order
            })
            .ok_or(Error::Field(id_name))?
        } else {
            id
        };
        let mut found = false;
        for raw in self.raw.array(collection)? {
            let value: Value = serde_json::from_str(raw.get())?;
            found |= integer(&value, id_name)? == id;
        }
        if !found {
            return Err(Error::Field(id_name));
        }
        self.raw.set(selected_name, &id)
    }

    pub(super) fn remove_specs(&mut self, ids: &[i32]) -> Result<(), Error> {
        self.raw.charge(ids.len().saturating_mul(4))?;
        let ids: BTreeSet<_> = ids.iter().copied().collect();
        if ids.contains(&self.raw.get::<i32>("default-spec-id")?) {
            return Err(Error::Field("default-spec-id"));
        }
        let mut retained = Vec::new();
        for raw in self.raw.array("partition-specs")? {
            let value: Value = serde_json::from_str(raw.get())?;
            if !ids.contains(&integer(&value, "spec-id")?) {
                retained.push(raw);
            }
        }
        self.raw.set("partition-specs", &retained)
    }

    fn partition_ids(&mut self, fields: &[Value], specs: &[Value], last: i32) -> Result<i32, Error> {
        let version: u8 = self.raw.get("format-version")?;
        let mut highest = last;
        for (index, field) in fields.iter().enumerate() {
            let id = integer(field, "field-id")?;
            if version == 1 {
                if i32::try_from(index)
                    .ok()
                    .and_then(|index| index.checked_add(1000))
                    != Some(id)
                {
                    return Err(Error::Field("field-id"));
                }
            } else {
                let mut known = false;
                for spec in specs {
                    for prior in spec["fields"].as_array().ok_or(Error::Field("fields"))? {
                        self.raw.charge(1)?;
                        if integer(prior, "field-id")? == id {
                            if prior["source-id"] != field["source-id"]
                                || prior["source-ids"] != field["source-ids"]
                                || prior["transform"] != field["transform"]
                            {
                                return Err(Error::Field("partition-field-id-reuse"));
                            }
                            known = true;
                        }
                        if prior["source-id"] == field["source-id"]
                            && prior["source-ids"] == field["source-ids"]
                            && prior["transform"] == field["transform"]
                            && prior["name"] == field["name"]
                            && integer(prior, "field-id")? != id
                        {
                            return Err(Error::Field("partition-field-id-reuse"));
                        }
                    }
                }
                if !known && id <= last {
                    return Err(Error::Field("partition-field-id-reuse"));
                }
            }
            highest = highest.max(id);
        }
        Ok(highest)
    }
}

fn names(partition: bool) -> (&'static str, &'static str, &'static str) {
    if partition {
        ("partition-specs", "spec-id", "default-spec-id")
    } else {
        ("sort-orders", "order-id", "default-sort-order-id")
    }
}

fn equivalent(existing: &Value, fields: &[Value], partition: bool) -> bool {
    existing.as_array().is_some_and(|before| {
        before.len() == fields.len()
            && before.iter().zip(fields).all(|(before, after)| {
                before["source-id"] == after["source-id"]
                    && before["source-ids"] == after["source-ids"]
                    && before["transform"] == after["transform"]
                    && if partition {
                        before["name"] == after["name"]
                    } else {
                        before["direction"] == after["direction"]
                            && before["null-order"] == after["null-order"]
                    }
            })
    })
}
