use serde_json::Value;

use super::{raw, MetadataObject, State, TableUpdate};
use crate::table::TableMetadataError as Error;

impl State {
    pub(super) fn auxiliary_update(&mut self, update: &TableUpdate) -> Result<bool, Error> {
        match update {
            TableUpdate::SetStatistics { statistics, .. } => self.set_auxiliary("statistics", statistics)?,
            TableUpdate::SetPartitionStatistics { partition_statistics } => {
                self.set_auxiliary("partition-statistics", partition_statistics)?;
            }
            TableUpdate::RemoveStatistics { snapshot_id } => {
                self.remove_auxiliary("statistics", &Value::from(*snapshot_id))?;
            }
            TableUpdate::RemovePartitionStatistics { snapshot_id } => {
                self.remove_auxiliary("partition-statistics", &Value::from(*snapshot_id))?;
            }
            TableUpdate::AddEncryptionKey { encryption_key } => {
                self.set_auxiliary("encryption-keys", encryption_key)?;
            }
            TableUpdate::RemoveEncryptionKey { key_id } => {
                self.remove_auxiliary("encryption-keys", &Value::from(key_id.clone()))?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn set_auxiliary(&mut self, collection: &'static str, object: &MetadataObject) -> Result<(), Error> {
        let raw = self.payload(object)?;
        let value: Value = serde_json::from_str(raw.get())?;
        let mut validation = serde_json::json!({});
        validation[collection] = Value::Array(vec![value.clone()]);
        crate::table::validate_auxiliary_definition(&validation, &self.source_head, self.limits)?;
        let id_name = id_name(collection);
        let id = &value[id_name];
        if id.is_null() {
            return Err(Error::Field(id_name));
        }
        let entries = self.raw.array(collection)?;
        let mut retained = Vec::new();
        for entry in entries {
            let prior: Value = serde_json::from_str(entry.get())?;
            if prior[id_name] != *id {
                retained.push(entry);
            } else if collection == "encryption-keys" {
                return Ok(());
            }
        }
        retained.push(raw);
        self.raw.set(collection, &retained)?;
        self.validate_payloads()
    }

    pub(super) fn remove_auxiliary(&mut self, collection: &'static str, id: &Value) -> Result<(), Error> {
        let mut retained: raw::Array = Vec::new();
        for entry in self.raw.array(collection)? {
            let value: Value = serde_json::from_str(entry.get())?;
            if value[id_name(collection)] != *id {
                retained.push(entry);
            }
        }
        self.raw.set(collection, &retained)
    }

    pub(super) fn validate_payloads(&mut self) -> Result<(), Error> {
        let bytes = self.raw.finish()?;
        let root = crate::table::decode_bounded_json(&bytes, self.limits)?;
        let mut head = self.source_head.clone();
        head.format_version = self.raw.get("format-version")?;
        head.table_uuid = root["table-uuid"]
            .as_str()
            .map(uuid::Uuid::parse_str)
            .transpose()
            .map_err(|_| Error::Field("table-uuid"))?;
        crate::table::validate_metadata_payloads(&root, &head, self.limits)
    }
}

fn id_name(collection: &str) -> &'static str {
    if collection == "encryption-keys" {
        "key-id"
    } else {
        "snapshot-id"
    }
}
