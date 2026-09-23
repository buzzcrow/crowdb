use std::collections::BTreeSet;

use serde_json::{json, Value};

use super::{raw, MetadataObject, State, TableUpdate};
use crate::{commit::SnapshotRefType, table::TableMetadataError as Error};

impl State {
    pub(super) fn snapshot_update(&mut self, update: &TableUpdate) -> Result<bool, Error> {
        match update {
            TableUpdate::AddSnapshot { snapshot } => self.add_snapshot(snapshot)?,
            TableUpdate::SetSnapshotRef {
                ref_name,
                kind,
                snapshot_id,
                min_snapshots_to_keep,
                max_snapshot_age_ms,
                max_ref_age_ms,
            } => {
                let mut reference = json!({"snapshot-id":snapshot_id,"type":match kind {
                    SnapshotRefType::Branch => "branch", SnapshotRefType::Tag => "tag",
                }});
                for (name, value) in [
                    ("min-snapshots-to-keep", min_snapshots_to_keep.map(i64::from)),
                    ("max-snapshot-age-ms", *max_snapshot_age_ms),
                    ("max-ref-age-ms", *max_ref_age_ms),
                ] {
                    if let Some(value) = value {
                        reference[name] = Value::from(value);
                    }
                }
                self.set_ref(ref_name, *snapshot_id, &reference)?;
            }
            TableUpdate::RemoveSnapshotRef { ref_name } => self.remove_ref(ref_name)?,
            TableUpdate::RemoveSnapshots { snapshot_ids } => self.remove_snapshots(snapshot_ids)?,
            _ => return self.auxiliary_update(update),
        }
        Ok(true)
    }

    fn add_snapshot(&mut self, snapshot: &MetadataObject) -> Result<(), Error> {
        let payload = self.payload(snapshot)?;
        let value: Value = serde_json::from_str(payload.get())?;
        let id = number(&value, "snapshot-id")?;
        let mut snapshots = self.raw.array("snapshots")?;
        for raw in &snapshots {
            let prior: Value = serde_json::from_str(raw.get())?;
            if number(&prior, "snapshot-id")? == id {
                return Err(Error::Field("snapshot-id"));
            }
        }
        let version: u8 = self.raw.get("format-version")?;
        if version > 1 {
            let sequence = number(&value, "sequence-number")?;
            if sequence <= self.raw.get::<i64>("last-sequence-number")? || !value["manifest-list"].is_string()
            {
                return Err(Error::Field("sequence-number"));
            }
            self.raw.set("last-sequence-number", &sequence)?;
        }
        if version == 3 {
            let first = number(&value, "first-row-id")?;
            let added = number(&value, "added-rows")?;
            let next: i64 = self.raw.get("next-row-id")?;
            if first != next || added < 0 {
                return Err(Error::Field("first-row-id"));
            }
            let next = first.checked_add(added).ok_or(Error::Field("next-row-id"))?;
            self.raw.set("next-row-id", &next)?;
        }
        snapshots.push(payload);
        self.raw.set("snapshots", &snapshots)?;
        self.validate_payloads()?;
        self.added_snapshots.insert(id);
        Ok(())
    }

    fn refs(&mut self) -> Result<raw::Object, Error> {
        let mut refs = self.raw.object("refs")?;
        if !refs.contains_key("main") {
            let current: Option<i64> = self.raw.get("current-snapshot-id")?;
            if let Some(current) = current.filter(|id| *id != -1) {
                refs.insert(
                    "main".into(),
                    raw::encode(&json!({"snapshot-id":current,"type":"branch"}), self.raw.limit)?,
                );
            }
        }
        Ok(refs)
    }

    fn set_ref(&mut self, name: &str, id: i64, reference: &Value) -> Result<(), Error> {
        self.raw.charge(name.len())?;
        if name.len() > self.raw.limit {
            return Err(Error::Bounds);
        }
        let mut refs = self.refs()?;
        let mut timestamp = None;
        for raw in self.raw.array("snapshots")? {
            let value: Value = serde_json::from_str(raw.get())?;
            if number(&value, "snapshot-id")? == id {
                timestamp = Some(number(&value, "timestamp-ms")?);
            }
        }
        let timestamp = timestamp.ok_or(Error::Field("snapshot-id"))?;
        if let Some(existing) = refs.get(name) {
            if serde_json::from_str::<Value>(existing.get())? == *reference {
                return Ok(());
            }
        }
        if name == "main" {
            self.raw.set("current-snapshot-id", &id)?;
            let mut log = self.raw.array("snapshot-log")?;
            let timestamp = if self.added_snapshots.contains(&id) {
                timestamp
            } else {
                self.timestamp_ms
            };
            log.push(raw::encode(
                &json!({"timestamp-ms":timestamp,"snapshot-id":id}),
                self.raw.limit,
            )?);
            self.raw.set("snapshot-log", &log)?;
            self.changed_main.insert(id);
        }
        refs.insert(name.into(), raw::encode(reference, self.raw.limit)?);
        self.raw.set("refs", &refs)
    }

    fn remove_ref(&mut self, name: &str) -> Result<(), Error> {
        self.raw.charge(name.len())?;
        let mut refs = self.refs()?;
        refs.remove(name);
        if name == "main" {
            self.raw.set("current-snapshot-id", &-1)?;
        }
        self.raw.set("refs", &refs)
    }

    fn remove_snapshots(&mut self, ids: &[i64]) -> Result<(), Error> {
        self.raw.charge(ids.len().saturating_mul(8))?;
        let ids: BTreeSet<_> = ids.iter().copied().collect();
        let mut retained = Vec::new();
        let mut removed = BTreeSet::new();
        for raw in self.raw.array("snapshots")? {
            let value: Value = serde_json::from_str(raw.get())?;
            let id = number(&value, "snapshot-id")?;
            if ids.contains(&id) {
                removed.insert(id);
            } else {
                retained.push(raw);
            }
        }
        for raw in &self.refs()? {
            let value: Value = serde_json::from_str(raw.1.get())?;
            if removed.contains(&number(&value, "snapshot-id")?) {
                self.remove_ref(raw.0)?;
            }
        }
        for id in &removed {
            self.remove_auxiliary("statistics", &Value::from(*id))?;
            self.remove_auxiliary("partition-statistics", &Value::from(*id))?;
        }
        self.removed_snapshots |= !removed.is_empty();
        self.raw.set("snapshots", &retained)
    }

    pub(super) fn snapshot_log(&mut self) -> Result<(), Error> {
        if !self.removed_snapshots && self.changed_main.is_empty() {
            return Ok(());
        }
        let current: Option<i64> = self.raw.get("current-snapshot-id")?;
        let mut ids = BTreeSet::new();
        for raw in self.raw.array("snapshots")? {
            let value: Value = serde_json::from_str(raw.get())?;
            ids.insert(number(&value, "snapshot-id")?);
        }
        let mut retained = Vec::new();
        for raw in self.raw.array("snapshot-log")? {
            let value: Value = serde_json::from_str(raw.get())?;
            let id = number(&value, "snapshot-id")?;
            if !ids.contains(&id) && self.removed_snapshots {
                retained.clear();
            } else if !(self.added_snapshots.contains(&id)
                && self.changed_main.contains(&id)
                && current != Some(id))
            {
                retained.push(raw);
            }
        }
        self.raw.set("snapshot-log", &retained)
    }
}

fn number(value: &Value, name: &'static str) -> Result<i64, Error> {
    value[name].as_i64().ok_or(Error::Field(name))
}
