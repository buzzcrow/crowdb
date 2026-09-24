use super::{raw, State, TableUpdate};
use crate::{file::TableLocation, table::TableMetadataError as Error};

impl State {
    pub(super) fn scalar(&mut self, update: &TableUpdate) -> Result<bool, Error> {
        match update {
            TableUpdate::AssignUuid { uuid } => {
                if self
                    .raw
                    .fields
                    .get("table-uuid")
                    .filter(|value| value.get() != "null")
                    .is_some_and(|value| {
                        serde_json::from_str::<String>(value.get())
                            .ok()
                            .and_then(|value| uuid::Uuid::parse_str(&value).ok())
                            != uuid::Uuid::parse_str(uuid).ok()
                    })
                {
                    return Err(Error::Field("table-uuid"));
                }
                self.raw.set("table-uuid", uuid)?;
            }
            TableUpdate::UpgradeFormatVersion { format_version } => self.upgrade(*format_version)?,
            TableUpdate::SetLocation { location } => {
                let before: String = if self.raw.fields.contains_key("location") {
                    self.raw.get("location")?
                } else {
                    self.source_head.metadata_location.table().to_string()
                };
                let parse = |location: &str| {
                    format!("{}/", location.trim_end_matches('/'))
                        .parse::<TableLocation>()
                        .map_err(|_| Error::Binding)
                };
                if parse(location)? != parse(&before)? {
                    return Err(Error::Binding);
                }
                self.raw.set("location", location.trim_end_matches('/'))?;
            }
            TableUpdate::SetProperties { updates } => {
                let mut properties = self.raw.object("properties")?;
                let bytes = updates.iter().try_fold(0_usize, |bytes, (name, value)| {
                    bytes
                        .checked_add(name.len())
                        .and_then(|bytes| bytes.checked_add(value.len()))
                        .filter(|bytes| *bytes <= self.raw.limit)
                        .ok_or(Error::Bounds)
                })?;
                self.raw.charge(bytes)?;
                for (name, value) in updates {
                    self.raw.charge(name.len().saturating_add(value.len()))?;
                    properties.insert(name.clone(), raw::encode(value, self.raw.limit)?);
                }
                self.raw.set("properties", &properties)?;
            }
            TableUpdate::RemoveProperties { removals } => {
                let mut properties = self.raw.object("properties")?;
                for name in removals {
                    self.raw.charge(name.len())?;
                    properties.remove(name);
                }
                self.raw.set("properties", &properties)?;
            }
            _ => return self.snapshot_update(update),
        }
        Ok(true)
    }

    fn upgrade(&mut self, target: i32) -> Result<(), Error> {
        let version: i32 = self.raw.get("format-version")?;
        if target < version || target > 3 {
            return Err(Error::Field("format-version"));
        }
        for next in (version + 1)..=target {
            if next == 2 {
                self.raw.set("last-sequence-number", &0)?;
            }
            if next == 3 {
                self.raw.set("next-row-id", &0)?;
                if self.raw.get::<Option<i64>>("current-snapshot-id")? == Some(-1) {
                    self.raw.set("current-snapshot-id", &Option::<i64>::None)?;
                }
            }
            self.raw.set("format-version", &next)?;
            self.upgrades
                .push(u8::try_from(next).map_err(|_| Error::Field("format-version"))?);
        }
        Ok(())
    }
}
