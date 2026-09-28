use std::collections::BTreeMap;

use crate::file::FileLocation;

use super::{ManifestListEntry, ManifestListError as Error, ManifestVersion};

#[derive(Clone, Debug)]
pub struct ManifestListSelection {
    pub location: FileLocation,
    pub table_version: ManifestVersion,
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence: i64,
    pub first_row_id: Option<i64>,
    pub added_rows: Option<i64>,
}

impl ManifestListSelection {
    pub(super) fn validate(&self) -> Result<(), Error> {
        if self.sequence < 0
            || self.parent_snapshot_id == Some(self.snapshot_id)
            || (self.table_version == ManifestVersion::V1 && self.sequence != 0)
        {
            return Err(Error::Field);
        }
        match (self.table_version, self.first_row_id, self.added_rows) {
            (ManifestVersion::V3, Some(first), Some(rows))
                if self.sequence > 0 && first >= 0 && rows >= 0 && first.checked_add(rows).is_some() => {}
            (_, None, None) => {}
            _ => return Err(Error::Field),
        }
        Ok(())
    }

    pub(super) fn validate_metadata(&self, metadata: &BTreeMap<String, Vec<u8>>) -> Result<(), Error> {
        if let Some(version) = metadata.get("format-version") {
            let valid = match version.as_slice() {
                b"1" => self.sequence == 0 && self.first_row_id.is_none(),
                b"2" => self.table_version != ManifestVersion::V1 && self.first_row_id.is_none(),
                b"3" => self.table_version == ManifestVersion::V3 && self.first_row_id.is_some(),
                _ => false,
            };
            if !valid {
                return Err(Error::Field);
            }
        }
        for (key, expected) in [
            ("snapshot-id", Some(self.snapshot_id)),
            ("sequence-number", Some(self.sequence)),
            ("first-row-id", self.first_row_id),
        ] {
            if let Some(value) = metadata.get(key) {
                if Some(number(value)?) != expected {
                    return Err(Error::Field);
                }
            }
        }
        if let Some(value) = metadata.get("parent-snapshot-id") {
            let actual = if value == b"null" {
                None
            } else {
                Some(number(value)?)
            };
            if actual != self.parent_snapshot_id {
                return Err(Error::Field);
            }
        }
        Ok(())
    }

    pub(super) fn validate_entry(&self, entry: &ManifestListEntry) -> Result<(), Error> {
        if entry.sequence > self.sequence
            || (entry.added_snapshot_id == self.snapshot_id && entry.sequence != self.sequence)
            || (self.first_row_id.is_none() && entry.first_row_id.is_some())
        {
            return Err(Error::Field);
        }
        Ok(())
    }
}

fn number(value: &[u8]) -> Result<i64, Error> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or(Error::Field)
}
