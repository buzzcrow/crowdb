use std::collections::BTreeMap;

use serde_json::Value;

use super::{CandidateFileSource, Error};
use crate::table::TableMetadataDocument;

pub(super) struct Statistics<'document> {
    prior: Option<&'document TableMetadataDocument>,
    entries: BTreeMap<i64, (&'document str, u64)>,
}

impl<'document> Statistics<'document> {
    pub(super) fn new(
        source: &CandidateFileSource,
        prior: Option<&'document TableMetadataDocument>,
        files: usize,
        work: &mut usize,
    ) -> Result<Self, Error> {
        let mut entries = BTreeMap::new();
        if let Some(prior) = prior {
            if !source
                .fence
                .prior()
                .is_some_and(|source| source.selected().head == *prior.selected_head())
            {
                return Err(Error::Binding);
            }
            if let Some(values) = prior.fields().get("partition-statistics") {
                let values = values.as_array().ok_or(Error::Binding)?;
                if values.len() > files {
                    return Err(Error::Bounds);
                }
                for entry in values {
                    let id = entry["snapshot-id"].as_i64().ok_or(Error::Binding)?;
                    let path = entry["statistics-path"].as_str().ok_or(Error::Binding)?;
                    let length = entry["file-size-in-bytes"].as_u64().ok_or(Error::Binding)?;
                    charge(work, path.len() + 1)?;
                    if entries.insert(id, (path, length)).is_some() {
                        return Err(Error::Binding);
                    }
                }
            }
        }
        Ok(Self { prior, entries })
    }

    pub(super) fn contains(
        &self,
        candidate: &TableMetadataDocument,
        entry: &Value,
        work: &mut usize,
    ) -> Result<bool, Error> {
        let Some(prior) = self.prior else {
            return Ok(false);
        };
        let id = entry["snapshot-id"].as_i64().ok_or(Error::Binding)?;
        let Some((path, length)) = self.entries.get(&id) else {
            return Ok(false);
        };
        let next_path = entry["statistics-path"].as_str().ok_or(Error::Binding)?;
        charge(work, path.len() + next_path.len() + 1)?;
        if next_path != *path || entry["file-size-in-bytes"].as_u64() != Some(*length) {
            return Ok(false);
        }
        let previous = prior.snapshots().get(&id).ok_or(Error::Binding)?;
        let current = candidate.snapshots().get(&id).ok_or(Error::Binding)?;
        for snapshot in [previous, current] {
            charge(work, 1)?;
            for location in snapshot.manifest_list.iter().chain(&snapshot.manifests) {
                charge(work, location.relative_key().len() + 1)?;
            }
        }
        Ok(previous == current)
    }
}

fn charge(work: &mut usize, amount: usize) -> Result<(), Error> {
    *work = work.checked_sub(amount).ok_or(Error::Bounds)?;
    Ok(())
}
