use crate::table::{TableMetadataDocument, TableMetadataError as Error};

use super::{charge, number};

pub(super) fn validate(
    prior: &TableMetadataDocument,
    candidate: &TableMetadataDocument,
    work: &mut usize,
) -> Result<(), Error> {
    retained_payloads(prior, candidate, work)?;
    let version = candidate.selected_head().format_version;
    let last_sequence = number(prior, "last-sequence-number");
    let next_row = number(prior, "next-row-id");
    let mut allocated = 0_i64;
    for (id, snapshot) in candidate.snapshots() {
        charge(work)?;
        if let Some(retained) = prior.snapshots().get(id) {
            if snapshot != retained {
                return Err(Error::Field("snapshots"));
            }
            continue;
        }
        if version > 1 && (snapshot.sequence <= last_sequence || snapshot.manifest_list.is_none()) {
            return Err(Error::Field("sequence-number"));
        }
        if version == 3 {
            let first = snapshot.first_row_id.ok_or(Error::Field("first-row-id"))?;
            let rows = snapshot.added_rows.ok_or(Error::Field("added-rows"))?;
            if first < next_row {
                return Err(Error::Field("first-row-id"));
            }
            allocated = allocated.checked_add(rows).ok_or(Error::Field("added-rows"))?;
        }
    }
    if allocated > number(candidate, "next-row-id") - next_row {
        return Err(Error::Field("next-row-id"));
    }
    Ok(())
}

fn retained_payloads(
    prior: &TableMetadataDocument,
    candidate: &TableMetadataDocument,
    work: &mut usize,
) -> Result<(), Error> {
    let mut retained = std::collections::BTreeMap::new();
    if let Some(values) = prior
        .fields()
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
    {
        for value in values {
            charge(work)?;
            retained.insert(
                value["snapshot-id"].as_i64().ok_or(Error::Field("snapshot-id"))?,
                value,
            );
        }
    }
    if let Some(values) = candidate
        .fields()
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
    {
        for value in values {
            charge(work)?;
            if let Some(prior) = value["snapshot-id"].as_i64().and_then(|id| retained.get(&id)) {
                for field in ["summary", "key-id"] {
                    if prior.get(field) != value.get(field) {
                        return Err(Error::Field(field));
                    }
                }
            }
        }
    }
    Ok(())
}
