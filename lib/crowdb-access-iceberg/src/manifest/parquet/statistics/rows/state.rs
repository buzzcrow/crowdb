use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use super::{charge, projection, value, Error};
use crate::{manifest::PartitionTransform, table::TableMetadataDocument};

struct Spec {
    fields: BTreeMap<i32, PartitionTransform>,
    complete: bool,
}

pub(super) struct State {
    specs: BTreeMap<i32, Spec>,
    previous: Option<BTreeMap<i32, value::Value>>,
    seen: BTreeSet<i32>,
    pub(super) retained_bytes: usize,
}

impl State {
    pub(super) fn new(
        document: &TableMetadataDocument,
        present: &[i32],
        work: &mut usize,
    ) -> Result<Self, Error> {
        let root = document.fields();
        let definitions = root
            .get("partition-specs")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .or_else(|| root.get("partition-spec").map(std::slice::from_ref))
            .ok_or(Error::Schema)?;
        let mut specs = BTreeMap::new();
        for definition in definitions {
            charge(work, 1)?;
            let id = definition
                .get("spec-id")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            let mut fields = BTreeMap::new();
            for (index, field) in definition
                .get("fields")
                .unwrap_or(definition)
                .as_array()
                .ok_or(Error::Schema)?
                .iter()
                .enumerate()
            {
                charge(work, 1)?;
                let field_id = field
                    .get("field-id")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(1000 + i64::try_from(index).map_err(|_| Error::Schema)?);
                let text = field["transform"].as_str().ok_or(Error::Schema)?;
                charge(work, text.len())?;
                fields.insert(
                    i32::try_from(field_id).map_err(|_| Error::Schema)?,
                    PartitionTransform::parse(text).map_err(|_| Error::Schema)?,
                );
            }
            let complete = fields.iter().all(|(id, transform)| {
                *transform == PartitionTransform::Void || present.binary_search(id).is_ok()
            });
            specs.insert(
                i32::try_from(id).map_err(|_| Error::Schema)?,
                Spec { fields, complete },
            );
        }
        Ok(Self {
            specs,
            previous: None,
            seen: BTreeSet::new(),
            retained_bytes: 0,
        })
    }

    pub(super) fn observe(
        &mut self,
        tuple: BTreeMap<i32, value::Value>,
        counts: &[Option<i64>; 14],
        projection: &BTreeMap<i32, projection::PartitionField>,
        work: &mut usize,
    ) -> Result<(), Error> {
        counts_valid(counts)?;
        let spec_id = i32::try_from(counts[2].ok_or(Error::Schema)?).map_err(|_| Error::Schema)?;
        let spec = self.specs.get(&spec_id).ok_or(Error::Schema)?;
        for (id, value) in &tuple {
            charge(work, 1)?;
            let kind = projection
                .get(id)
                .and_then(|field| field.result.as_ref())
                .ok_or(Error::Schema)?;
            value::transform(value, kind, spec.fields.get(id))?;
        }
        let mut order = Ordering::Less;
        if let Some(previous) = &self.previous {
            order = Ordering::Equal;
            for ((id, prior), (next_id, next)) in previous.iter().zip(&tuple) {
                charge(work, prior.bytes().min(next.bytes()))?;
                if id != next_id {
                    return Err(Error::Schema);
                }
                order = prior.compare(next)?;
                if order != Ordering::Equal {
                    break;
                }
            }
        }
        if order == Ordering::Greater {
            return Err(Error::Rows);
        }
        if order == Ordering::Less {
            self.seen.clear();
        }
        if !self.seen.insert(spec_id) && spec.complete {
            return Err(Error::Rows);
        }
        self.retained_bytes = tuple.values().map(|value| value.bytes() + 128).sum();
        self.previous = Some(tuple);
        Ok(())
    }
}

fn counts_valid(counts: &[Option<i64>; 14]) -> Result<(), Error> {
    if (2..=5).any(|id| counts[id].is_none())
        || (2..=10)
            .chain([13])
            .any(|id| counts[id].is_some_and(|value| value < 0))
        || counts[10]
            .zip(counts[3])
            .is_some_and(|(total, data)| total > data)
        || (counts[4] == Some(0) && (counts[3] != Some(0) || counts[5] != Some(0)))
        || (counts[9] == Some(0) && counts[8].is_some_and(|records| records != 0))
        || (counts[7] == Some(0)
            && counts[13].unwrap_or(0) == 0
            && counts[6].is_some_and(|records| records != 0))
    {
        return Err(Error::Rows);
    }
    Ok(())
}
