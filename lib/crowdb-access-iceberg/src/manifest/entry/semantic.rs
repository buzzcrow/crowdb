use super::{ManifestEntryError as Error, ManifestFileFields};
use crate::manifest::{FileContentKind, ManifestContext, PrimitiveType};

pub(super) fn validate(
    context: &ManifestContext,
    file_kind: FileContentKind,
    file: &ManifestFileFields,
) -> Result<(), Error> {
    if let Some(ids) = &file.equality_ids {
        for id in ids {
            let field = context.retained_field(*id).ok_or(Error::Field)?;
            if field.repeated
                || !field
                    .primitive
                    .as_ref()
                    .is_some_and(PrimitiveType::equality_eligible)
            {
                return Err(Error::Field);
            }
        }
    }
    let metrics = &file.metrics;
    for map in [
        &metrics.column_sizes,
        &metrics.value_counts,
        &metrics.null_value_counts,
        &metrics.nan_value_counts,
    ]
    .into_iter()
    .flatten()
    {
        for id in map.keys() {
            let primitive = primitive(context, file_kind, *id)?;
            if metrics
                .nan_value_counts
                .as_ref()
                .is_some_and(|values| values.contains_key(id))
                && !matches!(primitive, Some(PrimitiveType::Float | PrimitiveType::Double))
            {
                return Err(Error::Field);
            }
        }
    }
    for (map, lower) in [(&metrics.lower_bounds, true), (&metrics.upper_bounds, false)] {
        if let Some(map) = map {
            for (id, bytes) in map {
                if !lower
                    && metrics
                        .lower_bounds
                        .as_ref()
                        .is_some_and(|values| values.contains_key(id))
                {
                    continue;
                }
                let kind = primitive(context, file_kind, *id)?.ok_or(Error::Field)?;
                let (lower, upper) = if lower {
                    (
                        Some(bytes.as_slice()),
                        metrics
                            .upper_bounds
                            .as_ref()
                            .and_then(|values| values.get(id))
                            .map(Vec::as_slice),
                    )
                } else {
                    (None, Some(bytes.as_slice()))
                };
                super::bounds::validate(&kind, lower, upper)?;
            }
        }
    }
    Ok(())
}

fn primitive(
    context: &ManifestContext,
    file_kind: FileContentKind,
    id: i32,
) -> Result<Option<PrimitiveType>, Error> {
    if file_kind == FileContentKind::PositionDeletes {
        match id {
            2_147_483_546 => return Ok(Some(PrimitiveType::String)),
            2_147_483_545 => return Ok(Some(PrimitiveType::Long)),
            2_147_483_544 => return Ok(None),
            _ => {}
        }
    }
    Ok(context.retained_field(id).ok_or(Error::Field)?.primitive.clone())
}
