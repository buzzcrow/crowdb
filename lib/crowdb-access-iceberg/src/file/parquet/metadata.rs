use super::{
    compact::{self, Value},
    schema, ParquetMetadata, ParquetMetadataError as Error, ParquetMetadataLimits,
};

pub(super) fn decode(
    bytes: &[u8],
    footer_start: u64,
    limits: ParquetMetadataLimits,
) -> Result<ParquetMetadata, Error> {
    let root = compact::decode(bytes, limits)?;
    let fields = root.fields()?;
    if fields.contains_key(&8) || fields.contains_key(&9) {
        return Err(Error::Unsupported);
    }
    let version = required(fields, 1)?.integer(5)?;
    if !matches!(version, 1 | 2) {
        return Err(Error::Invalid);
    }
    let schema = schema::decode(required(fields, 2)?, limits)?;
    let rows = nonnegative(required(fields, 3)?)?;
    let groups = required(fields, 4)?.list(12)?;
    if groups.len() > limits.row_groups {
        return Err(Error::Bounds);
    }
    let leaves: Vec<_> = schema.iter().filter_map(|field| field.physical_type).collect();
    let mut total_rows = 0_u64;
    for group in groups {
        total_rows = total_rows
            .checked_add(row_group(group, &leaves, footer_start)?)
            .ok_or(Error::Invalid)?;
    }
    if total_rows != rows {
        return Err(Error::Invalid);
    }
    Ok(ParquetMetadata {
        rows,
        row_groups: groups.len(),
        schema,
    })
}

fn row_group(group: &Value<'_>, leaves: &[i32], footer_start: u64) -> Result<u64, Error> {
    let fields = group.fields()?;
    let columns = required(fields, 1)?.list(12)?;
    if columns.len() != leaves.len() {
        return Err(Error::Invalid);
    }
    let expected_bytes = nonnegative(required(fields, 2)?)?;
    let rows = nonnegative(required(fields, 3)?)?;
    let mut total_bytes = 0_u64;
    for (column, physical_type) in columns.iter().zip(leaves) {
        let fields = column.fields()?;
        if fields.contains_key(&1) || fields.contains_key(&8) || fields.contains_key(&9) {
            return Err(Error::Unsupported);
        }
        nonnegative(required(fields, 2)?)?;
        total_bytes = total_bytes
            .checked_add(column_metadata(
                required(fields, 3)?,
                *physical_type,
                footer_start,
            )?)
            .ok_or(Error::Invalid)?;
    }
    if total_bytes != expected_bytes {
        return Err(Error::Invalid);
    }
    Ok(rows)
}

fn column_metadata(value: &Value<'_>, physical_type: i32, footer_start: u64) -> Result<u64, Error> {
    let fields = value.fields()?;
    if required(fields, 1)?.integer(5)? != i64::from(physical_type) {
        return Err(Error::Invalid);
    }
    let encodings = required(fields, 2)?.list(5)?;
    if encodings.is_empty() {
        return Err(Error::Invalid);
    }
    for encoding in encodings {
        encoding.integer(5)?;
    }
    let path = required(fields, 3)?.list(8)?;
    if path.is_empty() {
        return Err(Error::Invalid);
    }
    for name in path {
        std::str::from_utf8(name.bytes()?).map_err(|_| Error::Invalid)?;
    }
    if !(0..=7).contains(&required(fields, 4)?.integer(5)?) {
        return Err(Error::Unsupported);
    }
    nonnegative(required(fields, 5)?)?;
    let uncompressed = nonnegative(required(fields, 6)?)?;
    let compressed = nonnegative(required(fields, 7)?)?;
    let data = nonnegative(required(fields, 9)?)?;
    let start = fields.get(&11).map(nonnegative).transpose()?.unwrap_or(data);
    if start < 4
        || start > data
        || data >= footer_start
        || start
            .checked_add(compressed)
            .filter(|end| *end <= footer_start && *end > data)
            .is_none()
    {
        return Err(Error::Invalid);
    }
    Ok(uncompressed)
}

fn required<'value, 'data>(
    fields: &'value std::collections::BTreeMap<i16, Value<'data>>,
    id: i16,
) -> Result<&'value Value<'data>, Error> {
    fields.get(&id).ok_or(Error::Invalid)
}

fn nonnegative(value: &Value<'_>) -> Result<u64, Error> {
    u64::try_from(value.integer(6)?).map_err(|_| Error::Invalid)
}
