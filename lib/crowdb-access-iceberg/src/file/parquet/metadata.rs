use super::{
    compact::{self, Value},
    schema, ParquetColumnChunk, ParquetMetadata, ParquetMetadataError as Error, ParquetMetadataLimits,
    ParquetRowGroup,
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
    let leaves = schema::columns(&schema)?;
    let mut total_rows = 0_u64;
    let mut decoded_groups = Vec::new();
    for group in groups {
        let group = row_group(group, &leaves, footer_start)?;
        total_rows = total_rows.checked_add(group.rows).ok_or(Error::Invalid)?;
        decoded_groups.push(group);
    }
    if total_rows != rows {
        return Err(Error::Invalid);
    }
    Ok(ParquetMetadata {
        rows,
        row_groups: groups.len(),
        schema,
        groups: decoded_groups,
    })
}

fn row_group(
    group: &Value<'_>,
    leaves: &[schema::ColumnSchema<'_>],
    footer_start: u64,
) -> Result<ParquetRowGroup, Error> {
    let fields = group.fields()?;
    let columns = required(fields, 1)?.list(12)?;
    if columns.len() != leaves.len() {
        return Err(Error::Invalid);
    }
    let expected_bytes = nonnegative(required(fields, 2)?)?;
    let rows = nonnegative(required(fields, 3)?)?;
    let mut total_bytes = 0_u64;
    let mut decoded_columns = Vec::new();
    for (column, leaf) in columns.iter().zip(leaves) {
        let fields = column.fields()?;
        if fields.contains_key(&1) || fields.contains_key(&8) || fields.contains_key(&9) {
            return Err(Error::Unsupported);
        }
        nonnegative(required(fields, 2)?)?;
        let (column, bytes) = column_metadata(required(fields, 3)?, leaf, rows, footer_start)?;
        total_bytes = total_bytes.checked_add(bytes).ok_or(Error::Invalid)?;
        decoded_columns.push(column);
    }
    if total_bytes != expected_bytes {
        return Err(Error::Invalid);
    }
    Ok(ParquetRowGroup {
        rows,
        columns: decoded_columns,
    })
}

fn column_metadata(
    value: &Value<'_>,
    leaf: &schema::ColumnSchema<'_>,
    rows: u64,
    footer_start: u64,
) -> Result<(ParquetColumnChunk, u64), Error> {
    let fields = value.fields()?;
    if required(fields, 1)?.integer(5)? != i64::from(leaf.physical_type) {
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
    if path.len() != leaf.path.len() {
        return Err(Error::Invalid);
    }
    for (name, expected) in path.iter().zip(&leaf.path) {
        if name.bytes()? != expected.as_bytes() {
            return Err(Error::Invalid);
        }
    }
    if !(0..=7).contains(&required(fields, 4)?.integer(5)?) {
        return Err(Error::Unsupported);
    }
    let values = nonnegative(required(fields, 5)?)?;
    if !leaf.repeated && values != rows {
        return Err(Error::Invalid);
    }
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
    Ok((
        ParquetColumnChunk {
            schema_index: leaf.index,
            definition_level: leaf.definition_level,
            repeated: leaf.repeated,
            offset: start,
            length: compressed,
            data_offset: data,
            compression: i32::try_from(required(fields, 4)?.integer(5)?).map_err(|_| Error::Invalid)?,
            values,
        },
        uncompressed,
    ))
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
