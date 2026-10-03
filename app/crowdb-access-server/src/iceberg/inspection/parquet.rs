use super::super::table_read::TableHttp;
use super::{failed, Result};
use crowdb_access_iceberg::{
    file::{
        read_parquet_metadata, ContentFormat, FileRecord, ParquetColumnChunk, ParquetLogicalType,
        ParquetSchemaElement,
    },
    manifest::ManifestScalarEntry,
};
use serde_json::{json, Value};

pub(super) async fn inspect(
    service: &TableHttp,
    record: &FileRecord,
    entry: &ManifestScalarEntry,
    offset: usize,
) -> Result<Value> {
    if record.format != ContentFormat::Parquet {
        return Ok(
            json!({"kind":"unsupported","location":record.location.to_string(),"format":format!("{:?}",record.format),"size":record.length,"reason":"Metadata inspector is available for Parquet files; this format is not yet decoded"}),
        );
    }
    let metadata = read_parquet_metadata(
        service.blocks.clone(),
        record,
        super::super::table_limits::commits().auxiliary.parquet,
    )
    .await
    .map_err(failed)?;
    let next = (offset.saturating_add(20) < metadata.groups.len()).then_some(offset + 20);
    let groups: Vec<_> = metadata.groups.iter().enumerate().skip(offset).take(20).map(|(index,group)|json!({"index":index,"rows":group.rows,"columns":group.columns.iter().map(|column|column_json(column,&metadata.schema[column.schema_index])).collect::<Vec<_>>()})).collect();
    Ok(
        json!({"kind":"parquet","location":record.location.to_string(),"size":record.length,
        "content":format!("{:?}",entry.entry.content),"equality_ids":entry.file.equality_ids,
        "physical_rows":metadata.rows,"row_group_count":metadata.row_groups,"groups":groups,"next":next,
        "footer":{"offset":metadata.footer.offset,"length":metadata.footer.length,"version":metadata.footer.version,"writer":metadata.footer.writer,"properties":metadata.footer.properties},
        "schema":metadata.schema.iter().map(|field|json!({"name":field.name,"id":field.field_id,"physical_type":physical(field.physical_type),"logical_type":field.logical_type.as_ref().map(|v|format!("{v:?}")),"converted_type":field.converted_type,"children":field.children,"repetition":field.repetition,"precision":field.precision,"scale":field.scale})).collect::<Vec<_>>(),
        "logical_metadata_bytes":metadata.footer.length+12,"data_page_bytes":0}),
    )
}
fn physical(kind: Option<i32>) -> &'static str {
    match kind {
        Some(0) => "BOOLEAN",
        Some(1) => "INT32",
        Some(2) => "INT64",
        Some(3) => "INT96",
        Some(4) => "FLOAT",
        Some(5) => "DOUBLE",
        Some(6) => "BYTE_ARRAY",
        Some(7) => "FIXED_LEN_BYTE_ARRAY",
        _ => "STRUCT",
    }
}
fn column_json(column: &ParquetColumnChunk, field: &ParquetSchemaElement) -> Value {
    let stats = column.statistics.as_ref().map(|s|json!({"nulls":s.nulls,"distinct":s.distinct,"lower":s.lower.as_ref().map(|b|bound(b,field)),"upper":s.upper.as_ref().map(|b|bound(b,field)),"lower_exact":s.lower_exact,"upper_exact":s.upper_exact}));
    json!({"path":column.path,"field_id":field.field_id,"physical_type":physical(field.physical_type),"logical_type":field.logical_type.as_ref().map(|v|format!("{v:?}")),"converted_type":field.converted_type,"scale":field.scale,"precision":field.precision,
        "offset":column.offset,"compressed":column.length,"uncompressed":column.uncompressed,"data_offset":column.data_offset,"values":column.values,
        "codec":match column.compression {0=>"UNCOMPRESSED",1=>"SNAPPY",2=>"GZIP",3=>"LZO",4=>"BROTLI",5=>"LZ4",6=>"ZSTD",7=>"LZ4_RAW",_=>"UNKNOWN"},
        "encodings":column.encodings.iter().map(|v|match v {0=>"PLAIN",2=>"PLAIN_DICTIONARY",3=>"RLE",4=>"BIT_PACKED",5=>"DELTA_BINARY_PACKED",6=>"DELTA_LENGTH_BYTE_ARRAY",7=>"DELTA_BYTE_ARRAY",8=>"RLE_DICTIONARY",9=>"BYTE_STREAM_SPLIT",_=>"UNKNOWN"}).collect::<Vec<_>>(),"statistics":stats})
}
fn bound(bytes: &[u8], field: &ParquetSchemaElement) -> String {
    if matches!(
        field.logical_type,
        Some(ParquetLogicalType::Integer { signed: false, .. })
    ) || matches!(field.converted_type, Some(11..=14))
    {
        return match bytes.len() {
            4 => u32::from_le_bytes(bytes.try_into().unwrap()).to_string(),
            8 => u64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
            _ => format!("hex:{}", hex::encode(bytes)),
        };
    }
    if field.logical_type.as_ref().is_some_and(|kind| {
        !matches!(
            kind,
            ParquetLogicalType::String | ParquetLogicalType::Integer { .. }
        )
    }) || field
        .converted_type
        .is_some_and(|kind| !matches!(kind, 0 | 15..=18))
    {
        // Preserve encoded bounds when no logical formatter is available.
        return format!("hex:{}", hex::encode(bytes));
    }
    match (field.physical_type, bytes.len()) {
        (Some(1), 4) => i32::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some(2), 8) => i64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some(4), 4) => f32::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some(5), 8) => f64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        (Some(6), _)
            if field.converted_type == Some(0) || field.logical_type == Some(ParquetLogicalType::String) =>
        {
            String::from_utf8_lossy(bytes).into_owned()
        }
        _ => format!("hex:{}", hex::encode(bytes)),
    }
}
