use super::{bounds, cursor::Cursor, failed, present, Result, Selection, PAGE};
use crowdb_access_iceberg::{
    file::{AvroRecords, FileRecord},
    manifest::{
        ManifestContext, ManifestEntryProjection, ManifestEntryState, ManifestListEntry,
        ManifestListProjection, ManifestListSelection, ManifestMetadata,
    },
};
use serde_json::{json, Value};

const RESPONSE_BUDGET: usize = 2 * 1024 * 1024;
const BLOCK_BUDGET: u64 = 16 * 1024 * 1024;

impl Selection<'_> {
    pub(super) async fn list_page(
        &self,
        record: FileRecord,
        selected: ManifestListSelection,
    ) -> Result<Value> {
        let limits = super::super::table_limits::commits().prior.manifests;
        let mut reader = AvroRecords::open(
            self.service.blocks.clone(),
            record.clone(),
            limits.framing,
            limits.datum,
            limits.decoded_bytes,
        )
        .await
        .map_err(failed)?;
        let mut cursor = self.cursor(&record)?;
        if cursor.block > 0 {
            reader
                .seek(self.service.blocks.clone(), record.clone(), cursor.block)
                .map_err(failed)?;
        }
        let start = cursor.index;
        let mut rows = Vec::new();
        let mut size = 0;
        let mut bytes = 0;
        loop {
            let block_start = reader.position();
            let Some(block) = reader.next().await.map_err(failed)? else {
                return Ok(
                    json!({"kind":"manifest-list","location":record.location.to_string(),"size":record.length,"rows":rows,"next":null,"offset":start}),
                );
            };
            bytes += block.payload.length;
            if bytes > BLOCK_BUDGET {
                return Err(bounds());
            }
            let projection = ManifestListProjection::for_read(
                reader.schema(),
                selected.table_version,
                record.location.table(),
            )
            .map_err(failed)?;
            let mut records = projection
                .records(&block.bytes, block.records, limits.datum)
                .map_err(failed)?;
            for index in 0..block.records {
                let entry = records
                    .next_entry()
                    .map_err(failed)?
                    .ok_or_else(|| failed("missing record"))?;
                if index < cursor.skip {
                    continue;
                }
                if entry.sequence > selected.sequence {
                    return Err(failed("Manifest sequence exceeds snapshot"));
                }
                let row = present::manifest(&entry);
                let cost = serde_json::to_vec(&row).map_err(failed)?.len();
                if cost > RESPONSE_BUDGET {
                    return Err(bounds());
                }
                if rows.len() == PAGE || size + cost > RESPONSE_BUDGET {
                    let next = self.sign_cursor(
                        &record,
                        Cursor {
                            block: block_start,
                            skip: index,
                            index: start + rows.len(),
                            row_id: None,
                        },
                    )?;
                    return Ok(
                        json!({"kind":"manifest-list","location":record.location.to_string(),"size":record.length,"rows":rows,"next":next,"offset":start}),
                    );
                }
                size += cost;
                rows.push(row);
            }
            cursor.skip = 0;
        }
    }

    pub(super) async fn manifest_page(
        &self,
        record: FileRecord,
        list: &ManifestListEntry,
        context: &ManifestContext,
    ) -> Result<Value> {
        let limits = super::super::table_limits::commits().prior.manifests;
        let mut reader = AvroRecords::open(
            self.service.blocks.clone(),
            record.clone(),
            limits.framing,
            limits.datum,
            limits.decoded_bytes,
        )
        .await
        .map_err(failed)?;
        let codec = reader.metadata().get("avro.codec").map_or_else(
            || "null".into(),
            |value| String::from_utf8_lossy(value).into_owned(),
        );
        let writer = ManifestMetadata::parse(reader.metadata()).map_err(failed)?;
        let version = writer.version;
        let schema: Value = serde_json::from_slice(writer.schema_json).map_err(failed)?;
        let mut cursor = self.cursor(&record)?;
        let start = cursor.index;
        let mut state =
            ManifestEntryState::from_list(writer, list, record.location.table()).map_err(failed)?;
        if cursor.block > 0 {
            state = ManifestEntryState::new(
                version,
                record.location.table(),
                list.content,
                list.added_snapshot_id,
                list.sequence,
                cursor.row_id,
            )
            .map_err(failed)?;
            reader
                .seek(self.service.blocks.clone(), record.clone(), cursor.block)
                .map_err(failed)?;
        }
        let mut rows = Vec::new();
        let mut size = 0;
        let mut bytes = 0;
        loop {
            let block_start = reader.position();
            let row_id = state.next_row_id();
            let Some(block) = reader.next().await.map_err(failed)? else {
                return Ok(
                    json!({"kind":"manifest","location":record.location.to_string(),"descriptor":present::manifest(list),"codec":codec,"schema":schema,"rows":rows,"next":null,"offset":start}),
                );
            };
            bytes += block.payload.length;
            if bytes > BLOCK_BUDGET {
                return Err(bounds());
            }
            let projection = ManifestEntryProjection::with_context(
                reader.schema(),
                version,
                record.location.table(),
                context,
            )
            .map_err(failed)?;
            let mut records = projection
                .records(&block.bytes, block.records, limits.datum, &mut state)
                .map_err(failed)?;
            for index in 0..block.records {
                let entry = records
                    .next_entry()
                    .map_err(failed)?
                    .ok_or_else(|| failed("missing record"))?;
                if index < cursor.skip {
                    continue;
                }
                if entry.inherited.data_sequence > list.sequence
                    || entry.inherited.file_sequence > list.sequence
                {
                    return Err(failed("File sequence exceeds manifest"));
                }
                let row = present::entry(&entry, context);
                let cost = serde_json::to_vec(&row).map_err(failed)?.len();
                if cost > RESPONSE_BUDGET {
                    return Err(bounds());
                }
                if rows.len() == PAGE || size + cost > RESPONSE_BUDGET {
                    let next = self.sign_cursor(
                        &record,
                        Cursor {
                            block: block_start,
                            skip: index,
                            index: start + rows.len(),
                            row_id,
                        },
                    )?;
                    return Ok(
                        json!({"kind":"manifest","location":record.location.to_string(),"descriptor":present::manifest(list),"codec":codec,"schema":schema,"rows":rows,"next":next,"offset":start}),
                    );
                }
                size += cost;
                rows.push(row);
            }
            cursor.skip = 0;
        }
    }
}
