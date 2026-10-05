use super::{bad_request, bounds, failed, missing_reference, Result, Selection, MAX_SCAN};
use crowdb_access_iceberg::{
    file::{AvroRecords, FileLocation},
    manifest::{
        ManifestContext, ManifestListEntry, ManifestListReader, ManifestListSelection, ManifestMetadata,
        ManifestReader, ManifestVersion,
    },
};
use serde_json::Value;

impl Selection<'_> {
    pub(super) async fn browse(&self, manifest: Option<&str>, file: Option<&str>) -> Result<Value> {
        let limits = super::super::table_limits::commits().prior.manifests;
        let location = self.snapshot["manifest-list"].as_str().ok_or_else(|| {
            failed(
                "Snapshot has no manifest list; legacy inline manifests are not supported by this inspector",
            )
        })?;
        let location = location.parse::<FileLocation>().map_err(failed)?;
        let record = self.record(&location).await?;
        if record.length > limits.manifest_bytes {
            return Err(bounds());
        }
        let selection = ManifestListSelection {
            location,
            table_version: version(&self.metadata)?,
            snapshot_id: self.snapshot["snapshot-id"].as_i64().ok_or_else(bad_request)?,
            parent_snapshot_id: self.snapshot["parent-snapshot-id"].as_i64(),
            sequence: self.snapshot["sequence-number"].as_i64().unwrap_or(0),
            first_row_id: self.snapshot["first-row-id"].as_i64(),
            added_rows: self.snapshot["added-rows"].as_i64(),
        };
        if manifest.is_none() {
            // Validate canonical header linkage before accepting page continuation.
            ManifestListReader::open_selected(
                self.service.blocks.clone(),
                record.clone(),
                selection.clone(),
                limits.framing,
                limits.datum,
                limits.decoded_bytes,
            )
            .await
            .map_err(failed)?;
            return self.list_page(record, selection).await;
        }
        let mut reader = ManifestListReader::open_selected(
            self.service.blocks.clone(),
            record.clone(),
            selection,
            limits.framing,
            limits.datum,
            limits.decoded_bytes,
        )
        .await
        .map_err(failed)?;
        for _ in 0..MAX_SCAN {
            let Some(entry) = reader.next_entry().await.map_err(failed)? else {
                return Err(missing_reference());
            };
            if manifest == Some(entry.location.to_string().as_str()) {
                return self.manifest(entry, file).await;
            }
        }
        Err(bounds())
    }

    async fn manifest(&self, entry: ManifestListEntry, file: Option<&str>) -> Result<Value> {
        let record = self.record(&entry.location).await?;
        let limits = super::super::table_limits::commits().prior.manifests;
        if record.length > limits.manifest_bytes {
            return Err(bounds());
        }
        let header = AvroRecords::open(
            self.service.blocks.clone(),
            record.clone(),
            limits.framing,
            limits.datum,
            limits.decoded_bytes,
        )
        .await
        .map_err(failed)?;
        let writer = ManifestMetadata::parse(header.metadata()).map_err(failed)?;
        let context = self.manifest_context(writer, entry.partition_spec_id)?;
        if file.is_none() {
            return self.manifest_page(record, &entry, &context).await;
        }
        let mut reader = ManifestReader::open(
            self.service.blocks.clone(),
            record.clone(),
            entry,
            context.clone(),
            limits.framing,
            limits.datum,
            limits.decoded_bytes,
        )
        .await
        .map_err(failed)?;
        for _ in 0..MAX_SCAN {
            let Some(entry) = reader.next_entry().await.map_err(failed)? else {
                return Err(missing_reference());
            };
            if file == Some(entry.file.location.to_string().as_str()) {
                let record = self.record(&entry.file.location).await?;
                if record.length != entry.file.length {
                    return Err(failed("File length differs from manifest"));
                }
                return super::parquet::inspect(self.service, &record, &entry, self.offset).await;
            }
        }
        Err(bounds())
    }

    fn manifest_context(&self, writer: ManifestMetadata<'_>, spec_id: i32) -> Result<ManifestContext> {
        let schema: Value = serde_json::from_slice(writer.schema_json).map_err(failed)?;
        let schema_id = writer.schema_id.unwrap_or(0);
        let trusted = self.metadata["schemas"]
            .as_array()
            .and_then(|items| {
                items
                    .iter()
                    .find(|item| item["schema-id"].as_i64() == Some(i64::from(schema_id)))
            })
            .or_else(|| self.metadata.get("schema"))
            .ok_or_else(missing_reference)?;
        if trusted != &schema {
            return Err(failed(
                "Manifest schema is not retained in selected table metadata",
            ));
        }
        let spec = self.metadata["partition-specs"]
            .as_array()
            .and_then(|items| {
                items
                    .iter()
                    .find(|item| item["spec-id"].as_i64() == Some(i64::from(spec_id)))
            })
            .and_then(|item| item.get("fields"))
            .or_else(|| self.metadata.get("partition-spec"))
            .ok_or_else(missing_reference)?;
        ManifestContext::parse(
            writer.version,
            schema_id,
            spec_id,
            &serde_json::to_vec(trusted).map_err(failed)?,
            &serde_json::to_vec(spec).map_err(failed)?,
        )
        .map_err(failed)
    }
}
fn version(metadata: &Value) -> Result<ManifestVersion> {
    match metadata["format-version"].as_u64() {
        Some(1) => Ok(ManifestVersion::V1),
        Some(2) => Ok(ManifestVersion::V2),
        Some(3) => Ok(ManifestVersion::V3),
        _ => Err(bad_request()),
    }
}
