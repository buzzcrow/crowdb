use super::{
    charge_bytes, source, AvroBlocks, Error, FileKind, FileRepository, ManifestListReader, ManifestMetadata,
    ManifestVersion, PriorManifestSource, TableMetadataDocument,
};

impl PriorManifestSource {
    pub(super) async fn scan(
        &mut self,
        files: &FileRepository,
        document: &TableMetadataDocument,
    ) -> Result<(), Error> {
        let limits = self.limits;
        let context = self.context;
        let mut references = 0_u64;
        let mut retained = 0_usize;
        let mut bytes = 0_u64;
        let version = match self.selected.head.format_version {
            1 => ManifestVersion::V1,
            2 => ManifestVersion::V2,
            3 => ManifestVersion::V3,
            _ => return Err(Error::Unavailable),
        };
        for snapshot in document.snapshots().values() {
            if snapshot.manifest_list.is_none() {
                if snapshot.manifests.len() as u64 > limits.manifests.manifests {
                    return Err(Error::Bounds);
                }
                for location in &snapshot.manifests {
                    references = charge_bytes(references, 1, limits.references)?;
                    let record = files
                        .load(context, location)
                        .await
                        .map_err(source)?
                        .ok_or(Error::Unavailable)?
                        .bind_kind(FileKind::Manifest)
                        .map_err(source)?;
                    bytes = charge_bytes(bytes, record.length, limits.manifests.manifest_bytes)?;
                    let reader =
                        AvroBlocks::open(self.blocks.clone(), record.clone(), limits.manifests.framing)
                            .await
                            .map_err(source)?;
                    let metadata = ManifestMetadata::parse(reader.metadata()).map_err(source)?;
                    if metadata.version != ManifestVersion::V1 {
                        return Err(Error::Unavailable);
                    }
                    self.insert(
                        record,
                        metadata.partition_spec_id.unwrap_or(0),
                        metadata.content,
                        &mut retained,
                    )?;
                }
                continue;
            }
            let selection = snapshot.manifest_selection(version).map_err(source)?;
            let record = files
                .load(context, &selection.location)
                .await
                .map_err(source)?
                .ok_or(Error::Unavailable)?;
            bytes = charge_bytes(bytes, record.length, limits.manifests.manifest_bytes)?;
            let mut manifests = 0_u64;
            let mut reader = ManifestListReader::open_selected(
                self.blocks.clone(),
                record,
                selection,
                limits.manifests.framing,
                limits.manifests.datum,
                limits.manifests.decoded_bytes,
            )
            .await?;
            while let Some(entry) = reader.next_entry().await? {
                manifests = manifests
                    .checked_add(1)
                    .filter(|count| *count <= limits.manifests.manifests)
                    .ok_or(Error::Bounds)?;
                references = references
                    .checked_add(1)
                    .filter(|count| *count <= limits.references)
                    .ok_or(Error::Bounds)?;
                let record = files
                    .load(context, &entry.location)
                    .await
                    .map_err(source)?
                    .ok_or(Error::Unavailable)?
                    .bind_kind(FileKind::Manifest)
                    .map_err(source)?;
                if record.length != entry.length {
                    return Err(Error::Unavailable);
                }
                bytes = charge_bytes(bytes, record.length, limits.manifests.manifest_bytes)?;
                self.insert(record, entry.partition_spec_id, entry.content, &mut retained)?;
            }
        }
        Ok(())
    }
}
