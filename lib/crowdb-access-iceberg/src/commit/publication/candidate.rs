use std::sync::Arc;

use super::{CommitPublicationError as Error, Publisher};
use crate::{
    catalog::{CatalogContext, CatalogStore},
    commit::TableCommitOperation,
    error::ValidationError,
    file::{
        ContentFormat, FileBlockStore, FileContent, FileIdentity, FileKind, FileRecord, FileRepository,
        FileTreeWriter,
    },
    operation::MAX_PAYLOAD_BYTES,
    table::{TableHead, TableMetadataDocument},
};

impl Publisher {
    pub(super) async fn write_candidate(
        &self,
        operation: &TableCommitOperation,
        document: &TableMetadataDocument,
    ) -> Result<(), Error> {
        self.current(operation).await?;
        write_metadata_file(
            self.store.clone(),
            self.blocks.clone(),
            operation.context,
            document,
        )
        .await?;
        self.current(operation).await?;
        Ok(())
    }
}

pub(in crate::commit) async fn write_metadata_file(
    store: Arc<dyn CatalogStore>,
    blocks: Arc<dyn FileBlockStore>,
    domain: CatalogContext,
    document: &TableMetadataDocument,
) -> Result<(), Error> {
    let head = document.selected_head();
    let files = FileRepository::new(store);
    let inline = FileContent::select_inline(FileKind::Metadata, document.canonical());
    let content = if let Some(content) = inline {
        content
    } else {
        if let Some(existing) = files.load(domain, &head.metadata_location).await? {
            validate(&existing, head, document.canonical().len())?;
            return Ok(());
        }
        let mut writer = FileTreeWriter::new(
            blocks,
            FileIdentity {
                table: head.metadata_location.table(),
                file: head.metadata_file,
            },
            64 * 1024,
        )?;
        writer.push(document.canonical()).await?;
        let tree = writer.finish().await?;
        FileContent::Chunks { root: tree.root }
    };
    let record = FileRecord {
        file: head.metadata_file,
        location: head.metadata_location.clone(),
        kind: FileKind::Metadata,
        format: ContentFormat::Json,
        length: document.canonical().len() as u64,
        digest: head.metadata_digest,
        content,
        hint: None,
    };
    let selected = files.publish(domain, &record).await?;
    validate(&selected, head, document.canonical().len())?;
    Ok(())
}

fn validate(record: &FileRecord, head: &TableHead, length: usize) -> Result<(), ValidationError> {
    if record.file != head.metadata_file
        || record.location != head.metadata_location
        || record.digest != head.metadata_digest
        || record.length != length as u64
        || record.kind != FileKind::Metadata
        || record.format != ContentFormat::Json
    {
        return Err(ValidationError::IdentityMismatch);
    }
    Ok(())
}

pub(in crate::commit) fn response(head: &TableHead, canonical: &[u8]) -> Result<Vec<u8>, ValidationError> {
    let location =
        serde_json::to_vec(&head.metadata_location.to_string()).map_err(|_| ValidationError::Record)?;
    let length = canonical
        .len()
        .checked_add(location.len())
        .and_then(|length| length.checked_add(b"{\"metadata-location\":,\"metadata\":}".len()))
        .filter(|length| *length <= MAX_PAYLOAD_BYTES)
        .ok_or(ValidationError::RecordTooLarge)?;
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(b"{\"metadata-location\":");
    bytes.extend(location);
    bytes.extend_from_slice(b",\"metadata\":");
    bytes.extend_from_slice(canonical);
    bytes.push(b'}');
    Ok(bytes)
}
