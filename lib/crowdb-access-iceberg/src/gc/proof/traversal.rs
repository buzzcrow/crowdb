use std::sync::Arc;

use crate::{
    catalog::{check_context, CatalogError},
    error::ValidationError,
    file::{AvroDatumLimits, AvroLimits, FileBlockStore, FileReader},
    key::{CatalogScope, IcebergKey},
    record::StorageRecord,
    table::{head_key, TableHead, TableMetadataDocument, TableMetadataLimits},
};

use super::super::{
    avro_links, metadata_links, AvroMarkCursor, AvroMarkLimits, GcMarkError, GcNode, GcPhase, GcRepository,
    GcTask, ReachableFile, ReachableKind,
};

impl GcRepository {
    /// # Errors
    /// Rejects a changed current root before starting a durable proof.
    pub async fn start_proof(&self, task: &GcTask) -> Result<GcTask, CatalogError> {
        self.verify_proof_task(task).await?;
        if task.phase != GcPhase::Roots
            || task.queue_write != 0
            || task.paused
            || task.kind != super::super::GcTaskKind::LiveTable
        {
            return Err(CatalogError::Busy);
        }
        check_context(self.store.as_ref(), task.context).await?;
        let head = task.head.as_ref().ok_or(ValidationError::Record)?;
        let key = head_key(head.catalog, head.table);
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        if StorageRecord::decode(&key, &value.bytes)? != StorageRecord::TableHead(Box::new(head.clone())) {
            return Err(CatalogError::Conflict);
        }
        let mut next = task.progress()?;
        self.push_proof_root(&mut next, head).await?;
        self.update(task, &next).await?;
        Ok(next)
    }

    pub(in crate::gc) async fn push_proof_root(
        &self,
        task: &mut GcTask,
        head: &TableHead,
    ) -> Result<(), CatalogError> {
        if head.catalog != task.context.catalog
            || task.head.as_ref().map_or(true, |root| root.table != head.table)
        {
            return Err(ValidationError::IdentityMismatch.into());
        }
        let file = self.resolve_gc_file(&head.metadata_location).await?;
        if file.file != head.metadata_file || file.digest != head.metadata_digest {
            return Err(ValidationError::Record.into());
        }
        let node = GcNode {
            continuation: task.proof.pending.clone(),
            head: Some(head.clone()),
            task: task.identity,
            file: file.file,
            location: file.location,
            digest: file.digest,
            kind: ReachableKind::Metadata,
            cursor: AvroMarkCursor::default(),
            complete: false,
        };
        task.proof.pending = Some(
            self.put_proof_bytes(task, StorageRecord::GcNode(Box::new(node)).encode()?)
                .await?,
        );
        task.proof.complete = false;
        task.queue_write = task
            .queue_write
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        Ok(())
    }

    /// # Errors
    /// A missing frame or corrupt canonical source retains the prior durable proof state.
    pub async fn advance_proof(
        &self,
        task: &GcTask,
        blocks: Arc<dyn FileBlockStore>,
        metadata: TableMetadataLimits,
        framing: AvroLimits,
        datum: AvroDatumLimits,
    ) -> Result<GcTask, GcMarkError> {
        self.verify_proof_task(task).await?;
        if task.phase != GcPhase::Mark || task.proof.complete || task.paused {
            return Err(CatalogError::Busy.into());
        }
        let mut next = task.progress()?;
        let Some(reference) = &task.proof.pending else {
            if task.proof.root.is_none() || task.queue_read != task.queue_write {
                return Err(ValidationError::Record.into());
            }
            next.proof.complete = true;
            next.phase = GcPhase::Fence;
            self.update(task, &next).await?;
            return Ok(next);
        };
        let payload = self
            .proof_payload(task, &reference.page_key(0)?.encode()?)
            .await?;
        if payload.reference != *reference {
            return Err(ValidationError::Record.into());
        }
        let root = crowdb_protocol::iceberg_fb::root_as_fbiceberg_record(&payload.bytes)
            .map_err(|_| ValidationError::Record)?;
        let node = root.value_as_fbgc_node().ok_or(ValidationError::Record)?;
        let mut suffix = task.identity.as_bytes().to_vec();
        suffix.extend_from_slice(node.file_id().bytes());
        let key = IcebergKey::Catalog {
            catalog: task.context.catalog,
            scope: CatalogScope::GcNode,
            suffix,
        };
        let StorageRecord::GcNode(mut node) = StorageRecord::decode(&key, &payload.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        let (links, complete) = self
            .proof_links(&mut node, blocks, metadata, framing, datum)
            .await?;
        if complete {
            let (root, inserted) = self
                .insert_mark(task, task.proof.root.as_ref(), node.file)
                .await?;
            next.proof.root = Some(root);
            next.marked = next
                .marked
                .checked_add(u64::from(inserted))
                .ok_or(ValidationError::GenerationExhausted)?;
            next.proof.pending.clone_from(&node.continuation);
            next.queue_read = next
                .queue_read
                .checked_add(1)
                .ok_or(ValidationError::GenerationExhausted)?;
        } else {
            next.proof.pending = Some(
                self.put_proof_bytes(task, StorageRecord::GcNode(node).encode()?)
                    .await?,
            );
        }
        for link in links.into_iter().rev() {
            self.push_proof_link(&mut next, link).await?;
        }
        self.update(task, &next).await?;
        Ok(next)
    }

    async fn push_proof_link(&self, task: &mut GcTask, link: ReachableFile) -> Result<(), CatalogError> {
        let file = self.resolve_gc_file(&link.location).await?;
        let node = GcNode {
            continuation: task.proof.pending.clone(),
            head: None,
            task: task.identity,
            file: file.file,
            location: file.location,
            digest: file.digest,
            kind: link.kind,
            cursor: AvroMarkCursor::default(),
            complete: link.kind == ReachableKind::File,
        };
        task.proof.pending = Some(
            self.put_proof_bytes(task, StorageRecord::GcNode(Box::new(node)).encode()?)
                .await?,
        );
        task.queue_write = task
            .queue_write
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        Ok(())
    }

    async fn proof_links(
        &self,
        node: &mut GcNode,
        blocks: Arc<dyn FileBlockStore>,
        metadata: TableMetadataLimits,
        framing: AvroLimits,
        datum: AvroDatumLimits,
    ) -> Result<(Vec<ReachableFile>, bool), GcMarkError> {
        let file = self.resolve_gc_file(&node.location).await?;
        if file.file != node.file || file.digest != node.digest {
            return Err(ValidationError::Record.into());
        }
        if node.kind == ReachableKind::File {
            return Ok((Vec::new(), true));
        }
        if node.kind == ReachableKind::Metadata {
            let head = node.head.as_ref().ok_or(ValidationError::Record)?;
            if head.metadata_file != file.file
                || head.metadata_location != file.location
                || file.length > metadata.bytes as u64
            {
                return Err(ValidationError::Record.into());
            }
            let mut reader = FileReader::new(blocks, file, None, 64 * 1024)?;
            let mut bytes = Vec::new();
            while let Some(frame) = reader.next().await? {
                bytes.extend_from_slice(&frame);
            }
            let document = TableMetadataDocument::parse(bytes, head, metadata)?;
            let links = metadata_links(document.canonical(), &node.location, metadata)?;
            let start = usize::try_from(node.cursor.record_offset).map_err(|_| ValidationError::Record)?;
            if start > links.len() {
                return Err(ValidationError::Record.into());
            }
            let end = start.saturating_add(128).min(links.len());
            node.cursor.record_offset = end as u64;
            return Ok((links[start..end].to_vec(), end == links.len()));
        }
        let page = avro_links(
            blocks,
            &file,
            node.kind,
            &node.cursor,
            AvroMarkLimits {
                framing,
                datum,
                decoded_bytes: 8 * 1024 * 1024,
                page_items: 128,
            },
        )
        .await?;
        node.cursor = page.next;
        Ok((page.links, page.complete))
    }
}
