use std::sync::Arc;

use crate::error::ValidationError;

use super::{
    probe_orc_footer, probe_parquet_footer, probe_puffin_footer, read_puffin_metadata, AvroContainerError,
    AvroDatumLimits, AvroLimits, AvroRecords, ByteRange, ContentFormat, FileBlockStore, FileContent,
    FileIdentity, FileIoError, FileKind, FileLocation, FileReader, FileRecord, FileTree, FormatProbeError,
    JsonSealError, JsonSealer, PuffinMetadataError, MAX_COMPRESSION_INPUT_BYTES,
};

#[derive(Debug, thiserror::Error)]
pub enum FileSealError {
    #[error(transparent)]
    Invalid(#[from] ValidationError),
    #[error(transparent)]
    Storage(#[from] FileIoError),
    #[error(transparent)]
    Json(#[from] JsonSealError),
    #[error(transparent)]
    Avro(#[from] AvroContainerError),
    #[error(transparent)]
    Format(#[from] FormatProbeError),
    #[error(transparent)]
    Puffin(#[from] PuffinMetadataError),
    #[error("file seal exceeds configured byte limit")]
    Bounds,
}

pub struct FileSealer {
    store: Arc<dyn FileBlockStore>,
    max_file_bytes: u64,
}

impl FileSealer {
    /// Seals an ordinary SDK upload without requiring a semantic kind header.
    /// Iceberg usage is checked when a manifest or metadata reference is admitted.
    /// # Errors
    /// Rejects unknown containers, malformed bytes and invalid storage identity.
    pub async fn seal_uploaded(
        &self,
        owner: FileIdentity,
        location: FileLocation,
        tree: FileTree,
    ) -> Result<FileRecord, FileSealError> {
        let end = tree.length.min(4);
        let mut reader = FileReader::from_tree(
            self.store.clone(),
            owner,
            tree.clone(),
            Some(ByteRange { start: 0, end }),
            4,
        )?;
        let mut prefix = Vec::with_capacity(4);
        while let Some(bytes) = reader.next().await? {
            prefix.extend_from_slice(&bytes);
        }
        let format = match prefix.as_slice() {
            b"PAR1" => ContentFormat::Parquet,
            b"Obj\x01" => ContentFormat::Avro,
            b"PFA1" => ContentFormat::Puffin,
            bytes if bytes.starts_with(b"ORC") => ContentFormat::Orc,
            _ => ContentFormat::Json,
        };
        let kind = if format == ContentFormat::Json {
            FileKind::Metadata
        } else {
            FileKind::Unbound
        };
        self.seal(owner, location, tree, kind, format).await
    }

    /// # Errors
    /// Rejects an unbounded or empty file ceiling.
    pub fn new(store: Arc<dyn FileBlockStore>, max_file_bytes: u64) -> Result<Self, FileSealError> {
        if max_file_bytes == 0 || max_file_bytes > u64::MAX / 8 {
            return Err(FileSealError::Bounds);
        }
        Ok(Self {
            store,
            max_file_bytes,
        })
    }

    /// Verifies complete staged bytes and their declared container before creating
    /// a publishable authority. Iceberg table semantics belong to commit admission.
    /// # Errors
    /// Rejects mismatched identities, digest, format, byte limits or corrupt storage.
    pub async fn seal(
        &self,
        owner: FileIdentity,
        location: FileLocation,
        tree: FileTree,
        kind: FileKind,
        format: ContentFormat,
    ) -> Result<FileRecord, FileSealError> {
        if owner.table != location.table() || tree.length > self.max_file_bytes {
            return Err(FileSealError::Bounds);
        }
        let mut reader = FileReader::from_tree(self.store.clone(), owner, tree.clone(), None, 64 * 1024)?;
        let mut inline = (kind.allows_inline() && tree.length <= MAX_COMPRESSION_INPUT_BYTES as u64)
            .then(|| usize::try_from(tree.length).ok())
            .flatten()
            .map(Vec::with_capacity);
        while let Some(bytes) = reader.next().await? {
            if let Some(inline) = &mut inline {
                inline.extend_from_slice(&bytes);
            }
        }
        let content = inline
            .as_deref()
            .and_then(|bytes| FileContent::select_inline(kind, bytes))
            .unwrap_or(FileContent::Chunks { root: tree.root });
        let mut record = FileRecord {
            file: owner.file,
            location,
            kind,
            format,
            length: tree.length,
            digest: tree.digest,
            content,
            hint: None,
        };
        record.validate()?;
        record.hint = self.validate_format(&record).await?;
        Ok(record)
    }

    async fn validate_format(&self, record: &FileRecord) -> Result<Option<super::FormatHint>, FileSealError> {
        match record.format {
            ContentFormat::Json => {
                JsonSealer::new(self.store.clone(), 16, self.max_file_bytes, 128)?
                    .validate(record.clone())
                    .await?;
                Ok(None)
            }
            ContentFormat::Avro => {
                let mut records = AvroRecords::open(
                    self.store.clone(),
                    record.clone(),
                    AvroLimits {
                        header_bytes: 1024 * 1024,
                        metadata_entries: 64,
                        block_bytes: 8 * 1024 * 1024,
                        records_per_block: 1_000_000,
                    },
                    AvroDatumLimits {
                        depth: 64,
                        values: 4_000_000,
                        value_bytes: 8 * 1024 * 1024,
                    },
                    8 * 1024 * 1024,
                )
                .await?;
                let hint = records.header_hint();
                while records.next().await?.is_some() {}
                Ok(Some(hint))
            }
            ContentFormat::Parquet => Ok(Some(probe_parquet_footer(self.store.clone(), record).await?)),
            ContentFormat::Orc => Ok(Some(probe_orc_footer(self.store.clone(), record).await?.footer)),
            ContentFormat::Puffin => {
                let hint = probe_puffin_footer(self.store.clone(), record).await?.payload;
                read_puffin_metadata(self.store.clone(), record, 1024 * 1024, 1024 * 1024).await?;
                Ok(Some(hint))
            }
        }
    }
}
