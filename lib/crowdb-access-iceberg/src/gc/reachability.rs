use std::sync::Arc;

use crate::file::{
    AvroBlocks, AvroCodec, AvroContainerError, AvroDatumLimits, AvroFieldPath, AvroLimits, AvroProjection,
    AvroScalar, AvroSchema, ContentFormat, FileBlockStore, FileLocation, FileRecord,
};

mod metadata;
pub use metadata::metadata_links;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ReachableKind {
    Metadata,
    ManifestList,
    Manifest,
    File,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReachableFile {
    pub location: FileLocation,
    pub kind: ReachableKind,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AvroMarkCursor {
    pub checkpoint: Vec<u8>,
    pub record_offset: u64,
}

#[derive(Clone, Debug)]
pub struct AvroMarkPage {
    pub links: Vec<ReachableFile>,
    pub next: AvroMarkCursor,
    pub complete: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct AvroMarkLimits {
    pub framing: AvroLimits,
    pub datum: AvroDatumLimits,
    pub decoded_bytes: usize,
    pub page_items: usize,
}

/// Reads one bounded page of references from a canonical manifest or manifest list.
/// # Errors
/// Rejects corrupt OCF data, invalid field types, foreign paths and invalid continuation.
pub async fn avro_links(
    blocks: Arc<dyn FileBlockStore>,
    file: &FileRecord,
    kind: ReachableKind,
    cursor: &AvroMarkCursor,
    limits: AvroMarkLimits,
) -> Result<AvroMarkPage, AvroContainerError> {
    if file.format != ContentFormat::Avro
        || !matches!(kind, ReachableKind::ManifestList | ReachableKind::Manifest)
        || !(2..=256).contains(&limits.page_items)
    {
        return Err(AvroContainerError::Bounds);
    }
    let checkpoint = (!cursor.checkpoint.is_empty()).then_some(cursor.checkpoint.as_slice());
    let mut reader = AvroBlocks::resume(blocks, file.clone(), limits.framing, checkpoint).await?;
    let schema = AvroSchema::parse(
        reader
            .metadata()
            .get("avro.schema")
            .ok_or(AvroContainerError::Schema)?,
    )?;
    let codec = AvroCodec::parse(reader.codec())?;
    let start = reader.checkpoint()?;
    let Some(block) = reader.next().await? else {
        if cursor.record_offset != 0 {
            return Err(AvroContainerError::Framing);
        }
        return Ok(AvroMarkPage {
            links: Vec::new(),
            next: cursor.clone(),
            complete: true,
        });
    };
    let count = block.records;
    if cursor.record_offset > count {
        return Err(AvroContainerError::Framing);
    }
    let decoded = block.decode_validated(codec, limits.decoded_bytes, &schema, limits.datum)?;
    let paths = reference_paths(kind);
    let projection = AvroProjection::paths(&schema, &paths)?;
    let mut records = projection.records(&decoded, count, limits.datum)?;
    let mut offset = 0;
    let mut links = Vec::new();
    while let Some(values) = records.next_record()? {
        offset += 1;
        if offset <= cursor.record_offset {
            continue;
        }
        let (keep, location, referenced) = if kind == ReachableKind::ManifestList {
            (true, values.first(), None)
        } else {
            let keep = match values.first() {
                Some(AvroScalar::Int(0 | 1)) => true,
                Some(AvroScalar::Int(2)) => false,
                _ => return Err(AvroContainerError::Schema),
            };
            (keep, values.get(1), values.get(2))
        };
        let Some(AvroScalar::String(location)) = location else {
            return Err(AvroContainerError::Schema);
        };
        let location = checked_location(location, file)?;
        if keep {
            links.push(ReachableFile {
                location,
                kind: if kind == ReachableKind::ManifestList {
                    ReachableKind::Manifest
                } else {
                    ReachableKind::File
                },
            });
            if let Some(AvroScalar::String(location)) = referenced {
                links.push(ReachableFile {
                    location: checked_location(location, file)?,
                    kind: ReachableKind::File,
                });
            } else if !matches!(referenced, None | Some(AvroScalar::Null)) {
                return Err(AvroContainerError::Schema);
            }
        }
        if links.len() + 2 > limits.page_items {
            break;
        }
    }
    let next = if offset == count {
        AvroMarkCursor {
            checkpoint: reader.checkpoint()?,
            record_offset: 0,
        }
    } else {
        AvroMarkCursor {
            checkpoint: start,
            record_offset: offset,
        }
    };
    Ok(AvroMarkPage {
        links,
        next,
        complete: false,
    })
}

fn reference_paths(kind: ReachableKind) -> Vec<AvroFieldPath<'static>> {
    if kind == ReachableKind::ManifestList {
        vec![AvroFieldPath {
            ids: &[500],
            required: true,
        }]
    } else {
        vec![
            AvroFieldPath {
                ids: &[0],
                required: true,
            },
            AvroFieldPath {
                ids: &[2, 100],
                required: true,
            },
            AvroFieldPath {
                ids: &[2, 143],
                required: false,
            },
        ]
    }
}

fn checked_location(location: &str, owner: &FileRecord) -> Result<FileLocation, AvroContainerError> {
    let location: FileLocation = location.parse().map_err(|_| AvroContainerError::Schema)?;
    if location.table() != owner.location.table() {
        return Err(AvroContainerError::Schema);
    }
    Ok(location)
}
