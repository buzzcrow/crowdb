use crate::error::ValidationError;
use crate::operation::MAX_PAYLOAD_BYTES;
use bincode::Options;
use crowdb_access_multipart::validate_selected_parts;
pub use crowdb_access_multipart::SelectedPart;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{FileContent, MultipartPart};

const MAGIC_V1: &[u8; 5] = b"ICMS\x01";
const MAGIC_V2: &[u8; 5] = b"ICMS\x02";
const ENTRY_BYTES: usize = 42;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartSelection {
    parts: Vec<SelectedPart>,
    count: u16,
    snapshots: Option<Vec<SelectedStreamPart>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SelectedStreamPart {
    pub length: u64,
    pub bytes: Vec<u8>,
    pub etag: String,
}

impl MultipartSelection {
    /// # Errors
    /// Rejects empty, oversized, unordered or duplicate part selections.
    pub fn new(parts: Vec<SelectedPart>) -> Result<Self, ValidationError> {
        validate_selected_parts(&parts, 10_000).map_err(|_| ValidationError::Record)?;
        let count = u16::try_from(parts.len()).map_err(|_| ValidationError::Record)?;
        Ok(Self {
            parts,
            count,
            snapshots: None,
        })
    }

    /// Captures complete streamed part locations at Complete so later part
    /// replacements cannot change the selected file or its GC references.
    /// # Errors
    /// Rejects mixed legacy parts, invalid descriptors or incoherent digests.
    pub fn with_stream_parts(parts: &[MultipartPart]) -> Result<Self, ValidationError> {
        let mut estimated_bytes = 7_usize
            .checked_add(parts.len().saturating_mul(ENTRY_BYTES + 64))
            .ok_or(ValidationError::RecordTooLarge)?;
        for part in parts {
            let stream = part.stream.as_ref().ok_or(ValidationError::Record)?;
            let FileContent::Locations { bytes, etag } = &stream.content else {
                return Err(ValidationError::Record);
            };
            estimated_bytes = estimated_bytes
                .checked_add(bytes.len())
                .and_then(|size| size.checked_add(etag.len()))
                .ok_or(ValidationError::RecordTooLarge)?;
            if estimated_bytes > MAX_PAYLOAD_BYTES {
                return Err(ValidationError::RecordTooLarge);
            }
        }
        let selected = parts
            .iter()
            .map(|part| SelectedPart {
                number: part.number,
                revision: part.revision,
                digest: part.selection_digest(),
            })
            .collect();
        let mut selection = Self::new(selected)?;
        let snapshots = parts
            .iter()
            .map(|part| {
                let stream = part.stream.as_ref().ok_or(ValidationError::Record)?;
                let FileContent::Locations { bytes, etag } = &stream.content else {
                    return Err(ValidationError::Record);
                };
                Ok(SelectedStreamPart {
                    length: stream.length,
                    bytes: bytes.clone(),
                    etag: etag.clone(),
                })
            })
            .collect::<Result<Vec<_>, ValidationError>>()?;
        validate_snapshots(&selection.parts, &snapshots)?;
        selection.snapshots = Some(snapshots);
        Ok(selection)
    }

    #[must_use]
    pub fn parts(&self) -> &[SelectedPart] {
        &self.parts
    }

    #[must_use]
    pub fn count(&self) -> u16 {
        self.count
    }

    #[must_use]
    pub fn snapshots(&self) -> Option<&[SelectedStreamPart]> {
        self.snapshots.as_deref()
    }

    /// # Panics
    /// Panics if bincode cannot serialize an already validated in-memory snapshot.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(7 + ENTRY_BYTES * self.parts.len());
        bytes.extend_from_slice(if self.snapshots.is_some() {
            MAGIC_V2
        } else {
            MAGIC_V1
        });
        bytes.extend_from_slice(&self.count.to_be_bytes());
        for part in &self.parts {
            bytes.extend_from_slice(&part.number.to_be_bytes());
            bytes.extend_from_slice(&part.revision.to_be_bytes());
            bytes.extend_from_slice(&part.digest);
        }
        if let Some(snapshots) = &self.snapshots {
            bytes.extend_from_slice(&bincode::serialize(snapshots).expect("validated snapshots serialize"));
        }
        bytes
    }

    /// # Errors
    /// Rejects unknown versions, invalid framing and noncanonical part sequences.
    pub fn decode(bytes: &[u8]) -> Result<Self, ValidationError> {
        if bytes.len() > MAX_PAYLOAD_BYTES {
            return Err(ValidationError::RecordTooLarge);
        }
        let version = bytes.get(..5).ok_or(ValidationError::Record)?;
        if bytes.len() < 7 || (version != MAGIC_V1 && version != MAGIC_V2) {
            return Err(ValidationError::Record);
        }
        let count = usize::from(u16::from_be_bytes([bytes[5], bytes[6]]));
        let entries_end = 7 + count * ENTRY_BYTES;
        if count == 0
            || count > 10_000
            || bytes.len() < entries_end
            || (version == MAGIC_V1 && bytes.len() != entries_end)
        {
            return Err(ValidationError::Record);
        }
        let parts = bytes[7..entries_end]
            .chunks_exact(ENTRY_BYTES)
            .map(|entry| {
                Ok(SelectedPart {
                    number: u16::from_be_bytes(entry[..2].try_into().map_err(|_| ValidationError::Record)?),
                    revision: u64::from_be_bytes(
                        entry[2..10].try_into().map_err(|_| ValidationError::Record)?,
                    ),
                    digest: entry[10..].try_into().map_err(|_| ValidationError::Record)?,
                })
            })
            .collect::<Result<_, ValidationError>>()?;
        let mut selection = Self::new(parts)?;
        if version == MAGIC_V2 {
            let encoded = &bytes[entries_end..];
            let snapshot_count = u64::from_le_bytes(
                encoded
                    .get(..8)
                    .ok_or(ValidationError::Record)?
                    .try_into()
                    .map_err(|_| ValidationError::Record)?,
            );
            if snapshot_count != count as u64 {
                return Err(ValidationError::Record);
            }
            let snapshots: Vec<SelectedStreamPart> = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_limit(MAX_PAYLOAD_BYTES as u64)
                .reject_trailing_bytes()
                .deserialize(encoded)
                .map_err(|_| ValidationError::Record)?;
            validate_snapshots(&selection.parts, &snapshots)?;
            selection.snapshots = Some(snapshots);
        }
        Ok(selection)
    }
}

fn validate_snapshots(
    parts: &[SelectedPart],
    snapshots: &[SelectedStreamPart],
) -> Result<(), ValidationError> {
    if parts.len() != snapshots.len() {
        return Err(ValidationError::Record);
    }
    for (part, snapshot) in parts.iter().zip(snapshots) {
        let content = FileContent::Locations {
            bytes: snapshot.bytes.clone(),
            etag: snapshot.etag.clone(),
        };
        content.validate(snapshot.length, &[0; 32])?;
        let mut digest = Sha256::new();
        digest.update(snapshot.length.to_le_bytes());
        digest.update(&snapshot.bytes);
        digest.update(snapshot.etag.as_bytes());
        if <[u8; 32]>::from(digest.finalize()) != part.digest {
            return Err(ValidationError::Record);
        }
    }
    Ok(())
}
