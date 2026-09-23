use crate::error::ValidationError;

const MAGIC: &[u8; 5] = b"ICMS\x01";
const ENTRY_BYTES: usize = 42;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SelectedPart {
    pub number: u16,
    pub revision: u64,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartSelection {
    parts: Vec<SelectedPart>,
    count: u16,
}

impl MultipartSelection {
    /// # Errors
    /// Rejects empty, oversized, unordered or duplicate part selections.
    pub fn new(parts: Vec<SelectedPart>) -> Result<Self, ValidationError> {
        if parts.is_empty() || parts.len() > 10_000 {
            return Err(ValidationError::Record);
        }
        let mut previous = 0;
        for part in &parts {
            if part.number <= previous || part.number > 10_000 || part.revision == 0 {
                return Err(ValidationError::Record);
            }
            previous = part.number;
        }
        let count = u16::try_from(parts.len()).map_err(|_| ValidationError::Record)?;
        Ok(Self { parts, count })
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
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(7 + ENTRY_BYTES * self.parts.len());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.count.to_be_bytes());
        for part in &self.parts {
            bytes.extend_from_slice(&part.number.to_be_bytes());
            bytes.extend_from_slice(&part.revision.to_be_bytes());
            bytes.extend_from_slice(&part.digest);
        }
        bytes
    }

    /// # Errors
    /// Rejects unknown versions, invalid framing and noncanonical part sequences.
    pub fn decode(bytes: &[u8]) -> Result<Self, ValidationError> {
        if bytes.len() < 7 || bytes.get(..5) != Some(MAGIC) {
            return Err(ValidationError::Record);
        }
        let count = usize::from(u16::from_be_bytes([bytes[5], bytes[6]]));
        if count == 0 || count > 10_000 || bytes.len() != 7 + count * ENTRY_BYTES {
            return Err(ValidationError::Record);
        }
        let parts = bytes[7..]
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
        Self::new(parts)
    }
}
