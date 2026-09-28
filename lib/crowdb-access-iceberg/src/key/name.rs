use crate::error::ValidationError;

use super::{NamespaceId, MAX_KEY_BYTES};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NameSuffix<'name> {
    pub parent: Option<NamespaceId>,
    pub name: &'name str,
}

impl<'name> NameSuffix<'name> {
    /// # Errors
    /// Rejects empty, ambiguous or oversized names before allocation.
    pub fn encode(self) -> Result<Vec<u8>, ValidationError> {
        validate_name(self.name)?;
        let length = u16::try_from(self.name.len()).map_err(|_| ValidationError::KeyTooLarge)?;
        let mut bytes = Vec::with_capacity(18 + self.name.len());
        let parent = self.parent.as_ref().map_or(&[0; 16], NamespaceId::as_bytes);
        bytes.extend_from_slice(parent);
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(self.name.as_bytes());
        Ok(bytes)
    }

    /// # Errors
    /// Rejects truncation, trailing data, invalid UTF-8 and invalid names.
    pub fn decode(bytes: &'name [u8]) -> Result<Self, ValidationError> {
        if bytes.len() < 18 || bytes.len() > MAX_KEY_BYTES - 23 {
            return Err(ValidationError::Key);
        }
        let parent = if bytes[..16] == [0; 16] {
            None
        } else {
            Some(NamespaceId::from_bytes(&bytes[..16])?)
        };
        let length = usize::from(u16::from_be_bytes([bytes[16], bytes[17]]));
        if length != bytes.len() - 18 {
            return Err(ValidationError::Key);
        }
        let name = std::str::from_utf8(&bytes[18..]).map_err(|_| ValidationError::Text)?;
        validate_name(name)?;
        Ok(Self { parent, name })
    }
}

fn validate_name(name: &str) -> Result<(), ValidationError> {
    if name.is_empty() || name.bytes().any(|byte| byte == 0 || byte == 0x1f) {
        return Err(ValidationError::Text);
    }
    if name.len() > MAX_KEY_BYTES - 41 {
        return Err(ValidationError::KeyTooLarge);
    }
    Ok(())
}
