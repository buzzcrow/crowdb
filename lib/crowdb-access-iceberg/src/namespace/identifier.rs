use crate::error::ValidationError;
use crate::key::NameSuffix;

pub const MAX_NAMESPACE_LEVELS: usize = 32;
pub const MAX_IDENTIFIER_BYTES: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct NamespaceIdentifier(Vec<String>);

impl NamespaceIdentifier {
    /// # Errors
    /// Rejects empty identifiers, ambiguous components and encoded-size overflow.
    pub fn new(components: Vec<String>) -> Result<Self, ValidationError> {
        if components.is_empty() || components.len() > MAX_NAMESPACE_LEVELS {
            return Err(ValidationError::Text);
        }
        let mut encoded_bytes = 0_usize;
        for component in &components {
            NameSuffix {
                parent: None,
                name: component,
            }
            .encode()?;
            encoded_bytes = encoded_bytes.saturating_add(2 + component.len());
            if encoded_bytes > MAX_IDENTIFIER_BYTES {
                return Err(ValidationError::KeyTooLarge);
            }
        }
        Ok(Self(components))
    }

    /// # Errors
    /// Rejects empty components and invalid or oversized multipart names.
    /// Input is already URL-decoded; the advertised separator is the unit separator.
    pub fn from_rest(decoded: &str) -> Result<Self, ValidationError> {
        if decoded.len() > MAX_IDENTIFIER_BYTES {
            return Err(ValidationError::KeyTooLarge);
        }
        Self::new(decoded.split('\u{1f}').map(str::to_owned).collect())
    }

    #[must_use]
    pub fn components(&self) -> &[String] {
        &self.0
    }

    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        (self.0.len() > 1).then(|| Self(self.0[..self.0.len() - 1].to_vec()))
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.0[self.0.len() - 1]
    }

    /// # Errors
    /// Rejects component lengths outside the storage representation.
    pub fn encode(&self) -> Result<Vec<u8>, ValidationError> {
        let mut bytes = Vec::new();
        for component in &self.0 {
            let length = u16::try_from(component.len()).map_err(|_| ValidationError::KeyTooLarge)?;
            bytes.extend_from_slice(&length.to_be_bytes());
            bytes.extend_from_slice(component.as_bytes());
        }
        Ok(bytes)
    }

    /// # Errors
    /// Rejects truncated lengths, invalid UTF-8 and noncanonical identifiers.
    pub fn decode(mut bytes: &[u8]) -> Result<Self, ValidationError> {
        if bytes.len() > MAX_IDENTIFIER_BYTES {
            return Err(ValidationError::KeyTooLarge);
        }
        let mut components = Vec::new();
        while !bytes.is_empty() {
            if bytes.len() < 2 || components.len() == MAX_NAMESPACE_LEVELS {
                return Err(ValidationError::Key);
            }
            let length = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
            let component = bytes.get(2..2 + length).ok_or(ValidationError::Key)?;
            components.push(
                std::str::from_utf8(component)
                    .map_err(|_| ValidationError::Text)?
                    .to_owned(),
            );
            bytes = &bytes[2 + length..];
        }
        Self::new(components)
    }
}
