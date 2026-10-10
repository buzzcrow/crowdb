use std::fmt;

use serde::{Deserialize, Serialize};

use crate::DatasetError;

const MAX_SEGMENT_BYTES: usize = 255;
const MAX_NAME_BYTES: usize = 255;
pub const DEFAULT_NAMESPACE: &str = "dataset-ns";

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct NamespacePath(Vec<String>);

impl NamespacePath {
    /// The fixed namespace carried by every Dataset identity in the first version.
    #[must_use]
    pub fn root() -> Self {
        Self(vec![DEFAULT_NAMESPACE.to_owned()])
    }

    /// # Errors
    /// Rejects empty, reserved, or oversized segments.
    pub fn new<I, S>(segments: I) -> Result<Self, DatasetError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let segments = segments
            .into_iter()
            .map(Into::into)
            .map(|segment| {
                if segment.is_empty()
                    || segment.len() > MAX_SEGMENT_BYTES
                    || segment.bytes().any(|byte| byte == 0 || byte == 0x1f)
                {
                    return Err(DatasetError::InvalidNamespaceSegment);
                }
                Ok(segment)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self(segments))
    }

    #[must_use]
    pub fn segments(&self) -> &[String] {
        &self.0
    }

    #[must_use]
    pub fn is_root(&self) -> bool {
        self.0 == [DEFAULT_NAMESPACE]
    }

    #[must_use]
    pub fn is_default(&self) -> bool {
        self.is_root()
    }

    /// # Errors
    /// Rejects names that cannot be represented in a qualified key.
    pub fn encode_key(&self, name: &str) -> Result<Vec<u8>, DatasetError> {
        validate_name(name)?;
        let mut encoded = Vec::with_capacity(self.0.iter().map(String::len).sum::<usize>() + name.len() + 2);
        for segment in &self.0 {
            push_part(&mut encoded, segment)?;
        }
        encoded.push(0xff);
        push_part(&mut encoded, name)?;
        Ok(encoded)
    }
}

impl Default for NamespacePath {
    fn default() -> Self {
        Self::root()
    }
}

impl fmt::Display for NamespacePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.join("."))
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct DatasetIdentity {
    namespace: NamespacePath,
    name: String,
}

impl DatasetIdentity {
    /// # Errors
    /// Rejects invalid namespace segments and dataset names.
    pub fn new(namespace: NamespacePath, name: impl Into<String>) -> Result<Self, DatasetError> {
        let name = name.into();
        validate_name(&name)?;
        Ok(Self { namespace, name })
    }

    #[must_use]
    pub const fn namespace(&self) -> &NamespacePath {
        &self.namespace
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// # Errors
    /// Propagates namespace and name validation.
    pub fn key(&self) -> Result<Vec<u8>, DatasetError> {
        self.namespace.encode_key(&self.name)
    }
}

fn validate_name(name: &str) -> Result<(), DatasetError> {
    if name.is_empty()
        || name.len() > MAX_NAME_BYTES
        || name.bytes().any(|byte| byte == 0 || byte == 0x1f || byte == 0xff)
    {
        return Err(DatasetError::InvalidName);
    }
    Ok(())
}

fn push_part(output: &mut Vec<u8>, value: &str) -> Result<(), DatasetError> {
    let length = u16::try_from(value.len()).map_err(|_| DatasetError::InvalidNamespaceSegment)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}
