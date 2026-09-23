use crate::error::ValidationError;
use crate::key::FileId;

use super::{FileContent, FileLocation};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum FileKind {
    Metadata = 0,
    ManifestList = 1,
    Manifest = 2,
    Data = 3,
    PositionDelete = 4,
    EqualityDelete = 5,
    DeletionVector = 6,
    Statistics = 7,
}

impl FileKind {
    #[must_use]
    pub const fn allows_inline(self) -> bool {
        matches!(self, Self::Metadata | Self::ManifestList | Self::Manifest)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ContentFormat {
    Json = 0,
    Avro = 1,
    Parquet = 2,
    Orc = 3,
    Puffin = 4,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FormatHint {
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileRecord {
    pub file: FileId,
    pub location: FileLocation,
    pub kind: FileKind,
    pub format: ContentFormat,
    pub length: u64,
    pub digest: [u8; 32],
    pub content: FileContent,
    pub hint: Option<FormatHint>,
}

impl FileRecord {
    /// # Errors
    /// Rejects invalid format/kind pairs and inconsistent bounded storage variants.
    pub fn validate(&self) -> Result<(), ValidationError> {
        let valid_format = match self.kind {
            FileKind::Metadata => self.format == ContentFormat::Json,
            FileKind::ManifestList | FileKind::Manifest => self.format == ContentFormat::Avro,
            FileKind::Data | FileKind::PositionDelete | FileKind::EqualityDelete => {
                matches!(
                    self.format,
                    ContentFormat::Parquet | ContentFormat::Orc | ContentFormat::Avro
                )
            }
            FileKind::DeletionVector => self.format == ContentFormat::Puffin,
            FileKind::Statistics => matches!(self.format, ContentFormat::Puffin | ContentFormat::Parquet),
        };
        if !valid_format {
            return Err(ValidationError::Record);
        }
        if matches!(self.content, FileContent::Inline { .. }) && !self.kind.allows_inline() {
            return Err(ValidationError::Record);
        }
        self.content.validate(self.length, &self.digest)
    }

    #[must_use]
    pub fn usable_hint(&self) -> Option<FormatHint> {
        self.hint.filter(|hint| {
            hint.length > 0
                && hint
                    .offset
                    .checked_add(hint.length)
                    .is_some_and(|end| end <= self.length)
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileMapping {
    pub location: FileLocation,
    pub file: FileId,
}
