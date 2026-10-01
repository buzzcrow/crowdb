use crate::error::ValidationError;
use crate::key::FileId;
use crowdb_protocol::chunkdb::rpc::Location;

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
    Unbound = 8,
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
    Opaque = 5,
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
    /// Builds one unbound file authority from completed chunk locations.
    /// # Errors
    /// Rejects invalid locations, `ETag`, or file record contents.
    pub fn from_uploaded_locations(
        file: FileId,
        location: FileLocation,
        locations: &[Location],
        length: u64,
        etag: String,
    ) -> Result<Self, ValidationError> {
        let content = FileContent::from_locations(locations, length, etag)?;
        let extension = std::path::Path::new(location.relative_key()).extension();
        let has_extension = |wanted: &str| extension.is_some_and(|value| value.eq_ignore_ascii_case(wanted));
        let (kind, format) = if has_extension("json") {
            (FileKind::Metadata, ContentFormat::Json)
        } else if has_extension("avro") {
            (FileKind::Unbound, ContentFormat::Avro)
        } else if has_extension("parquet") {
            (FileKind::Unbound, ContentFormat::Parquet)
        } else if has_extension("orc") {
            (FileKind::Unbound, ContentFormat::Orc)
        } else if has_extension("puffin") {
            (FileKind::Unbound, ContentFormat::Puffin)
        } else {
            (FileKind::Unbound, ContentFormat::Opaque)
        };
        let record = Self {
            file,
            location,
            kind,
            format,
            length,
            digest: [0; 32],
            content,
            hint: None,
        };
        record.validate()?;
        Ok(record)
    }

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
            FileKind::Unbound => matches!(
                self.format,
                ContentFormat::Avro
                    | ContentFormat::Parquet
                    | ContentFormat::Orc
                    | ContentFormat::Puffin
                    | ContentFormat::Opaque
            ),
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

    /// Resolves a stored, unbound upload for one validated Iceberg use.
    /// The returned view does not mutate the immutable file authority.
    /// # Errors
    /// Rejects an incompatible kind, format or storage variant.
    pub fn bind_kind(&self, kind: FileKind) -> Result<Self, ValidationError> {
        self.validate()?;
        if kind == FileKind::Unbound || (self.kind != FileKind::Unbound && self.kind != kind) {
            return Err(ValidationError::Record);
        }
        let mut bound = self.clone();
        bound.kind = kind;
        bound.validate()?;
        Ok(bound)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileMapping {
    pub location: FileLocation,
    pub file: FileId,
}
