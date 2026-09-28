#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestVersion {
    V1,
    V2,
    V3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryStatus {
    Existing,
    Added,
    Deleted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestContent {
    Data,
    Deletes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileContentKind {
    Data,
    PositionDeletes,
    EqualityDeletes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestEntry {
    pub status: EntryStatus,
    pub content: FileContentKind,
    pub snapshot_id: Option<i64>,
    pub data_sequence: Option<i64>,
    pub file_sequence: Option<i64>,
    pub first_row_id: Option<i64>,
    pub record_count: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InheritedEntry {
    pub snapshot_id: i64,
    pub data_sequence: i64,
    pub file_sequence: i64,
    pub first_row_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ManifestInheritanceError {
    #[error("manifest and file content disagree")]
    Content,
    #[error("required manifest entry inheritance source is missing")]
    Missing,
    #[error("invalid manifest sequence, count or row ID")]
    Number,
    #[error("manifest row ID range exhausted")]
    Overflow,
}

#[derive(Clone)]
pub struct ManifestInheritance {
    version: ManifestVersion,
    content: ManifestContent,
    snapshot_id: i64,
    sequence: i64,
    next_row_id: Option<i64>,
}

impl ManifestInheritance {
    /// The version belongs to the manifest, not the containing table or snapshot.
    /// A new snapshot may assign row IDs to manifests from an older table version.
    /// # Errors
    /// Rejects negative sequence/row IDs and delete manifests in version one.
    pub fn new(
        version: ManifestVersion,
        content: ManifestContent,
        snapshot_id: i64,
        sequence: i64,
        first_row_id: Option<i64>,
    ) -> Result<Self, ManifestInheritanceError> {
        if sequence < 0 || first_row_id.is_some_and(|value| value < 0) {
            return Err(ManifestInheritanceError::Number);
        }
        if (version == ManifestVersion::V1 && content != ManifestContent::Data)
            || (content == ManifestContent::Deletes && first_row_id.is_some())
        {
            return Err(ManifestInheritanceError::Content);
        }
        Ok(Self {
            version,
            content,
            snapshot_id,
            sequence,
            next_row_id: first_row_id,
        })
    }

    #[must_use]
    pub fn next_row_id(&self) -> Option<i64> {
        self.next_row_id
    }

    /// Resolves one entry in manifest order, retaining only the next row ID.
    /// Errors leave the inheritance cursor unchanged.
    /// # Errors
    /// Rejects mixed content, missing required fields, negative values and overflow.
    pub fn resolve(&mut self, entry: ManifestEntry) -> Result<InheritedEntry, ManifestInheritanceError> {
        self.validate_entry(entry)?;
        let snapshot_id = match (self.version, entry.snapshot_id) {
            (_, Some(snapshot_id)) => snapshot_id,
            (ManifestVersion::V1, None) => return Err(ManifestInheritanceError::Missing),
            (_, None) => self.snapshot_id,
        };
        let data_sequence = self.sequence(entry.status, entry.data_sequence)?;
        let file_sequence = self.sequence(entry.status, entry.file_sequence)?;
        let first_row_id = if entry.content == FileContentKind::Data {
            entry.first_row_id.or(self.next_row_id)
        } else {
            None
        };
        let next = first_row_id
            .map(|first| {
                first
                    .checked_add(entry.record_count)
                    .ok_or(ManifestInheritanceError::Overflow)
            })
            .transpose()?;
        if entry.content == FileContentKind::Data && entry.first_row_id.is_none() {
            self.next_row_id = next;
        }
        Ok(InheritedEntry {
            snapshot_id,
            data_sequence,
            file_sequence,
            first_row_id,
        })
    }

    fn validate_entry(&self, entry: ManifestEntry) -> Result<(), ManifestInheritanceError> {
        let is_data = entry.content == FileContentKind::Data;
        if is_data != (self.content == ManifestContent::Data) || (!is_data && entry.first_row_id.is_some()) {
            return Err(ManifestInheritanceError::Content);
        }
        if entry.record_count < 0
            || entry.first_row_id.is_some_and(|value| value < 0)
            || entry.data_sequence.is_some_and(|value| value < 0)
            || entry.file_sequence.is_some_and(|value| value < 0)
        {
            return Err(ManifestInheritanceError::Number);
        }
        Ok(())
    }

    fn sequence(&self, status: EntryStatus, value: Option<i64>) -> Result<i64, ManifestInheritanceError> {
        match (self.version, value, status) {
            (ManifestVersion::V1, _, _) => Ok(0),
            (_, Some(value), _) => Ok(value),
            (_, None, EntryStatus::Added) => Ok(self.sequence),
            (_, None, _) => Err(ManifestInheritanceError::Missing),
        }
    }
}
