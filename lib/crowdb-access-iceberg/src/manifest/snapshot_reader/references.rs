use crate::file::{FileLocation, FileRecord};
use crate::manifest::{
    ManifestContent, ManifestContext, ManifestListEntry, ManifestListReader, SnapshotManifestError as Error,
    SnapshotManifestSource,
};

type Resolved = (FileRecord, ManifestContext);

pub(super) enum References {
    List(Box<ManifestListReader>),
    Legacy {
        locations: std::vec::IntoIter<FileLocation>,
        snapshot_id: i64,
    },
}

impl References {
    pub(super) async fn next(
        &mut self,
        source: &dyn SnapshotManifestSource,
    ) -> Result<Option<(ManifestListEntry, Option<Resolved>)>, Error> {
        match self {
            Self::List(reader) => Ok(reader.next_entry().await?.map(|entry| (entry, None))),
            Self::Legacy {
                locations,
                snapshot_id,
            } => {
                let Some(location) = locations.next() else {
                    return Ok(None);
                };
                let (record, context) = source.resolve(&location).await?;
                if record.location != location {
                    return Err(Error::Unavailable);
                }
                let entry = ManifestListEntry {
                    location,
                    length: record.length,
                    partition_spec_id: context.spec_id(),
                    added_snapshot_id: *snapshot_id,
                    content: ManifestContent::Data,
                    sequence: 0,
                    min_sequence: 0,
                    file_counts: [None; 3],
                    row_counts: [None; 3],
                    first_row_id: None,
                    partitions: None,
                };
                Ok(Some((entry, Some((record, context)))))
            }
        }
    }
}
