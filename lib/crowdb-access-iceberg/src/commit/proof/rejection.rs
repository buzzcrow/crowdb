use crate::{
    file::{
        AvroContainerError, DeletionVectorError, FormatProbeError, ParquetMetadataError, PuffinMetadataError,
    },
    manifest::{
        ManifestContextError, ManifestEntryError, ManifestListError, ManifestMetadataError,
        SelectedParquetError, SnapshotDvError, SnapshotManifestError, SnapshotValidationError,
    },
};

pub(in crate::commit) fn invalid_files(error: &SnapshotValidationError) -> bool {
    match error {
        SnapshotValidationError::Parquet(SelectedParquetError::Metadata(error)) => parquet(error),
        SnapshotValidationError::Manifest(error) => manifest(error),
        SnapshotValidationError::Vector(SnapshotDvError::Vector(error)) => vector(error),
        SnapshotValidationError::Vector(SnapshotDvError::Incomplete) => false,
        SnapshotValidationError::Binding
        | SnapshotValidationError::Bounds
        | SnapshotValidationError::Unsupported
        | SnapshotValidationError::Unavailable
        | SnapshotValidationError::Parquet(_)
        | SnapshotValidationError::Vector(_) => true,
        SnapshotValidationError::Source(error) => source(error.as_ref()),
    }
}

fn source(error: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    if error.is::<ManifestMetadataError>() || error.is::<ManifestContextError>() {
        return true;
    }
    if let Some(error) = error.downcast_ref::<AvroContainerError>() {
        return avro(error);
    }
    if let Some(error) = error.downcast_ref::<ParquetMetadataError>() {
        return parquet(error);
    }
    if let Some(error) = error.downcast_ref::<PuffinMetadataError>() {
        return puffin(error);
    }
    if let Some(error) = error.downcast_ref::<FormatProbeError>() {
        return matches!(error, FormatProbeError::Container);
    }
    if let Some(error) = error.downcast_ref::<DeletionVectorError>() {
        return vector(error);
    }
    false
}

fn manifest(error: &SnapshotManifestError) -> bool {
    match error {
        SnapshotManifestError::List(ManifestListError::Avro(error))
        | SnapshotManifestError::Manifest(ManifestEntryError::Avro(error)) => avro(error),
        SnapshotManifestError::List(_)
        | SnapshotManifestError::Manifest(_)
        | SnapshotManifestError::Bounds
        | SnapshotManifestError::RowIds
        | SnapshotManifestError::Identity(_)
        | SnapshotManifestError::Unavailable => true,
        SnapshotManifestError::Source(error) => source(error.as_ref()),
        SnapshotManifestError::Incomplete => false,
    }
}

fn avro(error: &AvroContainerError) -> bool {
    matches!(
        error,
        AvroContainerError::Framing
            | AvroContainerError::Bounds
            | AvroContainerError::Codec
            | AvroContainerError::Schema
    )
}

fn parquet(error: &ParquetMetadataError) -> bool {
    matches!(
        error,
        ParquetMetadataError::Invalid
            | ParquetMetadataError::Bounds
            | ParquetMetadataError::Unsupported
            | ParquetMetadataError::Probe(FormatProbeError::Container)
    )
}

fn puffin(error: &PuffinMetadataError) -> bool {
    matches!(
        error,
        PuffinMetadataError::Invalid
            | PuffinMetadataError::Bounds
            | PuffinMetadataError::Probe(FormatProbeError::Container)
    )
}

fn vector(error: &DeletionVectorError) -> bool {
    match error {
        DeletionVectorError::Metadata(error) => puffin(error),
        DeletionVectorError::Invalid | DeletionVectorError::Bounds => true,
        DeletionVectorError::Storage(_) => false,
    }
}
