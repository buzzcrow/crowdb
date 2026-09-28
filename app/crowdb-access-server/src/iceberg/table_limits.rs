use crowdb_access_iceberg::{
    commit::{
        CandidateAuxiliaryLimits, CandidateSnapshotLimits, CommitPreparationLimits, CommitProofLimits,
        CommitRequestLimits, EvaluationLimits, PriorManifestLimits, RequirementLimits,
    },
    file::{AvroDatumLimits, AvroLimits, DeletionVectorLimits, ParquetMetadataLimits, ParquetPageLimits},
    manifest::{
        PositionDeleteLimits, SnapshotDvLimits, SnapshotFileLimits, SnapshotIdentityLimits,
        SnapshotManifestLimits,
    },
    table::TableMetadataLimits,
};

pub(super) const RESPONSE_RESERVE: usize = 64 * 1024;

pub(super) fn metadata() -> TableMetadataLimits {
    TableMetadataLimits {
        bytes: 2 * 1024 * 1024 - RESPONSE_RESERVE,
        values: 200_000,
        depth: 64,
        string_bytes: 1024 * 1024,
        collection_entries: 10_000,
    }
}

pub(super) fn commits() -> CommitProofLimits {
    let manifests = SnapshotManifestLimits {
        framing: AvroLimits {
            header_bytes: 256 * 1024,
            metadata_entries: 64,
            block_bytes: 4 * 1024 * 1024,
            records_per_block: 100_000,
        },
        datum: AvroDatumLimits {
            depth: 64,
            values: 1_000_000,
            value_bytes: 1024 * 1024,
        },
        decoded_bytes: 8 * 1024 * 1024,
        manifests: 1000,
        entries: 100_000,
        manifest_bytes: 64 * 1024 * 1024,
        identity: SnapshotIdentityLimits {
            keys: 100_000,
            key_bytes: 16 * 1024 * 1024,
        },
    };
    let parquet = ParquetMetadataLimits {
        footer_bytes: 1024 * 1024,
        values: 100_000,
        depth: 32,
        schema_elements: 4096,
        row_groups: 10_000,
    };
    CommitProofLimits {
        preparation: CommitPreparationLimits {
            request: CommitRequestLimits {
                json: metadata(),
                requirements: 1000,
                updates: 1000,
            },
            evaluation: EvaluationLimits {
                metadata: metadata(),
                requirements: RequirementLimits {
                    count: 1000,
                    text_bytes: 4096,
                },
                updates: 1000,
                work_bytes: 16 * 1024 * 1024,
            },
        },
        prior: PriorManifestLimits {
            snapshots: 1000,
            references: 100_000,
            index_bytes: 16 * 1024 * 1024,
            manifests,
        },
        snapshots: CandidateSnapshotLimits {
            snapshots: 1000,
            entries: 100_000,
            manifest_bytes: 64 * 1024 * 1024,
            ranges: 100_000,
            files: SnapshotFileLimits {
                manifests,
                data_files: 100_000,
                index_bytes: 16 * 1024 * 1024,
                position_deletes: PositionDeleteLimits {
                    metadata: parquet,
                    page: ParquetPageLimits {
                        bytes: 8 * 1024 * 1024,
                        values: 1_000_000,
                        pages: 100_000,
                    },
                    rows: 1_000_000,
                },
                vectors: SnapshotDvLimits {
                    vectors: 10_000,
                    blob_bytes: 16 * 1024 * 1024,
                    vector: DeletionVectorLimits {
                        blob_bytes: 16 * 1024 * 1024,
                        bitmaps: 100_000,
                    },
                },
                delete_rows: 1_000_000,
            },
        },
        auxiliary: CandidateAuxiliaryLimits {
            manifests,
            files: 1000,
            bytes: 64 * 1024 * 1024,
            work: 1_000_000,
            puffin_encoded_bytes: 1024 * 1024,
            puffin_decoded_bytes: 1024 * 1024,
            parquet,
            partition_rows: crowdb_access_iceberg::manifest::PartitionStatisticsRowLimits {
                page: ParquetPageLimits {
                    bytes: 1024 * 1024,
                    values: 100_000,
                    pages: 100_000,
                },
                rows: 1_000_000,
                buffered_bytes: 64 * 1024 * 1024,
            },
        },
    }
}
