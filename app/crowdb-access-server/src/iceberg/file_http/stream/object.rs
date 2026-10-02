// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::storage::IcebergFileWriter;

use crate::upload_flow::digest_pipe::DigestPipe;

/// Per-object state, separate from the long-lived shared small pipeline.
/// Small checksums run on the receive task over the original payload views.
pub(super) struct SmallObjectWriter {
    writer: IcebergFileWriter,
    digest: DigestPipe,
}

pub(super) enum ObjectWriter {
    Small(SmallObjectWriter),
    Large {
        writer: IcebergFileWriter,
        digest: DigestPipe,
    },
}

impl ObjectWriter {
    pub(super) fn new(writer: IcebergFileWriter, small: bool, sha256: bool) -> Result<Self, ()> {
        if small {
            Ok(Self::Small(SmallObjectWriter {
                writer,
                digest: DigestPipe::start_inline(sha256)?,
            }))
        } else {
            Ok(Self::Large {
                writer,
                digest: DigestPipe::start(sha256),
            })
        }
    }

    pub(super) fn parts_mut(&mut self) -> (&mut IcebergFileWriter, &mut DigestPipe) {
        match self {
            Self::Small(small) => (&mut small.writer, &mut small.digest),
            Self::Large { writer, digest } => (writer, digest),
        }
    }

    pub(super) fn writer_mut(&mut self) -> &mut IcebergFileWriter {
        self.parts_mut().0
    }

    pub(super) fn digest_mut(&mut self) -> &mut DigestPipe {
        self.parts_mut().1
    }
}
