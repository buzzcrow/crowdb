// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

pub use crate::multipart_complete::{CompletePart, CompleteRequestError, CompleteSelection};
use crowdb_access_iceberg::catalog::CatalogError;
use crowdb_access_iceberg::file::{
    MultipartPhase, MultipartRepository, MultipartSelection, MultipartSession, SelectedPart,
};

#[derive(Debug, thiserror::Error)]
pub enum CompleteResolveError {
    #[error("multipart selection does not match durable parts")]
    InvalidPart,
    #[error("a nonfinal multipart part is smaller than 5 MiB")]
    EntityTooSmall,
    #[error(transparent)]
    Catalog(#[from] CatalogError),
}

impl CompleteSelection {
    /// Resolves the selected parts against one current durable session snapshot.
    /// # Errors
    /// Rejects missing, replaced or differently hashed parts and storage failures.
    pub async fn resolve(
        &self,
        repository: &MultipartRepository,
        session: &MultipartSession,
    ) -> Result<MultipartSelection, CompleteResolveError> {
        if self.parts().len() > usize::from(session.limits.max_parts) {
            return Err(CompleteResolveError::InvalidPart);
        }
        if session.completion.is_some() {
            let frozen = repository.load_selection(session).await?;
            if let Some(snapshots) = frozen.snapshots() {
                if self.parts().len() != snapshots.len()
                    || self.parts().iter().zip(frozen.parts().iter().zip(snapshots)).any(
                        |(requested, (selected, snapshot))| {
                            requested.number != selected.number || requested.etag != snapshot.etag
                        },
                    )
                {
                    return Err(CompleteResolveError::InvalidPart);
                }
                return Ok(frozen);
            }
        }
        let mut selected = Vec::with_capacity(self.parts().len());
        let mut parts = Vec::with_capacity(self.parts().len());
        for (index, requested) in self.parts().iter().enumerate() {
            let part = if session.phase == MultipartPhase::Open {
                repository.part_for_upload(session, requested.number).await?
            } else {
                repository.part(session, requested.number).await?
            }
            .ok_or(CompleteResolveError::InvalidPart)?;
            if part.etag() != requested.etag {
                return Err(CompleteResolveError::InvalidPart);
            }
            if index + 1 < self.parts().len() && part.length() < 5 * 1024 * 1024 {
                return Err(CompleteResolveError::EntityTooSmall);
            }
            selected.push(SelectedPart {
                number: part.number,
                revision: part.revision,
                digest: part.selection_digest(),
            });
            parts.push(part);
        }
        if parts.iter().all(|part| part.stream.is_some()) {
            MultipartSelection::with_stream_parts(&parts).map_err(|_| CompleteResolveError::InvalidPart)
        } else {
            MultipartSelection::new(selected).map_err(|_| CompleteResolveError::InvalidPart)
        }
    }
}
