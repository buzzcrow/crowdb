// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Discard protection only after its candidate can no longer win the head CAS.

use crowdb_protocol::chunk_stream::{StreamDirectoryControl, StreamPublicationId};
use crowdb_protocol::key::{BinaryKey, StreamPublicationGuardKey};

use super::{DirectoryHeadSnapshot, DirectoryMetadataStore};
use crate::{Result, StreamError, MAX_DIRECTORY_SCAN_KEYS};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryPublicationRetirement {
    pub removed: usize,
    pub next: Option<StreamPublicationId>,
}

impl DirectoryMetadataStore {
    /// Removes bounded resolved/superseded publication guards. Head generations
    /// increase strictly, so a candidate at or below the current generation can
    /// no longer win its original CAS. A newer candidate remains protected.
    /// # Errors
    /// Rejects stale/wrong-scope heads, invalid budgets, corrupt guards or a
    /// concurrent retention change. Enumeration continues after the returned ID.
    pub async fn retire_directory_publications(
        &self,
        expected: &DirectoryHeadSnapshot,
        after: Option<StreamPublicationId>,
        max_guards: usize,
    ) -> Result<DirectoryPublicationRetirement> {
        if expected.store_id != self.store_id
            || expected.head.metadata_group_id != self.group_id
            || max_guards == 0
            || max_guards > MAX_DIRECTORY_SCAN_KEYS
        {
            return Err(StreamError::InvalidRequest(
                "publication retirement binding or budget is invalid".into(),
            ));
        }
        let name = expected.head.stream_name;
        let (control, revision) = self
            .load_directory_control(StreamDirectoryControl {
                stream_name: name,
                purpose: expected.head.purpose,
            })
            .await?;
        if revision == 0 {
            return Err(StreamError::ReadUnavailable(
                "root retention control is missing".into(),
            ));
        }
        let current = self.load_directory_head(name).await?;
        if !current.is_some_and(|head| head.revision == expected.revision && head.head == expected.head) {
            return Err(StreamError::Backpressure);
        }
        let page = self.scan_directory_publications(name, after, max_guards).await?;
        let mut keys = Vec::with_capacity(page.entries.len());
        for id in page.entries {
            let guard = self
                .load_directory_publication(name, id)
                .await?
                .ok_or(StreamError::Backpressure)?;
            if guard.head.purpose != expected.head.purpose {
                return Err(StreamError::Corruption(
                    "publication retirement purpose is invalid".into(),
                ));
            }
            if guard.head.generation <= expected.head.generation {
                keys.push(
                    StreamPublicationGuardKey {
                        stream_name: name,
                        publication_id: id,
                    }
                    .to_bytes(),
                );
            }
        }
        self.delete_retention_records(control, revision, &keys).await?;
        Ok(DirectoryPublicationRetirement {
            removed: keys.len(),
            next: page.next,
        })
    }
}
