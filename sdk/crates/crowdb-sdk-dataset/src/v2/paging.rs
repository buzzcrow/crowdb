// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ControlClient, OpaqueId, Page, SampleId, SnapshotSummary, SplitRevisionSummary, SplitSource};
use crate::Error;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;

#[derive(Clone, Debug)]
pub enum PageKind {
    Datasets,
    Inventory,
    Roots,
    Children(OpaqueId),
    Schema(OpaqueId),
    Samples(OpaqueId),
    SplitHistory(OpaqueId),
    SplitMembers(SplitSource),
}
impl PageKind {
    fn suffix(&self) -> Vec<&str> {
        match self {
            Self::Datasets => vec![],
            Self::Inventory => vec!["snapshot-inventory"],
            Self::Roots => vec!["roots"],
            Self::Children(id) => vec!["snapshots", id.as_str(), "children"],
            Self::Schema(id) => vec!["snapshots", id.as_str(), "schema"],
            Self::Samples(id) => vec!["snapshots", id.as_str(), "samples"],
            Self::SplitHistory(id) => vec!["snapshots", id.as_str(), "split-revisions"],
            Self::SplitMembers(source) => vec![
                "snapshots",
                source.snapshot.as_str(),
                "split-revisions",
                source.revision.as_str(),
                "splits",
                &source.name,
                "members",
            ],
        }
    }
    fn capability(&self) -> &'static str {
        match self {
            Self::SplitHistory(_) | Self::SplitMembers(_) => "split-revisions",
            _ => "bounded-inspection",
        }
    }
}
/// Lazy bounded page traversal. Construction performs no requests. A failed or
/// cancelled page fetch preserves the cursor; dropping it stops further work.
pub struct PageCursor<T> {
    client: ControlClient,
    kind: PageKind,
    limit: u16,
    after: Option<String>,
    done: bool,
    marker: PhantomData<T>,
}
impl<T: DeserializeOwned> PageCursor<T> {
    fn new(client: ControlClient, kind: PageKind, limit: u16) -> Result<Self, Error> {
        if !(1..=100).contains(&limit) {
            return Err(Error::Invalid("page size must be 1 through 100".into()));
        }
        Ok(Self {
            client,
            kind,
            limit,
            after: None,
            done: false,
            marker: PhantomData,
        })
    }
    /// # Errors
    /// Returns capability, stale cursor, malformed page, limit or service errors.
    pub async fn next_page(&mut self) -> Result<Option<Page<T>>, Error> {
        if self.done {
            return Ok(None);
        }
        self.client.require(self.kind.capability()).await?;
        let limit = self.limit.to_string();
        let mut query = vec![("limit", limit.as_str())];
        if let Some(after) = &self.after {
            query.push(("after", after));
        }
        let suffix = self.kind.suffix();
        let mut path = self.client.path(&suffix);
        if matches!(self.kind, PageKind::Datasets) {
            path.pop();
        }
        let page: Page<T> = self.client.inner.http.json_query("GET", &path, &query).await?;
        if page.entries.len() > usize::from(self.limit)
            || page.after.as_ref().is_some_and(|after| {
                after.is_empty() || after.len() > 43692 || self.after.as_ref() == Some(after)
            })
        {
            return Err(Error::Invalid("invalid continuation page".into()));
        }
        self.after.clone_from(&page.after);
        self.done = page.after.is_none();
        Ok(Some(page))
    }
    #[must_use]
    pub fn continuation(&self) -> Option<&str> {
        self.after.as_deref()
    }
    /// Resume with a server-issued continuation for this exact scope.
    /// # Errors
    /// Rejects an empty or oversized cursor before sending it.
    pub fn resume_after(&mut self, cursor: String) -> Result<(), Error> {
        if cursor.is_empty() || cursor.len() > 43692 {
            return Err(Error::Invalid("page cursor".into()));
        }
        self.after = Some(cursor);
        self.done = false;
        Ok(())
    }
}
impl ControlClient {
    /// # Errors
    /// Rejects a page size outside 1 through 100.
    pub fn datasets(&self, limit: u16) -> Result<PageCursor<crate::DatasetInfo>, Error> {
        PageCursor::new(self.clone(), PageKind::Datasets, limit)
    }
    /// # Errors
    /// Rejects a page size outside 1 through 100.
    pub fn inventory(&self, limit: u16) -> Result<PageCursor<SnapshotSummary>, Error> {
        PageCursor::new(self.clone(), PageKind::Inventory, limit)
    }
    /// # Errors
    /// Rejects a page size outside 1 through 100.
    pub fn roots(&self, limit: u16) -> Result<PageCursor<SnapshotSummary>, Error> {
        PageCursor::new(self.clone(), PageKind::Roots, limit)
    }
    /// # Errors
    /// Rejects a page size outside 1 through 100.
    pub fn children(&self, snapshot: OpaqueId, limit: u16) -> Result<PageCursor<SnapshotSummary>, Error> {
        PageCursor::new(self.clone(), PageKind::Children(snapshot), limit)
    }
    /// # Errors
    /// Rejects a page size outside 1 through 100.
    pub fn schema(
        &self,
        snapshot: OpaqueId,
        limit: u16,
    ) -> Result<PageCursor<crate::FieldDefinition>, Error> {
        PageCursor::new(self.clone(), PageKind::Schema(snapshot), limit)
    }
    /// # Errors
    /// Rejects a page size outside 1 through 100.
    pub fn samples(&self, snapshot: OpaqueId, limit: u16) -> Result<PageCursor<SampleId>, Error> {
        PageCursor::new(self.clone(), PageKind::Samples(snapshot), limit)
    }
    /// # Errors
    /// Rejects a page size outside 1 through 100.
    pub fn split_history(
        &self,
        snapshot: OpaqueId,
        limit: u16,
    ) -> Result<PageCursor<SplitRevisionSummary>, Error> {
        PageCursor::new(self.clone(), PageKind::SplitHistory(snapshot), limit)
    }
    /// # Errors
    /// Rejects a page size outside 1 through 100.
    pub fn split_members(&self, source: SplitSource, limit: u16) -> Result<PageCursor<SampleId>, Error> {
        PageCursor::new(self.clone(), PageKind::SplitMembers(source), limit)
    }
}
