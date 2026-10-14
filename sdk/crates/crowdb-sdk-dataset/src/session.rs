// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    Acknowledge, BatchStream, DatasetClient, Error, Progress, ReadPlan, ReadSession, Reclaim, SnapshotPage,
};
impl DatasetClient {
    /// # Errors
    /// Returns invalid plans, conflicts, capacity or service failures.
    pub async fn start_read(&self, plan: &ReadPlan) -> Result<ReadSession, Error> {
        self.call("POST", &["read-plans"], Some(plan)).await
    }
    /// # Errors
    /// Returns closed/reclaimed sessions or transport failures.
    pub async fn resume_read(&self, id: &str) -> Result<ReadSession, Error> {
        self.call::<(), _>("GET", &["read-plans", id], None).await
    }
    /// # Errors
    /// Returns cancellation, integrity, capacity or service failures.
    pub async fn next_batch(&self, id: &str) -> Result<BatchStream, Error> {
        Ok(BatchStream::new(
            self.http
                .stream("POST", &self.path(&["read-plans", id, "next"]), Some(&()))
                .await?,
        ))
    }
    /// # Errors
    /// Returns stale/forged progress or service/transport failures.
    pub async fn acknowledge(&self, id: &str, ack: &Acknowledge) -> Result<Progress, Error> {
        self.call("PUT", &["read-plans", id, "progress"], Some(ack)).await
    }
    /// # Errors
    /// Returns missing/closed sessions or service failures.
    pub async fn progress(&self, id: &str) -> Result<Progress, Error> {
        self.call::<(), _>("GET", &["read-plans", id, "progress"], None)
            .await
    }
    /// # Errors
    /// Returns missing sessions or service failures. Cancels only this session.
    pub async fn close_read(&self, id: &str) -> Result<ReadSession, Error> {
        self.call("DELETE", &["read-plans", id], Some(&())).await
    }
    /// # Errors
    /// Returns protected/missing snapshots or service failures. Deferred cleanup remains pending.
    pub async fn reclaim(&self, id: &str) -> Result<Reclaim, Error> {
        self.call("POST", &["snapshots", id, "reclaim"], Some(&())).await
    }
    /// # Errors
    /// Returns missing datasets or service failures.
    pub async fn reclaim_status(&self, id: &str) -> Result<Reclaim, Error> {
        self.call::<(), _>("GET", &["snapshots", id, "reclaim"], None)
            .await
    }
    /// Returns one bounded ancestry page; `after` is the preceding page's next token.
    /// # Errors
    /// Returns invalid bounds/cursors or service failures.
    pub async fn list_snapshots(&self, after: Option<&str>, limit: usize) -> Result<SnapshotPage, Error> {
        if !(1..=1000).contains(&limit) {
            return Err(Error::Invalid("snapshot page size".into()));
        }
        let size = limit.to_string();
        let mut query = vec![("limit", size.as_str())];
        if let Some(after) = after {
            query.push(("after", after));
        }
        self.http
            .json_query("GET", &self.path(&["snapshots"]), &query)
            .await
    }
}
