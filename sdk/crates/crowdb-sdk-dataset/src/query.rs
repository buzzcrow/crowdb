// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{BatchStream, DatasetClient, Error, ReadBinding, ReadPlan, Reclaim, SnapshotPage};
impl DatasetClient {
    /// Pins immutable versions without registering a read or saving progress.
    /// # Errors
    /// Returns invalid queries, missing snapshots or transport failures.
    pub async fn bind_read(&self, plan: &ReadPlan) -> Result<ReadBinding, Error> {
        self.call("POST", &["reads", "bind"], Some(plan)).await
    }
    /// The caller retains the current cursor until processing succeeds.
    /// # Errors
    /// Returns invalid continuations, deletion admission, capacity or service failures.
    pub async fn next_batch(&self, cursor: &str) -> Result<BatchStream, Error> {
        Ok(BatchStream::new(
            self.http
                .stream(
                    "POST",
                    &self.path(&["reads", "next"]),
                    Some(&serde_json::json!({"cursor":cursor})),
                )
                .await?,
        ))
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
