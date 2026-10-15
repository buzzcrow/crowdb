// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::journal_position;
use super::{scan_page_bytes, ChunkKvService};
use crate::catalog::ClippedScan;
use crowdb_chunk_kv::{ChunkKvError, Partition};
use crowdb_protocol::chunk_kv::{ChunkKvResponse, ScanDirection, ScanRequest};

impl ChunkKvService {
    /// Handles strict forward scanning using the caller's byte budget at the tree boundary.
    pub async fn handle_bounded_scan(
        &self,
        request: ScanRequest,
        budget: usize,
        now_wall_ms: u64,
        now_monotonic_ms: u64,
    ) -> ChunkKvResponse {
        self.metrics.request();
        let response = if budget == 0 || request.direction != ScanDirection::Forward {
            super::failure(
                self.catalog.load().generation,
                crowdb_protocol::chunk_kv::ChunkKvRpcErrorCode::InvalidRequest,
                "strict scan requires a positive byte budget and forward direction".into(),
            )
        } else {
            self.handle_scan_inner(request, now_wall_ms, now_monotonic_ms, Some(budget))
                .await
        };
        self.metrics.response(&response);
        response
    }
    pub(super) async fn execute_scan(
        &self,
        partition: &Partition,
        request: &ScanRequest,
        clipped: &ClippedScan,
        strict_budget: Option<usize>,
    ) -> Result<crowdb_chunk_kv::ScanPage, ChunkKvError> {
        if let Some(ingress) = partition.split_ingress() {
            return self
                .execute_split_scan(&ingress, request, clipped, strict_budget)
                .await;
        }
        self.execute_partition_scan(
            partition,
            request,
            clipped,
            clipped.limit as usize,
            strict_budget.map_or(self.max_scan_response_bytes, |limit| {
                limit.min(self.max_scan_response_bytes)
            }),
            strict_budget.is_some(),
        )
        .await
    }

    async fn execute_split_scan(
        &self,
        ingress: &crowdb_chunk_kv::SplitIngress,
        request: &ScanRequest,
        clipped: &ClippedScan,
        strict_budget: Option<usize>,
    ) -> Result<crowdb_chunk_kv::ScanPage, ChunkKvError> {
        let writers = match clipped.direction {
            ScanDirection::Forward => [ingress.retained_parent(), ingress.child()],
            ScanDirection::Reverse => [ingress.child(), ingress.retained_parent()],
        };
        let mut entries = Vec::new();
        let mut remaining_bytes = strict_budget.map_or(self.max_scan_response_bytes, |limit| {
            limit.min(self.max_scan_response_bytes)
        });
        for (index, writer) in writers.iter().enumerate() {
            let remaining = (clipped.limit as usize).saturating_sub(entries.len());
            if remaining == 0 || remaining_bytes == 0 {
                return Ok(crowdb_chunk_kv::ScanPage {
                    entries,
                    truncated: true,
                });
            }
            let page = self
                .execute_partition_scan(
                    writer,
                    request,
                    clipped,
                    remaining,
                    remaining_bytes,
                    strict_budget.is_some(),
                )
                .await?;
            remaining_bytes = remaining_bytes.saturating_sub(scan_page_bytes(&page));
            entries.extend(page.entries);
            if page.truncated {
                return Ok(crowdb_chunk_kv::ScanPage {
                    entries,
                    truncated: true,
                });
            }
            if entries.len() == clipped.limit as usize && index + 1 < writers.len() {
                return Ok(crowdb_chunk_kv::ScanPage {
                    entries,
                    truncated: true,
                });
            }
        }
        Ok(crowdb_chunk_kv::ScanPage {
            entries,
            truncated: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_partition_scan(
        &self,
        partition: &Partition,
        request: &ScanRequest,
        clipped: &ClippedScan,
        limit: usize,
        byte_budget: usize,
        strict: bool,
    ) -> Result<crowdb_chunk_kv::ScanPage, ChunkKvError> {
        let minimum = request.routing.min_journal_position.map(journal_position);
        let writer_epoch = partition.snapshot().ownership_epoch;
        if strict {
            return partition
                .scan_forward_bounded(
                    writer_epoch,
                    clipped.resume_after.as_deref().unwrap_or(&clipped.start),
                    clipped.resume_after.is_none(),
                    clipped.end.as_deref(),
                    limit,
                    byte_budget,
                    minimum,
                )
                .await;
        }
        match clipped.direction {
            ScanDirection::Forward => {
                if let Some(resume_after) = clipped.resume_after.as_deref() {
                    partition
                        .scan_forward_after(
                            writer_epoch,
                            resume_after,
                            clipped.end.as_deref(),
                            limit,
                            byte_budget,
                            minimum,
                        )
                        .await
                } else {
                    partition
                        .scan_forward(
                            writer_epoch,
                            Some(&clipped.start),
                            clipped.end.as_deref(),
                            limit,
                            byte_budget,
                            minimum,
                        )
                        .await
                }
            }
            ScanDirection::Reverse => {
                let start_before = clipped.resume_after.as_deref().or(clipped.end.as_deref());
                partition
                    .scan_reverse(
                        writer_epoch,
                        start_before,
                        Some(&clipped.start),
                        limit,
                        byte_budget,
                        minimum,
                    )
                    .await
            }
        }
    }
}
