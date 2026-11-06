// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/btree/work_cause.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

namespace crowdb::tree
{
// Closed dimensions. Quantities count completed physical work or committed
// logical results, never planned capacity. Operation outcomes remain in stages.
enum class TreeWorkCounter : uint8_t {
    FlushInputTables,
    FlushRangeJobs,
    FlushCandidateVersions,
    FlushWinningVersions,
    FlushWinningKeyBytes,
    FlushWinningValueBytes,
    FlushHandoffBytes,
    SourceDetached,
    CandidateChecks,
    CandidateSkips,
    PublicationMismatches,
    SplitOperations,
    SplitOutputPages,
    MergeOperations,
    MergeInputPages,
    MergeOutputPages,
    CoalesceOperations,
    CoalesceInputRuns,
    CoalesceOutputRuns,
    CoalesceInputEntries,
    CoalesceOutputEntries,
    PayloadReferencesDropped,
    MemoryReclaimedBytes,
    KeyCopyBytes,
    HandoffValueCopyBytes,
    CowMetadataCopyBytes,
    MaterializeCopyBytes,
    BackendBounceCopyBytes,
    EncodedFrameBytes,
    EncodedPrimaryFrames,
    EncodedOverflowFrames,
    EncodingKeyCopyBytes,
    EncodingValueCopyBytes,
    ReadOperations,
    ReadBytes,
    ReadErrors,
    WriteOperations,
    WriteBytes,
    WriteErrors,
    ReusedFrames,
    ReusedBackingBytes,
    ProxyAvoidedLoads,
    CacheHits,
    CacheMisses,
    CacheJoinedLoads,
    CacheEvictions,
    StaleCallbacks,
    RebaseMismatches,
    AdmissionRejected,
    AdmissionWaits,
    SnapshotRetirementDelays,
    BackingDeletedBytes,
    CheckpointFailures,
    Retries,
    LogicalRetirementBytes,
    Count
};
inline constexpr size_t kTreeWorkCounterCount = static_cast<size_t>(TreeWorkCounter::Count);
inline constexpr size_t kTreeWorkCauseCount   = static_cast<size_t>(TreeWorkCause::Count);
using TreeCounterValues                       = std::array<uint64_t, kTreeWorkCounterCount>;
using TreeCounterTotals                       = std::array<TreeCounterValues, kTreeWorkCauseCount>;
inline constexpr std::array<std::string_view, kTreeWorkCounterCount> kTreeWorkCounterNames{
    "flush.input.tables",
    "flush.range.jobs",
    "flush.candidate.versions",
    "flush.winning.versions",
    "flush.winning.key.bytes",
    "flush.winning.value.bytes",
    "flush.handoff.bytes",
    "source.detached",
    "candidate.checks",
    "candidate.skips",
    "publication.mismatches",
    "split.operations",
    "split.output.pages",
    "merge.operations",
    "merge.input.pages",
    "merge.output.pages",
    "coalesce.operations",
    "coalesce.input.runs",
    "coalesce.output.runs",
    "coalesce.input.entries",
    "coalesce.output.entries",
    "payload.references.dropped",
    "memory.reclaimed.bytes",
    "key.copy.bytes",
    "handoff.value.copy.bytes",
    "cow.metadata.copy.bytes",
    "materialize.copy.bytes",
    "backend.bounce.copy.bytes",
    "encode.frame.bytes",
    "encode.primary.frames",
    "encode.overflow.frames",
    "encode.key.copy.bytes",
    "encode.value.copy.bytes",
    "read.operations",
    "read.bytes",
    "read.errors",
    "write.operations",
    "write.bytes",
    "write.errors",
    "reused.frames",
    "reused.backing.bytes",
    "proxy.avoided.loads",
    "cache.hits",
    "cache.misses",
    "cache.joined.loads",
    "cache.evictions",
    "callbacks.stale",
    "rebase.mismatches",
    "admission.rejected",
    "admission.waits",
    "snapshot.retirement.delays",
    "backing.deleted.bytes",
    "checkpoint.failures",
    "retries",
    "memory.logical.retirement.bytes",
};
inline constexpr std::array<std::string_view, kTreeWorkCauseCount> kTreeWorkCauseNames{
    "other", "flush", "cache_pressure", "checkpoint", "gc", "read", "snapshot", "allocation"};

class TreeWorkCounters
{
  public:
    void add(TreeWorkCounter counter, uint64_t amount = 1, TreeWorkCause cause = TreeWorkCause::Other) noexcept;
    [[nodiscard]] TreeCounterTotals snapshot() const noexcept;

  private:
    std::array<std::array<std::atomic<uint64_t>, kTreeWorkCounterCount>, kTreeWorkCauseCount> values_{};
};
class TreeWorkMetrics;
class AllocationBudget;

namespace detail
{
class AllocationReservation;
}

void record_work(const std::shared_ptr<TreeWorkMetrics> &metrics, TreeWorkCounter counter, uint64_t amount = 1,
                 TreeWorkCause cause = TreeWorkCause::Other) noexcept;
void record_work(const std::shared_ptr<detail::AllocationReservation> &metadata, TreeWorkCounter counter,
                 uint64_t amount = 1, TreeWorkCause cause = TreeWorkCause::Other) noexcept;
void record_work(const std::shared_ptr<AllocationBudget> &budget, TreeWorkCounter counter, uint64_t amount = 1,
                 TreeWorkCause cause = TreeWorkCause::Other) noexcept;
} // namespace crowdb::tree
