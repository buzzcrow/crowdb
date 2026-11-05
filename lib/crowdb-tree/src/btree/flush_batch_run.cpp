// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_batch_internal.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
std::atomic<uint64_t> next_run_identity{1};
}

Status FlushBatch::State::prepare_run(Range &range, bool inline_probe)
{
    auto bounds = range.bounds;
    if (range.next_key) {
        bounds.lower = range.next_key;
    }
    auto limits = options.run_limits;
    if (inline_probe) {
        limits.max_entries = std::min(limits.max_entries, options.inline_entries);
    }
    const auto byte_limit =
        inline_probe ? std::min(options.inline_bytes, options.max_run_bytes) : options.max_run_bytes;
    MemTableRangeMerge merge(std::span<const std::shared_ptr<MemTable>>(inputs->tables), std::move(bounds),
                             report.frontier, report.floor, limits, options.max_sources, metadata);
    if (!merge.status().ok()) {
        return merge.status();
    }
    OverlayBuilder builder(limits, metadata);
    uint64_t       entries     = 0;
    uint64_t       bytes       = 0;
    uint64_t       key_bytes   = 0;
    uint64_t       value_bytes = 0;
    while (merge.valid() && (entries == 0 || bytes < byte_limit)) {
        EntryRef entry;
        auto     status = merge.next(&entry);
        // The next key stays in the merge when its arena window is full.
        // A key that cannot fit even an empty window is a real error.
        if (status.code() == Code::kResourceExhausted && entries != 0) {
            break;
        }
        if (!status.ok()) {
            return status;
        }
        key_bytes += entry.key_length;
        value_bytes += entry.value().size();
        bytes += entry.key_length + entry.value().size() + kCellHeaderSize;
        status = builder.append(std::move(entry));
        if (!status.ok()) {
            return status;
        }
        ++entries;
    }
    // Re-seek under fresh thread-local guards on the next worker. Frozen
    // sources and the fixed P/F make the first unconsumed key a stable bookmark,
    // including empty/binary keys and duplicate keys in different sources.
    if (merge.valid() && merge.key().size() > options.run_limits.max_key_bytes) {
        return Status::resource_exhausted("flush continuation key exceeds its arena window");
    }
    auto next_key = merge.valid() ? std::make_optional<OwnedKey>(merge.key(), metadata) : std::nullopt;
    if (entries != 0) {
        std::shared_ptr<const OverlayBatch> run;
        auto status = builder.finish(next_run_identity.fetch_add(1, std::memory_order_relaxed), &run);
        if (!status.ok()) {
            return status;
        }
        range.runs.push_back(std::move(run));
    }
    // Advance only after every potentially failing allocation. A rejected
    // continuation retains the prepared prefix and resumes it on retry.
    range.next_key = std::move(next_key);
    range.entries += entries;
    range.bytes += bytes;
    range.ready = !range.next_key;
    if (entries != 0) {
        const auto &work = publisher.work_metrics();
        work->dispatch.run(entries, bytes);
        record_work(work, TreeWorkCounter::FlushCandidateVersions, merge.candidates(), TreeWorkCause::Flush);
        record_work(work, TreeWorkCounter::FlushWinningVersions, entries, TreeWorkCause::Flush);
        record_work(work, TreeWorkCounter::FlushWinningKeyBytes, key_bytes, TreeWorkCause::Flush);
        record_work(work, TreeWorkCounter::FlushWinningValueBytes, value_bytes, TreeWorkCause::Flush);
    }
    return Status::Ok();
}
} // namespace crowdb::tree
