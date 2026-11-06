// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_core.h"
#include "snapshot/published_byte_export_internal.h"
#include "snapshot/snapshot_wire.h"

namespace crowdb::tree
{
void PublishedByteExport::State::encode_header()
{
    snapshot_detail::put_u32(header.data(), 0x4e535443U);
    snapshot_detail::put_u32(header.data() + 4, 1);
    header[8] = static_cast<uint8_t>(format);
    snapshot_detail::put_u64(header.data() + 9, info.slot);
    if (format == snapshot_format::kPortable) {
        snapshot_detail::put_u64(header.data() + 17, count);
        header_size = 25;
    }
    else {
        snapshot_detail::put_u64(header.data() + 17, root);
        snapshot_detail::put_u64(header.data() + 25, count);
        header_size = 33;
    }
}

Status PublishedByteExport::create(const std::shared_ptr<TreeCore> &core, snapshot_format format, size_t chunk_bytes,
                                   std::shared_ptr<PublishedByteExport> *out)
{
    return create(core, format, chunk_bytes, out, KeyRange::unbounded());
}

Status PublishedByteExport::create(const std::shared_ptr<TreeCore> &core, snapshot_format format, size_t chunk_bytes,
                                   std::shared_ptr<PublishedByteExport> *out, const KeyRange &range)
try {
    if (!core || out == nullptr || *out || chunk_bytes > kSnapshotChunkBytes) {
        return Status::invalid_argument("snapshot export needs an empty output and a bounded chunk size");
    }
    if (!range.validate().ok() || (format == snapshot_format::kNative && range.is_bounded())) {
        return Status::invalid_argument("a selected key range requires portable export");
    }
    if (format != snapshot_format::kPortable && format != snapshot_format::kNative) {
        return Status::not_supported("unknown snapshot wire format");
    }
    using detail::shared_allocation_bound;
    const auto bound = shared_allocation_bound<State>() + shared_allocation_bound<PublishedByteExport>() +
                       shared_allocation_bound<detail::AllocationReservation>() + sizeof(SnapshotExport);
    auto credit = detail::ResourceBudget::try_reserve(core->binding()->account(), detail::BudgetClass::kRouting, bound);
    if (!credit) {
        return Status::resource_exhausted("snapshot export control reserve unavailable");
    }
    auto metadata           = detail::AllocationReservation::create(std::move(*credit));
    auto state              = detail::reserved_shared<State>(metadata);
    state->metadata         = metadata;
    state->binding          = core->binding();
    state->format           = format;
    state->info.chunk_bytes = chunk_bytes == 0 ? kSnapshotChunkBytes : chunk_bytes;
    Status status;
    if (format == snapshot_format::kNative) {
        status = PublishedNativeCursor::create(core, {}, &state->native);
        if (status.ok()) {
            const auto &native = state->native->info();
            state->info.slot   = native.at_slot;
            state->root        = native.root_page_id;
            state->count       = native.frame_count;
        }
    }
    else {
        status = core->flush();
        if (status.ok()) {
            status = core->capture_snapshot(&state->snapshot);
        }
        if (status.ok()) {
            state->info.slot = state->snapshot->view()->coverage;
        }
    }
    if (status.ok()) {
        status = state->preflight(range);
    }
    if (!status.ok()) {
        return status;
    }
    *out = detail::reserved_construct<PublishedByteExport>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) PublishedByteExport(std::move(state)); });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot export allocation failed");
}

const SnapshotByteInfo &PublishedByteExport::info() const noexcept
{
    return state_->info;
}

const std::shared_ptr<detail::AllocationReservation> &PublishedByteExport::metadata() const noexcept
{
    return state_->metadata;
}
} // namespace crowdb::tree
