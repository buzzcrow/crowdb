// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "snapshot/published_byte_export_internal.h"
#include "snapshot/snapshot_wire.h"

#include <cstring>

namespace crowdb::tree
{
namespace
{
struct ChunkOutput
{
    ChunkOutput(size_t capacity, const std::shared_ptr<detail::AllocationReservation> &metadata)
        : bytes(capacity, detail::ReservationAllocator<uint8_t, AllocationKind::kPayload>(metadata))
    {
    }

    detail::ReservedVector<uint8_t, AllocationKind::kPayload> bytes;
};
} // namespace

Status PublishedByteExport::State::record_piece(std::span<const uint8_t> *out)
{
    if (format == snapshot_format::kNative) {
        while (!frame && !source_done) {
            auto status = native->next(&frame, &source_done);
            if (!status.ok()) {
                return status;
            }
        }
        if (frame) {
            snapshot_detail::put_u64(fields.data(), frame->page_id);
            snapshot_detail::put_u32(fields.data() + 8, static_cast<uint32_t>(frame->bytes.size()));
            *out = part == 0 ? std::span<const uint8_t>(fields).first(12) : frame->bytes;
            return Status::Ok();
        }
    }
    else {
        while (!entry && !source_done) {
            auto status = entries->next(&entry, &source_done);
            if (!status.ok()) {
                return status;
            }
        }
        if (entry) {
            auto status = snapshot_detail::entry_fields(*entry, &fields);
            if (!status.ok()) {
                return status;
            }
            switch (part) {
            case 0:
                *out = std::span<const uint8_t>(fields).first(4);
                break;
            case 1:
                *out = snapshot_detail::bytes(Slice(entry->key));
                break;
            case 2:
                *out = std::span<const uint8_t>(fields).subspan(4);
                break;
            default:
                *out = snapshot_detail::bytes(entry->cell.value);
                break;
            }
            return Status::Ok();
        }
    }
    phase = Phase::Trailer;
    return next_piece(out);
}

Status PublishedByteExport::State::next_piece(std::span<const uint8_t> *out)
{
    switch (phase) {
    case Phase::Header:
        *out = std::span(header).first(header_size);
        return Status::Ok();
    case Phase::Records:
        return record_piece(out);
    case Phase::Trailer:
        if (emitted_crc != info.final_crc || info.offset != info.total_bytes - 4 + piece_offset) {
            return Status::corruption("snapshot emission differs from its fixed preflight stream");
        }
        *out = trailer;
        return Status::Ok();
    case Phase::Done:
        *out = {};
        return Status::Ok();
    }
    return Status::internal_error("invalid snapshot wire phase");
}

void PublishedByteExport::State::consumed_piece()
{
    piece_offset = 0;
    switch (phase) {
    case Phase::Header:
        phase = Phase::Records;
        break;
    case Phase::Records:
        ++part;
        if (part == (format == snapshot_format::kNative ? 2U : 4U)) {
            part = 0;
            entry.reset();
            frame.reset();
            if (source_done) {
                phase = Phase::Trailer;
            }
        }
        break;
    case Phase::Trailer:
        phase = Phase::Done;
        break;
    case Phase::Done:
        break;
    }
}

Status PublishedByteExport::State::next(SnapshotChunk *out, bool *done)
{
    if (phase == Phase::Done) {
        *done = true;
        return Status::Ok();
    }
    // Cursor advancement may change stream fields, but never the capacity
    // allocated for this output window. Keep its subtraction bound local.
    const size_t capacity = info.chunk_bytes;
    if (capacity == 0 || capacity > kSnapshotChunkBytes || info.total_bytes < 4 || info.offset > info.total_bytes) {
        return Status::corruption("snapshot output has invalid fixed window geometry");
    }
    using detail::shared_allocation_bound;
    const auto bound =
        detail::AllocationBound(shared_allocation_bound<ChunkOutput>() +
                                shared_allocation_bound<detail::AllocationReservation>() + kSnapshotChunkHandleBytes)
            .add(capacity)
            .value();
    if (!bound) {
        return Status::resource_exhausted("snapshot chunk allocation bound overflow");
    }
    auto credit = detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kRouting, *bound);
    if (!credit) {
        return Status::resource_exhausted("snapshot chunk reserve unavailable");
    }
    auto   budget = detail::AllocationReservation::create(std::move(*credit));
    auto   buffer = detail::reserved_shared<ChunkOutput>(budget, capacity, budget);
    size_t used   = 0;
    while (used < capacity && phase != Phase::Done) {
        std::span<const uint8_t> piece;
        auto                     status = next_piece(&piece);
        if (!status.ok()) {
            return status;
        }
        if (piece_offset > piece.size()) {
            return Status::corruption("snapshot piece cursor exceeds its owner");
        }
        const auto take = std::min(capacity - used, piece.size() - piece_offset);
        if (info.offset > info.total_bytes || take > info.total_bytes - info.offset) {
            return Status::corruption("snapshot emission exceeds its preflight byte count");
        }
        if (used > buffer->bytes.size() || take > buffer->bytes.size() - used || take > kSnapshotChunkBytes) {
            return Status::corruption("snapshot piece exceeds its allocated output window");
        }
        const auto bytes = piece.subspan(piece_offset, take);
        if (take != 0) {
            const auto copy_bytes = static_cast<uint32_t>(take);
            std::memcpy(buffer->bytes.data() + used, bytes.data(), copy_bytes);
            record_work(budget, TreeWorkCounter::MaterializeCopyBytes, copy_bytes, TreeWorkCause::Snapshot);
        }
        if (phase != Phase::Trailer) {
            emitted_crc = snapshot_detail::crc_bytes(emitted_crc, bytes);
        }
        used += take;
        piece_offset += take;
        info.offset += take;
        if (piece_offset == piece.size()) {
            consumed_piece();
        }
    }
    *done = phase == Phase::Done;
    *out  = SnapshotChunk{.bytes             = std::span<const uint8_t>(buffer->bytes).first(used),
                          .owner             = std::move(buffer),
                          .allocation_budget = std::move(budget)};
    if (*done) {
        entries.reset();
        native.reset();
        snapshot.reset();
    }
    return Status::Ok();
}

Status PublishedByteExport::next(SnapshotChunk *out, bool *done)
{
    if (out == nullptr || done == nullptr) {
        return Status::invalid_argument("snapshot chunk requires result outputs");
    }
    *out  = {};
    *done = false;
    if (state_->busy.exchange(true, std::memory_order_acq_rel)) {
        return Status::unavailable("snapshot exporter already has an active window");
    }

    struct Leave
    {
        std::atomic<bool> &busy;

        ~Leave()
        {
            busy.store(false, std::memory_order_release);
        }
    } leave{state_->busy};

    if (!state_->failure.ok()) {
        return state_->failure;
    }
    try {
        auto status = state_->next(out, done);
        if (!status.ok()) {
            state_->failure = status.without_message();
        }
        return status;
    }
    catch (const std::bad_alloc &) {
        state_->failure = Status::resource_exhausted();
        return state_->failure;
    }
}
} // namespace crowdb::tree
