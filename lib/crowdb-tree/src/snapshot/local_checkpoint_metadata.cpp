// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/maptable/mapping_slot.h"
#include "snapshot/local_checkpoint_internal.h"

#include <algorithm>

namespace crowdb::tree
{
Status LocalCheckpointOperation::State::pack_frame(const DurableCapturedFrame &frame, uint64_t *word)
{
    const auto &domain  = registry->ledger_->limits();
    const auto &backing = frame.backing;
    if (!backing || backing->storage != BackingStorage::kDurableBlob || !backing->reloadable() ||
        backing->backend_id != domain.backend_id || backing->generation != domain.generation ||
        backing->address < domain.region_begin || backing->address % iu != 0 || backing->length % iu != 0 ||
        backing->address > UINT64_MAX - backing->length || backing->address / iu > slot_word::kMaxIuIndex ||
        backing->length / iu > slot_word::kMaxIuCount || frame.logical_bytes == 0 ||
        frame.logical_bytes > backing->length || frame.page_id >= image->pages->next_page_id) {
        return Status::corruption();
    }
    if (frame.type == page_type::kLeafBase) {
        ++image->anchor.leaf_count;
    }
    else if (frame.type == page_type::kInnerBase) {
        ++image->anchor.inner_count;
    }
    else if (frame.type != page_type::kOverflowFrame || frame.page_id == image->pages->root_page_id) {
        return Status::corruption();
    }
    *word = slot_word::pack_unloaded(backing->address / iu, static_cast<uint32_t>(backing->length / iu));
    return Status::Ok();
}

void LocalCheckpointOperation::State::segment()
{
    if (position == image->pages->frames.end()) {
        encoder.emplace(directory);
        image->anchor.segdir_len = encoder->logical_bytes();
        reserve(round_up_to_iu(encoder->logical_bytes(), iu), Stage::DirectoryReserved);
        return;
    }
    if (directory.size() == limits.max_segments) {
        finish(Status::resource_exhausted());
        return;
    }
    const auto index = position->first / kMappingSegmentSize;
    if (index >= kMaxMappingSegments) {
        finish(Status::invalid_argument());
        return;
    }
    words.fill(0);
    uint32_t live = 0;
    while (position != image->pages->frames.end() && position->first / kMappingSegmentSize == index) {
        const auto &[pid, frame] = *position;
        auto status =
            pid == frame.page_id ? pack_frame(frame, &words[pid % kMappingSegmentSize]) : Status::corruption();
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        ++live;
        ++position;
    }
    const SegmentImageHeader header{.seg_idx    = static_cast<uint32_t>(index),
                                    .generation = image->anchor.snapshot_seq,
                                    .slot_count = kMappingSegmentSize,
                                    .live_count = live};
    std::ranges::fill(window->bytes, uint8_t{0});
    segment_entry = {.seg_idx    = header.seg_idx,
                     .generation = header.generation,
                     .image_len  = static_cast<uint32_t>(segment_image_encoded_size(header.slot_count))};
    auto status   = snapshot::encode_segment_into(header, words, window->bytes, &segment_entry.image_crc);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    reserve(segment_bytes, Stage::SegmentReserved);
}

void LocalCheckpointOperation::State::directory_window()
{
    if (!encoder) {
        finish(Status::internal_error());
        return;
    }
    std::ranges::fill(window->bytes, uint8_t{0});
    size_t written = 0;
    auto   status  = encoder->next(window->bytes, &written);
    if (!status.ok() || written == 0) {
        finish(status.ok() ? Status::corruption() : std::move(status));
        return;
    }
    const auto address = directory_address + directory_offset;
    const auto bytes   = round_up_to_iu(written, iu);
    directory_offset += bytes;
    io(false, address, bytes, Stage::DirectoryWritten);
}

void LocalCheckpointOperation::State::anchor()
{
    auto status = snapshot::encode_anchor_into(image->anchor, window->bytes);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    const auto slot_bytes = snapshot::superblock_slot_bytes(iu);
    const auto address    = image->anchor.snapshot_seq % 2 == 0 ? slot_bytes : 0;
    io(false, address, slot_bytes, Stage::AnchorWritten, true);
}

void LocalCheckpointOperation::State::reserve(uint64_t length, Stage next)
{
    stage   = next;
    waiting = true;
    event_ready.store(false, std::memory_order_release);
    auto status =
        registry->ledger_->reserve(length, budget.bytes, [self = shared_from_this()](ExtentLedgerResult result) {
            self->reserved = std::move(result.extent);
            self->event(std::move(result.status));
        });
    if (!status.ok()) {
        event(std::move(status));
    }
}
} // namespace crowdb::tree
