// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/maptable/mapping_slot.h"
#include "snapshot/local_recovery_internal.h"

#include <algorithm>

namespace crowdb::tree
{
void LocalRecoveryOperation::State::next_anchor()
{
    if (slot == anchors.size()) {
        stage   = Stage::Activated;
        waiting = true;
        event_ready.store(false, std::memory_order_release);
        auto status = ledger->activate(budget.bytes, [self = shared_from_this()](ExtentLedgerResult result) {
            self->event(std::move(result.status));
        });
        if (!status.ok()) {
            event(std::move(status));
        }
        return;
    }
    const auto bytes = snapshot::superblock_slot_bytes(domain.iu_size);
    if ((slot + 1) * bytes > readable_bytes) {
        ++slot;
        wake();
        return;
    }
    read(slot * bytes, bytes, Stage::AnchorRead);
}

void LocalRecoveryOperation::State::anchor_read()
{
    snapshot::CommitAnchor anchor;
    if (event_status.code() == Code::kNotFound || !snapshot::decode_anchor(window->bytes.first(read_bytes), &anchor)) {
        ++slot;
        stage = Stage::NextAnchor;
        wake();
        return;
    }
    if (anchor.snapshot_seq == 0 || (anchor.snapshot_seq % 2 == 0 ? 1U : 0U) != slot ||
        anchor.segment_slots != kMappingSegmentSize ||
        anchor.next_page_id > kMappingSegmentSize * kMaxMappingSegments || anchor.segdir_len < 20 ||
        (anchor.segdir_len - 20) % 32 != 0 || (anchor.segdir_len - 20) / 32 > limits.max_segments) {
        finish(Status::corruption());
        return;
    }
    image               = detail::reserved_shared<ProtectedCheckpoint>(budget.bytes, budget.bytes);
    image->anchor       = anchor;
    pages               = detail::reserved_shared<CapturedDurablePages>(budget.bytes, budget.bytes);
    pages->root_page_id = anchor.root_page_id;
    pages->next_page_id = anchor.next_page_id;
    pages->coverage     = anchor.last_applied_slot;
    pages->generation   = anchor.snapshot_seq;
    pages->backend_id   = domain.backend_id;
    directory_offset    = 0;
    segment_index       = 0;
    protect(anchor.segdir_addr, round_up_to_iu(anchor.segdir_len, domain.iu_size), Stage::DirectoryProtected);
}

void LocalRecoveryOperation::State::read_directory()
{
    const auto &anchor = image->anchor;
    const auto  bytes =
        std::min<uint64_t>(metadata_window, round_up_to_iu(anchor.segdir_len, domain.iu_size) - directory_offset);
    read(anchor.segdir_addr + directory_offset, bytes, Stage::DirectoryRead);
}

void LocalRecoveryOperation::State::directory_read()
{
    const auto &anchor = image->anchor;
    if (directory_offset == 0) {
        uint32_t count  = 0;
        auto     status = snapshot::directory_entry_count(window->bytes.first(read_bytes), anchor.segdir_len, &count);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        directory.resize(count);
        decoder.emplace(directory, anchor.segdir_len);
    }
    if (!decoder) {
        finish(Status::internal_error());
        return;
    }
    const auto logical = std::min<size_t>(read_bytes, anchor.segdir_len - directory_offset);
    auto       status  = decoder->next(window->bytes.first(logical));
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    directory_offset += read_bytes;
    if (!decoder->complete()) {
        read_directory();
        return;
    }
    if (!decoder->finish().ok() || decoder->crc() != anchor.segdir_crc) {
        finish(Status::corruption());
        return;
    }
    stage = Stage::Segment;
    wake();
}

void LocalRecoveryOperation::State::segment()
{
    if (segment_index == directory.size()) {
        frame_position = pages->frames.begin();
        stage          = Stage::NextFrame;
        wake();
        return;
    }
    const auto &entry = directory[segment_index];
    if (entry.seg_idx >= kMaxMappingSegments ||
        (segment_index > 0 && directory[segment_index - 1].seg_idx >= entry.seg_idx) ||
        entry.image_len != segment_image_encoded_size(kMappingSegmentSize)) {
        finish(Status::corruption());
        return;
    }
    protect(entry.image_addr, round_up_to_iu(entry.image_len, domain.iu_size), Stage::SegmentProtected);
}

void LocalRecoveryOperation::State::segment_read()
{
    SegmentImageHeader header;
    auto               status = snapshot::decode_segment_into(window->bytes.first(read_bytes), &header, words);
    const auto        &entry  = directory[segment_index];
    if (!status.ok() || header.seg_idx != entry.seg_idx || header.generation != entry.generation ||
        header.slot_count != kMappingSegmentSize || header.body_crc != entry.image_crc) {
        finish(Status::corruption());
        return;
    }
    if (limits.accept_legacy_chunk_refs && std::ranges::any_of(words, slot_word::is_page_ref)) {
        // Preserve the old anchor's extent, but do not reuse its legacy slot
        // encoding as an ordinary byte-location segment.
        image->metadata->extents.push_back(std::move(image->metadata->segments.back().second));
        image->metadata->segments.pop_back();
    }
    word_index = 0;
    stage      = Stage::PageExtent;
    wake();
}

void LocalRecoveryOperation::State::page_extent()
{
    while (word_index < words.size() && slot_word::is_empty(words[word_index])) {
        ++word_index;
    }
    if (word_index == words.size()) {
        ++segment_index;
        stage = Stage::Segment;
        wake();
        return;
    }
    const auto word       = words[word_index];
    const bool legacy_ref = limits.accept_legacy_chunk_refs && slot_word::is_page_ref(word);
    if (!slot_word::is_byte_location(word) && !legacy_ref) {
        finish(Status::not_supported());
        return;
    }
    const auto pid = (uint64_t{directory[segment_index].seg_idx} * kMappingSegmentSize) + word_index;
    if (pid >= pages->next_page_id || pages->frames.size() == limits.max_frames) {
        finish(pid >= pages->next_page_id ? Status::corruption() : Status::resource_exhausted());
        return;
    }
    const auto iu_index = legacy_ref ? slot_word::page_ref_ordinal(word) : slot_word::unloaded_iu_index(word);
    const auto iu_count = legacy_ref ? slot_word::page_ref_iu_count(word) : slot_word::unloaded_iu_count(word);
    if (iu_index > UINT64_MAX / domain.iu_size) {
        finish(Status::corruption());
        return;
    }
    const auto address = iu_index * domain.iu_size;
    const auto bytes   = uint64_t{iu_count} * domain.iu_size;
    if (bytes > limits.max_blob_bytes) {
        finish(Status::resource_exhausted());
        return;
    }
    protect(address, bytes, Stage::PageProtected);
}

void LocalRecoveryOperation::State::page_protected()
{
    auto backing              = detail::reserved_shared<BackingRef>(budget.bytes);
    backing->backend_id       = recovered.backend_id;
    backing->generation       = recovered.generation;
    backing->content_id       = recovered.identity;
    backing->address          = recovered.address;
    backing->length           = recovered.length;
    backing->durability       = BackingDurability::kCheckpointCommitted;
    backing->owner            = recovered.owner;
    backing->physical_owner   = recovered.owner;
    backing->physical_metrics = recovered.physical_metrics.lock();
    const auto pid            = (uint64_t{directory[segment_index].seg_idx} * kMappingSegmentSize) + word_index;
    pages->frames.emplace(pid, DurableCapturedFrame{.page_id = pid, .backing = std::move(backing)});
    ++word_index;
    stage = Stage::PageExtent;
    wake();
}
} // namespace crowdb::tree
