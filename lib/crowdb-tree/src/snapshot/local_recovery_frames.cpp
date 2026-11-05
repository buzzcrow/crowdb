// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/recovered_frame_identity.h"
#include "crowdb-tree/btree/cell.h"
#include "crowdb-tree/maptable/compressor.h"
#include "snapshot/local_recovery_internal.h"
#include "snapshot/stored_overflow.h"

namespace crowdb::tree
{
void LocalRecoveryOperation::State::next_frame()
{
    if (frame_position == pages->frames.end()) {
        const auto root       = pages->frames.find(pages->root_page_id);
        const bool valid_root = pages->frames.empty()
                                  ? pages->root_page_id == kInvalidPageId
                                  : root != pages->frames.end() && root->second.type != page_type::kOverflowFrame;
        if (!valid_root || leaf_count != image->anchor.leaf_count || inner_count != image->anchor.inner_count) {
            finish(Status::corruption());
            return;
        }
        stage = Stage::Dependencies;
        wake();
        return;
    }
    const auto &backing = frame_position->second.backing;
    if (!backing || !backing->reloadable() || backing->encoding != 0 || backing->backend_id != domain.backend_id ||
        backing->generation != domain.generation) {
        finish(Status::corruption());
        return;
    }
    if (backing->length > limits.max_blob_bytes) {
        finish(Status::resource_exhausted());
        return;
    }
    read(backing->address, static_cast<size_t>(backing->length), Stage::FrameRead);
}

Status LocalRecoveryOperation::State::collect_cell(uint64_t page_id, Slice raw)
{
    const CellView cell(raw);
    if (!cell.valid() || cell.slot() > pages->coverage ||
        (cell.flags() != 0 && cell.flags() != kFlagTombstone && cell.flags() != kFlagOverflow) ||
        (cell.is_tombstone() && raw.size() != kCellHeaderSize)) {
        return Status::corruption();
    }
    if (!cell.is_overflow()) {
        return Status::Ok();
    }
    if (raw.size() != kOverflowCellSize || cell.overflow_head() == kInvalidPageId || cell.overflow_len() == 0) {
        return Status::corruption();
    }
    if (references.size() == limits.max_overflow_references) {
        return Status::resource_exhausted();
    }
    references.push_back({.page_id = page_id, .head = cell.overflow_head(), .bytes = cell.overflow_len()});
    return Status::Ok();
}

void LocalRecoveryOperation::State::frame_read()
{
    if (read_bytes < kDurableBlobHeader) {
        finish(Status::corruption());
        return;
    }
    const uint64_t logical = kDurableBlobHeader + uint64_t{frame_u32(window->bytes.data(), 5)};
    const uint32_t bytes   = durable_blob_raw_len(window->bytes.data(), read_bytes);
    if (logical > read_bytes || logical > UINT32_MAX || bytes <= kFrameHeaderSize + kFrameTrailerSize) {
        finish(Status::corruption());
        return;
    }
    if (bytes > limits.max_frame_bytes) {
        finish(Status::resource_exhausted());
        return;
    }
    window->frame.resize(bytes);
    auto status = decode_durable_page(window->bytes.data(), read_bytes, window->frame.data(), bytes);
    if (!status.ok()) {
        finish(status.without_message());
        return;
    }
    auto       &descriptor = frame_position->second;
    const auto *frame      = window->frame.data();
    if (!frame_validate(frame, bytes) ||
        !recovered_frame_identity_matches(*descriptor.backing, frame_page_type(frame),
                                          frame_u64(frame, fh::kSelfpage_id), descriptor.page_id)) {
        finish(Status::corruption());
        return;
    }
    descriptor.frame_bytes   = bytes;
    descriptor.logical_bytes = static_cast<uint32_t>(logical);
    descriptor.type          = frame_page_type(frame);
    if (descriptor.type == page_type::kLeafBase) {
        const LeafFrameView leaf(frame, bytes);
        for (uint32_t i = 0; i < leaf.count(); ++i) {
            status = collect_cell(descriptor.page_id, leaf.cell(i));
            if (!status.ok()) {
                finish(std::move(status));
                return;
            }
        }
        for (uint32_t i = 0; i < leaf.delta_count(); ++i) {
            status = collect_cell(descriptor.page_id, leaf.delta_cell(i));
            if (!status.ok()) {
                finish(std::move(status));
                return;
            }
        }
        status =
            capture_stored_overflow(leaf, limits.max_overflow_references, budget.bytes, &descriptor.stored_overflow);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        ++leaf_count;
    }
    else if (descriptor.type == page_type::kInnerBase) {
        ++inner_count;
    }
    else {
        const OverflowFrameView overflow(frame, bytes);
        if (overflow.chunk_len() == 0 || overflow.next_page_id() == descriptor.page_id) {
            finish(Status::corruption());
            return;
        }
        descriptor.overflow_bytes = overflow.chunk_len();
        descriptor.next_overflow  = overflow.next_page_id();
    }
    ++frame_position;
    stage = Stage::NextFrame;
    wake();
}

void LocalRecoveryOperation::State::dependencies()
{
    if (reference_index == references.size()) {
        auto status = pages->initialize_overflow_counts();
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        image_complete();
        return;
    }
    if (dependency_steps == limits.max_dependency_steps) {
        finish(Status::resource_exhausted());
        return;
    }
    ++dependency_steps;
    const auto &reference = references[reference_index];
    if (chain_pid == kInvalidPageId) {
        chain_pid   = reference.head;
        chain_bytes = reference.bytes;
    }
    if (protected_leaf != reference.page_id) {
        auto leaf = pages->frames.find(reference.page_id);
        if (leaf == pages->frames.end() || leaf->second.type != page_type::kLeafBase || !leaf->second.backing) {
            finish(Status::corruption());
            return;
        }
        protection           = detail::reserved_shared<LeafProtection>(budget.bytes, budget.bytes);
        overflow_backing     = detail::reserved_shared<OverflowBackingSet>(budget.bytes, budget.bytes);
        protection->primary  = leaf->second.backing->owner;
        auto backing         = detail::reserved_shared<BackingRef>(budget.bytes, *leaf->second.backing);
        backing->owner       = protection;
        backing->overflow    = overflow_backing;
        leaf->second.backing = std::move(backing);
        protected_leaf       = reference.page_id;
    }
    const auto found = pages->frames.find(chain_pid);
    if (found == pages->frames.end()) {
        finish(Status::corruption());
        return;
    }
    const auto &overflow = found->second;
    if (overflow.type != page_type::kOverflowFrame || !overflow.backing || !overflow.backing->reloadable() ||
        overflow.overflow_bytes == 0 || overflow.overflow_bytes > chain_bytes ||
        (overflow.next_overflow == kInvalidPageId) != (overflow.overflow_bytes == chain_bytes)) {
        finish(Status::corruption());
        return;
    }
    protection->overflow.push_back(overflow.backing->owner);
    overflow_backing->pages.emplace(chain_pid, OverflowBackingPage{.backing       = overflow.backing,
                                                                   .next          = overflow.next_overflow,
                                                                   .bytes         = overflow.overflow_bytes,
                                                                   .frame_bytes   = overflow.frame_bytes,
                                                                   .logical_bytes = overflow.logical_bytes});
    chain_bytes -= overflow.overflow_bytes;
    chain_pid = overflow.next_overflow;
    if (chain_bytes == 0) {
        ++reference_index;
    }
    wake();
}

void LocalRecoveryOperation::State::image_complete()
{
    image->pages = std::move(pages);

    anchors[slot] = std::move(image);
    references.clear();
    directory.clear();
    decoder.reset();
    recovered = {};
    protection.reset();
    overflow_backing.reset();
    reference_index  = 0;
    chain_pid        = kInvalidPageId;
    chain_bytes      = 0;
    dependency_steps = 0;
    protected_leaf   = kInvalidPageId;
    leaf_count       = 0;
    inner_count      = 0;
    directory_offset = 0;
    segment_index    = 0;
    word_index       = 0;
    ++slot;
    stage = Stage::NextAnchor;
    wake();
}
} // namespace crowdb::tree
