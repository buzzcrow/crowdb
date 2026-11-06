// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "portable_import.h"

#include <algorithm>

namespace crowdb::tree
{
Status PortableImportBuilder::spill_value(const CellView &cell, std::array<uint8_t, kOverflowCellSize> *encoded)
{
    const auto value  = cell.value();
    const auto head   = next_pid_;
    size_t     offset = 0;
    while (offset != value.size()) {
        uint64_t pid    = kInvalidPageId;
        auto     status = reserve_pid(&pid);
        if (!status.ok()) {
            return status;
        }
        const auto count = std::min<size_t>(value.size() - offset, overflow_chunk_cap(config_.frame_bytes));
        const auto next  = offset + count == value.size() ? kInvalidPageId : next_pid_;
        overflow_frame_build(scratch_.data(), scratch_.size(), pid, next,
                             reinterpret_cast<const uint8_t *>(value.data() + offset), count);
        status = append_native_frame(*catalog_, pid, scratch_, config_, loads_, {}, metadata_);
        if (!status.ok()) {
            return status;
        }
        offset += count;
    }
    frame_put_u64(encoded->data(), 0, cell.slot());
    (*encoded)[sizeof(uint64_t)] = kFlagOverflow;
    frame_put_u64(encoded->data(), kCellHeaderSize, head);
    frame_put_u64(encoded->data(), kCellHeaderSize + sizeof(uint64_t), value.size());
    return Status::Ok();
}

Status PortableImportBuilder::build_parent(std::span<const Node> nodes, Node *out)
{
    detail::ReservedVector<uint64_t> children{detail::ReservationAllocator<uint64_t>(metadata_)};
    detail::ReservedVector<Slice>    separators{detail::ReservationAllocator<Slice>(metadata_)};
    children.reserve(nodes.size());
    separators.reserve(nodes.size() - 1);
    for (size_t i = 0; i < nodes.size(); ++i) {
        children.push_back(nodes[i].pid);
        if (i != 0) {
            separators.push_back(Slice(nodes[i].first));
        }
    }
    uint64_t pid    = kInvalidPageId;
    auto     status = reserve_pid(&pid);
    if (!status.ok()) {
        return status;
    }
    if (!inner_frame_build_span(scratch_.data(), scratch_.size(), pid, children, separators)) {
        return Status::internal_error("portable import index preflight disagreed with its builder");
    }
    frame_set_inner_fence_pages(scratch_.data(), scratch_.size(), nodes.front().first_leaf, nodes.back().last_leaf);
    status = append_native_frame(*catalog_, pid, scratch_, config_, loads_, {}, metadata_);
    if (status.ok()) {
        *out = {.pid        = pid,
                .first      = nodes.front().first,
                .last       = nodes.back().last,
                .first_leaf = nodes.front().first_leaf,
                .last_leaf  = nodes.back().last_leaf};
    }
    return status;
}

Status PortableImportBuilder::build_index()
{
    const auto max_children =
        std::min<size_t>(static_cast<size_t>(config_.inner_max_keys) + 1, loads_.max_inner_children);
    while (level_.size() > 1) {
        detail::ReservedVector<Node> parents{detail::ReservationAllocator<Node>(metadata_)};
        size_t                       begin = 0;
        while (begin != level_.size()) {
            size_t count = 1;
            size_t keys  = 0;
            size_t used  = kFrameHeaderSize + kFrameTrailerSize + sizeof(uint64_t);
            while (count < max_children && count != level_.size() - begin) {
                const auto bytes = sizeof(uint64_t) + kInnerSlotSize + level_[begin + count].first.size();
                if (bytes > config_.frame_bytes - used ||
                    level_[begin + count].first.size() > loads_.max_inner_key_bytes - keys) {
                    break;
                }
                used += bytes;
                keys += level_[begin + count].first.size();
                ++count;
            }
            if (count == 1 && level_.size() - begin > 1) {
                return Status::resource_exhausted("portable import separator cannot fit a parent frame");
            }
            Node parent;
            auto status = build_parent(std::span<const Node>(level_).subspan(begin, count), &parent);
            if (!status.ok()) {
                return status;
            }
            parents.push_back(std::move(parent));
            begin += count;
        }
        level_ = std::move(parents);
    }
    catalog_->root_page_id = level_.front().pid;
    catalog_->next_page_id = next_pid_;
    return Status::Ok();
}
} // namespace crowdb::tree
