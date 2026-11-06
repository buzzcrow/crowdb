// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "block_directory.h"

namespace crowdb::tree
{
namespace
{
template <class T> void destroy_released(T *value) noexcept
{
    if (value != nullptr) {
        MetadataAllocator<T> allocator(value->budget);
        std::destroy_at(value);
        allocator.deallocate(value, 1);
    }
}
} // namespace

bool BlockDirectory::retire_step() noexcept
{
    if (!retiring_) {
        retiring_       = true;
        retire_path_[0] = &root_;
    }
    if (entries_drained_) {
        return epoch_.retire_backend_step();
    }

    for (size_t work = 0; work < 64; ++work) {
        auto &position = retire_index_[retire_depth_];
        if (retire_depth_ == 3) {
            auto *leaf = static_cast<Segment *>(retire_path_[3]);
            if (position < leaf->slots.size()) {
                auto *entry = leaf->slots[position++].exchange(nullptr, std::memory_order_relaxed);
                if (entry != &busy_) {
                    destroy_released(entry);
                }
            }
            else {
                destroy_released(leaf);
                --retire_depth_;
            }
            continue;
        }
        auto *node = static_cast<Node *>(retire_path_[retire_depth_]);
        if (position < node->children.size()) {
            auto *child = node->children[position++].exchange(nullptr, std::memory_order_relaxed);
            if (child != nullptr) {
                ++retire_depth_;
                retire_path_[retire_depth_]  = child;
                retire_index_[retire_depth_] = 0;
            }
        }
        else if (retire_depth_ != 0) {
            destroy_released(node);
            --retire_depth_;
        }
        else {
            entries_drained_ = true;
            return false;
        }
    }
    return false;
}
} // namespace crowdb::tree
