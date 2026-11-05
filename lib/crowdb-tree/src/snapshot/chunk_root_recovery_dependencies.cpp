// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/chunk_root_recovery.h"

namespace crowdb::tree
{
Status ChunkRootRecovery::dependencies()
{
    for (size_t work = 0; work < 64; ++work) {
        if (leaf_ == pages_->frames.end()) {
            return finish(steps_ == root_.dependency_entries ? pages_->initialize_overflow_counts()
                                                             : Status::corruption());
        }
        auto &leaf = leaf_->second;
        if (leaf.type != page_type::kLeafBase || reference_ == leaf.stored_overflow->size()) {
            ++leaf_;
            reference_ = 0;
            overflow_.reset();
            continue;
        }
        if (steps_ == root_.dependency_entries || steps_ == limits_.max_dependency_steps) {
            return Status::corruption();
        }
        if (!overflow_) {
            overflow_         = detail::reserved_shared<OverflowBackingSet>(metadata_, metadata_);
            auto owner        = detail::reserved_shared<LeafProtection>(metadata_);
            owner->primary    = leaf.backing->physical_owner;
            owner->overflow   = overflow_;
            auto backing      = detail::reserved_shared<BackingRef>(metadata_, *leaf.backing);
            backing->owner    = std::move(owner);
            backing->overflow = overflow_;
            leaf.backing      = std::move(backing);
        }
        if (pid_ == kInvalidPageId) {
            const auto &ref = (*leaf.stored_overflow)[reference_];
            pid_            = ref.head;
            remaining_      = ref.bytes;
        }
        const auto found = pages_->frames.find(pid_);
        if (found == pages_->frames.end()) {
            return Status::corruption();
        }
        const auto &frame = found->second;
        if (frame.type != page_type::kOverflowFrame || frame.overflow_bytes == 0 || frame.overflow_bytes > remaining_ ||
            (frame.next_overflow == kInvalidPageId) != (frame.overflow_bytes == remaining_)) {
            return Status::corruption();
        }
        overflow_->pages.emplace(pid_, OverflowBackingPage{.backing       = frame.backing,
                                                           .next          = frame.next_overflow,
                                                           .bytes         = frame.overflow_bytes,
                                                           .frame_bytes   = frame.frame_bytes,
                                                           .logical_bytes = frame.logical_bytes});
        ++steps_;
        remaining_ -= frame.overflow_bytes;
        pid_ = frame.next_overflow;
        if (remaining_ == 0) {
            ++reference_;
        }
    }
    return Status::Ok();
}
} // namespace crowdb::tree
