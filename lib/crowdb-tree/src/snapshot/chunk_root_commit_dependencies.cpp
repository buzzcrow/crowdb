// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/chunk_root_commit.h"

namespace crowdb::tree
{
Status ChunkRootCommit::dependencies()
{
    for (size_t work = 0; work < detail::kMaxStageScan && page_ != pages_->frames.end(); ++work) {
        const auto &frame = page_->second;
        if (frame.type != page_type::kLeafBase || dependency_ == frame.stored_overflow->size()) {
            ++page_;
            dependency_ = 0;
            continue;
        }
        if (root_.dependency_entries == max_dependency_steps_) {
            return fail(Status::resource_exhausted());
        }
        if (chain_pid_ == kInvalidPageId) {
            const auto &reference = (*frame.stored_overflow)[dependency_];
            chain_pid_            = reference.head;
            chain_bytes_          = reference.bytes;
        }
        const auto found = pages_->frames.find(chain_pid_);
        if (found == pages_->frames.end() || found->second.type != page_type::kOverflowFrame ||
            found->second.overflow_bytes == 0 || found->second.overflow_bytes > chain_bytes_ ||
            (found->second.next_overflow == kInvalidPageId) != (found->second.overflow_bytes == chain_bytes_)) {
            return fail(Status::corruption("Chunk checkpoint overflow dependency is incomplete"));
        }
        ++root_.dependency_entries;
        chain_bytes_ -= found->second.overflow_bytes;
        chain_pid_ = found->second.next_overflow;
        if (chain_bytes_ == 0) {
            ++dependency_;
        }
    }
    if (page_ == pages_->frames.end()) {
        reference_ = references_.begin();
        stage_     = Stage::Checksum;
    }
    return Status::Ok();
}
} // namespace crowdb::tree
