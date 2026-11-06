// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/chunk_root_commit.h"

#include <algorithm>

namespace crowdb::tree
{
Status ChunkRootCommit::finish_candidate(bool commit)
{
    stage_      = commit ? Stage::QueryCommit : Stage::QueryAbort;
    auto status = domain_->catalog->protection()->finish_candidate(
        domain_->tree, candidate_, commit ? CT_CHUNK_CANDIDATE_COMMIT : CT_CHUNK_CANDIDATE_ABORT);
    if (!status.ok()) {
        if (status_.ok()) {
            status_ = std::move(status);
        }
        return Status::Ok();
    }
    if (commit) {
        committed_ = true;
        return finish(Status::Ok());
    }
    stage_ = Stage::Release;
    return Status::Ok();
}

Status ChunkRootCommit::query()
{
    std::optional<detail::ChunkCandidateStatus> current;
    auto status = domain_->catalog->protection()->load_candidate(domain_->tree, candidate_, &current);
    if (!status.ok()) {
        return finish(std::move(status), true);
    }
    if (!current) {
        return stage_ == Stage::QueryBegin ? finish(status_.without_message())
                                           : finish(Status::corruption("Chunk candidate disappeared"), true);
    }
    if (current->id != candidate_ || current->generation != root_.identity.root_generation ||
        current->reference_count != references_.size() || current->prepared_references > references_.size() ||
        current->prepared_windows >
            (references_.size() + detail::kMaxReferenceWindow - 1) / detail::kMaxReferenceWindow ||
        current->released_windows > current->prepared_windows) {
        return finish(Status::corruption("Chunk candidate identity changed"), true);
    }
    const auto prepared =
        std::min<uint64_t>(references_.size(), current->prepared_windows * detail::kMaxReferenceWindow);
    const bool all_prepared = current->prepared_references == references_.size();
    using Candidate         = detail::ChunkCandidateState;
    if (current->prepared_references != prepared ||
        ((current->state == Candidate::Ready || current->state == Candidate::Committed) && !all_prepared) ||
        (current->state == Candidate::Preparing && all_prepared) ||
        (current->state <= Candidate::Committed && current->released_windows != 0)) {
        return finish(Status::corruption("Chunk candidate reference proof is inconsistent"), true);
    }
    return query_result(*current);
}

Status ChunkRootCommit::query_result(const detail::ChunkCandidateStatus &current)
{
    using Candidate = detail::ChunkCandidateState;
    if (current.state == Candidate::Committed) {
        committed_ = true;
        return finish(Status::Ok());
    }
    if (current.state == Candidate::Aborted || current.state == Candidate::Reclaimed) {
        prepared_ = current.prepared_windows;
        released_ = current.released_windows;
        if (status_.ok()) {
            status_ = Status::unavailable("Chunk candidate was aborted");
        }
        stage_ = Stage::Release;
        return Status::Ok();
    }
    if (stage_ == Stage::QueryBegin && current.prepared_windows == 0 && current.prepared_references == 0) {
        status_ = Status::Ok();
        stage_  = references_.empty() ? Stage::Commit : Stage::Prepare;
        return Status::Ok();
    }
    const auto accepted = std::min<uint64_t>(references_.size(), (prepared_ + 1) * detail::kMaxReferenceWindow);
    if (stage_ == Stage::QueryPrepare && current.prepared_windows == prepared_ + 1 &&
        current.prepared_references == accepted) {
        accepted_window();
        return Status::Ok();
    }
    // A successful linearizable query proves the failed mutation did not
    // commit. Abort the same candidate; never generate a replacement ID here.
    if (stage_ == Stage::QueryAbort) {
        return finish(status_.without_message(), true);
    }
    prepared_ = current.prepared_windows;
    released_ = current.released_windows;
    stage_    = Stage::Abort;
    return Status::Ok();
}

Status ChunkRootCommit::release()
{
    if (released_ == prepared_) {
        return finish(status_.without_message());
    }
    auto status = domain_->catalog->protection()->release_references(domain_->tree, candidate_, released_);
    if (!status.ok()) {
        return finish(std::move(status), true);
    }
    ++released_;
    return Status::Ok();
}
} // namespace crowdb::tree
