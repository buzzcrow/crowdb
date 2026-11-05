// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/chunk_root_commit.h"

namespace crowdb::tree
{
Status ChunkRootCommit::index()
{
    if (page_ == pages_->frames.end()) {
        page_  = pages_->frames.begin();
        stage_ = Stage::Dependencies;
        return Status::Ok();
    }
    std::array<const DurableCapturedFrame *, kChunkIndexFrameLimit> frames{};
    size_t                                                          count = 0;
    auto                                                            next  = page_;
    while (next != pages_->frames.end() && count < frames.size()) {
        if (next->first != next->second.page_id) {
            return fail(Status::corruption("Chunk checkpoint page identity mismatch"));
        }
        frames[count++] = &next->second;
        ++next;
    }
    ChunkMetadataBytes bytes{detail::ReservationAllocator<uint8_t>(window_)};
    auto status = encode_chunk_index(root_.identity, root_.indexes.size(), std::span(frames).first(count), &bytes);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    detail::ChunkStageId id;
    detail::ChunkId      chunk;
    status = ids_->next(PagePurpose::kPageIndex, &id, &chunk);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    std::shared_ptr<ChunkPackBacking> backing;
    status = prepare_chunk_backing(domain_, id, chunk, static_cast<uint32_t>(bytes.size()), PagePurpose::kPageIndex,
                                   metadata_, &backing);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    status = write_chunk_backing(*backing, bytes, window_);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    for (size_t i = 0; i < count; ++i) {
        references_.insert(frames[i]->backing->chunk->pack.stage);
        root_.stored_references += frames[i]->stored_overflow ? frames[i]->stored_overflow->size() : 0;
    }
    if (root_.stored_references > detail::kMaxCandidateReferences) {
        return fail(Status::resource_exhausted());
    }
    references_.insert(id);
    root_.indexes.push_back({.first_page  = frames[0]->page_id,
                             .last_page   = frames[count - 1]->page_id,
                             .frame_count = count,
                             .pack        = backing->pack});
    index_pins_.push_back(std::move(backing));
    page_ = next;
    return Status::Ok();
}

Status ChunkRootCommit::checksum()
{
    std::array<detail::ChunkStageId, detail::kMaxStageScan> window{};
    size_t                                                  count = 0;
    while (reference_ != references_.end() && count < window.size()) {
        window[count++] = *reference_++;
    }
    checksum_ = detail::chunk_reference_checksum(checksum_, std::span(window).first(count));
    if (reference_ != references_.end()) {
        return Status::Ok();
    }
    auto status = encode_chunk_root(root_, &manifest_);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    reference_ = references_.begin();
    stage_     = Stage::Begin;
    return Status::Ok();
}

Status ChunkRootCommit::begin()
{
    const detail::ChunkManifestCandidate candidate{.id                  = candidate_,
                                                   .expected_generation = root_.identity.root_generation - 1,
                                                   .reference_count     = references_.size(),
                                                   .reference_checksum  = checksum_,
                                                   .manifest            = manifest_};
    auto                                *catalog = domain_->catalog->protection();
    auto                                 status  = catalog->check_read_owner();
    if (!status.ok()) {
        return fail(std::move(status));
    }
    candidate_started_ = true;
    stage_             = Stage::QueryBegin;
    status             = catalog->begin_candidate(domain_->tree, candidate);
    if (!status.ok()) {
        status_ = std::move(status);
        return Status::Ok();
    }
    stage_ = references_.empty() ? Stage::Commit : Stage::Prepare;
    return Status::Ok();
}

Status ChunkRootCommit::prepare()
{
    pending_count_ = 0;
    auto next      = reference_;
    while (next != references_.end() && pending_count_ < pending_.size()) {
        pending_[pending_count_++] = *next++;
    }
    stage_      = Stage::QueryPrepare;
    auto status = domain_->catalog->protection()->prepare_references(domain_->tree, candidate_, prepared_,
                                                                     std::span(pending_).first(pending_count_));
    if (!status.ok()) {
        status_ = std::move(status);
        return Status::Ok();
    }
    accepted_window();
    return Status::Ok();
}

void ChunkRootCommit::accepted_window()
{
    for (size_t i = 0; i < pending_count_; ++i) {
        ++reference_;
    }
    ++prepared_;
    pending_count_ = 0;
    status_        = Status::Ok();
    stage_         = reference_ == references_.end() ? Stage::Commit : Stage::Prepare;
}
} // namespace crowdb::tree
