// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/chunk/chunk_pin_release.h"
#include "common/test_stage_catalog.h"

#include <map>

namespace crowdb::tree::test_chunk
{
class TestRootCatalog : public detail::TestStageCatalog, public detail::ChunkProtectionCatalog
{
  public:
    explicit TestRootCatalog(std::shared_ptr<AllocationBudget> storage_budget)
        : storage_budget_(std::move(storage_budget))
    {
    }

    std::shared_ptr<detail::ChunkPinReleaseQueue>                       queue;
    std::map<detail::ChunkStageId /*unused*/, detail::ChunkStageRecord> records;
    std::optional<detail::ChunkCandidateStatus>                         candidate;
    std::vector<uint8_t>                                                manifest;
    std::vector<uint8_t>                                                committed_manifest;
    std::map<uint64_t, std::vector<uint8_t>>                            retained_roots;
    size_t                                                              writer_releases       = 0;
    bool                                                                fail_writer_release   = false;
    uint64_t                                                            current_generation    = 6;
    uint32_t                                                            checksum              = 0;
    uint32_t                                                            expected_checksum     = 0;
    size_t                                                              begin_calls           = 0;
    size_t                                                              prepare_calls         = 0;
    size_t                                                              commit_calls          = 0;
    size_t                                                              abort_calls           = 0;
    size_t                                                              acquire_calls         = 0;
    size_t                                                              release_calls         = 0;
    bool                                                                lose_begin            = false;
    bool                                                                lose_prepare          = false;
    bool                                                                lose_commit           = false;
    bool                                                                lose_commit_outcome   = false;
    bool                                                                fail_commit           = false;
    bool                                                                fail_query            = false;
    bool                                                                delete_before_acquire = false;

    detail::ChunkProtectionCatalog *protection() noexcept override
    {
        return this;
    }

    std::shared_ptr<detail::ChunkPinReleaseQueue> pin_release_queue() const noexcept override
    {
        return queue;
    }

    Status check_read_owner() const override
    {
        return Status::Ok();
    }

    Status writer_quiesced(uint64_t /*unused*/, uint64_t /*unused*/) override
    {
        ++writer_releases;
        return fail_writer_release ? Status::unavailable() : Status::Ok();
    }

    [[nodiscard]] std::shared_ptr<TestRootCatalog> next_owner() const
    {
        auto next                = std::make_shared<TestRootCatalog>(storage_budget_);
        next->queue              = queue;
        next->records            = records;
        next->candidate          = candidate;
        next->manifest           = manifest;
        next->committed_manifest = committed_manifest;
        next->retained_roots     = retained_roots;
        next->current_generation = current_generation;
        next->checksum           = checksum;
        next->expected_checksum  = expected_checksum;
        return next;
    }

    Status load_root_bytes(uint64_t /*unused*/, uint64_t /*unused*/, std::vector<uint8_t> *out) const override
    {
        *out = manifest;
        return Status::Ok();
    }

    Status load_root_owned(uint64_t /*tree*/, uint64_t generation, detail::ChunkRootBytes *out) const override
    {
        if (current_generation == 0 || committed_manifest.empty()) {
            return Status::not_found();
        }
        if (generation != 0 && generation != current_generation) {
            const auto old = retained_roots.find(generation);
            if (old == retained_roots.end()) {
                return Status::not_found();
            }
            out->assign(old->second.begin(), old->second.end());
            return Status::Ok();
        }
        out->assign(committed_manifest.begin(), committed_manifest.end());
        return Status::Ok();
    }

    Status load_stage(uint64_t /*unused*/, detail::ChunkStageId id,
                      std::optional<detail::ChunkStageRecord> *out) const override
    {
        const auto found = records.find(id);
        if (found == records.end()) {
            out->reset();
        }
        else {
            *out = found->second;
        }
        return Status::Ok();
    }

    Status compare_stage(uint64_t /*unused*/, uint64_t /*unused*/,
                         const std::optional<detail::ChunkStageRecord> &expected,
                         const std::optional<detail::ChunkStageRecord> &next) override
    {
        auto status = detail::validate_stage_transition(expected, next);
        if (!status.ok()) {
            return status;
        }
        if (!next && !expected) {
            return Status::invalid_argument();
        }
        const auto                              id = next ? next->id : expected->id;
        std::optional<detail::ChunkStageRecord> current;
        (void)load_stage(19, id, &current);
        if (current == next) {
            return Status::Ok();
        }
        if (current != expected) {
            return Status::unavailable();
        }
        if (next) {
            // The mock durable catalog owns its persisted bytes independently
            // of the submitting checkpoint's eventually sealed reservation.
            detail::ChunkStageRecord persisted{.tree_id             = next->tree_id,
                                               .id                  = next->id,
                                               .revision            = next->revision,
                                               .chunk_high          = next->chunk_high,
                                               .chunk_low           = next->chunk_low,
                                               .capacity            = next->capacity,
                                               .manifest_generation = next->manifest_generation,
                                               .purpose             = next->purpose,
                                               .state               = next->state,
                                               .packs               = decltype(detail::ChunkStageRecord::packs)(
                                                   MetadataAllocator<detail::ChunkStagePack>(storage_budget_))};
            persisted.packs.assign(next->packs.begin(), next->packs.end());
            records.insert_or_assign(id, std::move(persisted));
        }
        else {
            records.erase(id);
        }
        return Status::Ok();
    }

    Status begin_candidate(uint64_t /*unused*/, const detail::ChunkManifestCandidate &request) override
    {
        ++begin_calls;
        if ((candidate && candidate->state != detail::ChunkCandidateState::Committed &&
             candidate->state != detail::ChunkCandidateState::Aborted) ||
            request.expected_generation != current_generation) {
            return Status::unavailable();
        }
        checksum          = 0;
        candidate         = {.id                  = request.id,
                             .generation          = request.expected_generation + 1,
                             .reference_count     = request.reference_count,
                             .prepared_references = 0,
                             .prepared_windows    = 0,
                             .released_windows    = 0,
                             .state               = request.reference_count == 0 ? detail::ChunkCandidateState::Ready
                                                                                 : detail::ChunkCandidateState::Preparing};
        expected_checksum = request.reference_checksum;
        manifest.assign(request.manifest.begin(), request.manifest.end());
        return lose_begin ? Status::unavailable() : Status::Ok();
    }

    Status load_candidate(uint64_t /*unused*/, detail::ChunkStageId id,
                          std::optional<detail::ChunkCandidateStatus> *out) const override
    {
        if (fail_query) {
            return Status::unavailable();
        }
        *out = candidate && candidate->id == id ? candidate : std::nullopt;
        return Status::Ok();
    }

    Status prepare_references(uint64_t /*unused*/, detail::ChunkStageId id, uint64_t window,
                              std::span<const detail::ChunkStageId> refs) override
    {
        ++prepare_calls;
        if (!candidate || candidate->id != id || candidate->prepared_windows != window || refs.size() > 64) {
            return Status::invalid_argument();
        }
        for (auto stage : refs) {
            const auto found = records.find(stage);
            if (found == records.end() || found->second.state != detail::ChunkStageState::Reloadable) {
                return Status::corruption();
            }
        }
        checksum = detail::chunk_reference_checksum(checksum, refs);
        ++candidate->prepared_windows;
        candidate->prepared_references += refs.size();
        if (candidate->prepared_references == candidate->reference_count) {
            if (checksum != expected_checksum) {
                return Status::corruption();
            }
            candidate->state = detail::ChunkCandidateState::Ready;
        }
        return lose_prepare ? Status::unavailable() : Status::Ok();
    }

    Status finish_candidate(uint64_t /*unused*/, detail::ChunkStageId id, uint32_t action) override
    {
        if (!candidate || candidate->id != id) {
            return Status::not_found();
        }
        if (action == CT_CHUNK_CANDIDATE_COMMIT) {
            ++commit_calls;
            if (fail_commit) {
                return Status::unavailable();
            }
            if (candidate->state != detail::ChunkCandidateState::Ready) {
                return Status::invalid_argument();
            }
            candidate->state                   = detail::ChunkCandidateState::Committed;
            retained_roots[current_generation] = committed_manifest;
            current_generation                 = candidate->generation;
            committed_manifest                 = manifest;
            if (lose_commit_outcome) {
                fail_query = true;
                return Status::unavailable("injected lost commit and query replies");
            }
            return lose_commit ? Status::unavailable() : Status::Ok();
        }
        ++abort_calls;
        if (candidate->state == detail::ChunkCandidateState::Committed) {
            return Status::unavailable();
        }
        candidate->state = detail::ChunkCandidateState::Aborted;
        return Status::Ok();
    }

    Status release_references(uint64_t /*unused*/, detail::ChunkStageId /*unused*/, uint64_t window) override
    {
        if (!candidate || candidate->state != detail::ChunkCandidateState::Aborted ||
            candidate->released_windows != window) {
            return Status::invalid_argument();
        }
        ++candidate->released_windows;
        ++release_calls;
        return Status::Ok();
    }

    Status change_pin(uint64_t /*unused*/, detail::ChunkStageId stage, detail::ChunkStageId /*unused*/,
                      bool acquire) override
    {
        if (!acquire) {
            return Status::Ok();
        }
        ++acquire_calls;
        if (delete_before_acquire) {
            records.at(stage).state = detail::ChunkStageState::Deleting;
        }
        return records.at(stage).state == detail::ChunkStageState::Reloadable ? Status::Ok() : Status::unavailable();
    }

    Status list_candidates(uint64_t /*unused*/, std::optional<detail::ChunkStageId> /*unused*/, uint32_t /*unused*/,
                           std::vector<detail::ChunkCandidateStatus> * /*unused*/) const override
    {
        return Status::not_supported();
    }

    Status prune_pins(uint64_t /*unused*/, detail::ChunkStageId /*unused*/,
                      std::optional<detail::ChunkStageId> /*unused*/, uint32_t /*unused*/,
                      detail::ChunkPinProgress * /*unused*/) override
    {
        return Status::not_supported();
    }

  private:
    std::shared_ptr<AllocationBudget> storage_budget_;
};

} // namespace crowdb::tree::test_chunk
