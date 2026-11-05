// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "chunk_stage.h"

namespace crowdb::tree::detail
{
// Leave four metadata keys for the final window's atomic release records.
inline constexpr size_t   kMaxReferenceWindow        = 60;
inline constexpr uint64_t kMaxCandidateReferences    = 16 * 1024 * 1024;
inline constexpr size_t   kMaxCandidateManifestBytes = 16 * 1024 * 1024;
enum class ChunkCandidateState : uint8_t { Preparing = 1, Ready, Committed, Aborted, Reclaimed };

struct ChunkCandidateStatus
{
    ChunkStageId        id{};
    uint64_t            generation          = 0;
    uint64_t            reference_count     = 0;
    uint64_t            prepared_references = 0;
    uint64_t            prepared_windows    = 0;
    uint64_t            released_windows    = 0;
    ChunkCandidateState state               = ChunkCandidateState::Preparing;
};

struct ChunkManifestCandidate
{
    ChunkStageId             id{};
    uint64_t                 expected_generation = 0;
    uint64_t                 reference_count     = 0;
    uint32_t                 reference_checksum  = 0;
    std::span<const uint8_t> manifest;
};

struct ChunkPinProgress
{
    std::optional<ChunkStageId> after;
    uint32_t                    examined = 0;
    uint32_t                    released = 0;
    bool                        more     = false;
};

// Accessed only while the owning RootCatalog is retained. Page backing owners
// hold that catalog independently of the live Tree and its mutable sources.
using ChunkRootBytes = std::vector<uint8_t, MetadataAllocator<uint8_t>>;

class ChunkProtectionCatalog
{
  public:
    virtual ~ChunkProtectionCatalog()                                                                   = default;
    [[nodiscard]] virtual Status check_read_owner() const                                               = 0;
    virtual Status               writer_quiesced(uint64_t tree, uint64_t epoch)                         = 0;
    virtual Status load_root_bytes(uint64_t tree, uint64_t generation, std::vector<uint8_t> *out) const = 0;

    virtual Status load_root_owned(uint64_t /*tree*/, uint64_t /*generation*/, ChunkRootBytes * /*out*/) const
    {
        return Status::not_supported();
    }

    virtual Status begin_candidate(uint64_t tree, const ChunkManifestCandidate &)                              = 0;
    virtual Status load_candidate(uint64_t tree, ChunkStageId, std::optional<ChunkCandidateStatus> *out) const = 0;
    virtual Status list_candidates(uint64_t tree, std::optional<ChunkStageId> after, uint32_t limit,
                                   std::vector<ChunkCandidateStatus> *out) const                               = 0;

    virtual Status next_candidate(uint64_t /*tree*/, std::optional<ChunkStageId> /*after*/,
                                  std::optional<ChunkCandidateStatus> * /*out*/) const
    {
        return Status::not_supported();
    }

    virtual Status prepare_references(uint64_t tree, ChunkStageId, uint64_t window, std::span<const ChunkStageId>) = 0;
    virtual Status finish_candidate(uint64_t tree, ChunkStageId, uint32_t action)                                  = 0;
    virtual Status release_references(uint64_t tree, ChunkStageId, uint64_t window)                                = 0;
    virtual Status change_pin(uint64_t tree, ChunkStageId stage, ChunkStageId pin, bool acquire)                   = 0;
    virtual Status prune_pins(uint64_t tree, ChunkStageId, std::optional<ChunkStageId> after, uint32_t limit,
                              ChunkPinProgress *out)                                                               = 0;
};

uint32_t chunk_reference_checksum(uint32_t initial, std::span<const ChunkStageId> references) noexcept;
} // namespace crowdb::tree::detail
