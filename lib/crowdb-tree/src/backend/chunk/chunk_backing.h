// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "chunk_fixed_pack.h"
#include "chunk_pin_release.h"

namespace crowdb::tree
{
class BackingMetricExtent;
class ChunkPhysicalRegistry;
struct ChunkPhysicalPack;

// A stable storage domain, independent of a Tree generation or CPU executor.
// Every descriptor owns a paid pin; the host release service outlives them.
struct ChunkBackingDomain
{
    uint64_t                                backend_id      = 0;
    uint64_t                                tree            = 0;
    uint64_t                                epoch           = 0;
    uint32_t                                mirrors         = 0;
    uint64_t                                max_chunk_bytes = 256 * 1024 * 1024;
    std::shared_ptr<detail::RootCatalog>    catalog;
    std::shared_ptr<detail::ChunkTransport> transport;
    std::shared_ptr<ChunkPhysicalRegistry>  physical_registry;

    [[nodiscard]] bool supported() const noexcept;
};

// Filled privately by the writer before its completion publishes a const view.
// Shared page descriptors use this one pack owner, including its physical-byte
// observation; no per-page copy of whole-pack storage accounting is needed.
struct ChunkPackBacking
{
    using Read  = Status (*)(const ChunkPackBacking &, std::span<uint8_t>, const std::shared_ptr<AllocationBudget> &);
    using Write = Status (*)(ChunkPackBacking &, std::span<const uint8_t>, const std::shared_ptr<AllocationBudget> &);
    Read                                      read  = nullptr;
    Write                                     write = nullptr;
    detail::ChunkFixedPackRef                 pack;
    std::shared_ptr<const ChunkBackingDomain> domain;
    std::shared_ptr<detail::ChunkPinOwner>    pin;
    std::shared_ptr<AllocationBudget>         physical_budget;
    std::shared_ptr<ChunkPhysicalPack>        physical;
    std::shared_ptr<BackingMetricExtent>      physical_metrics;
};

[[nodiscard]] bool chunk_backing_matches(const ChunkPackBacking &backing, uint64_t domain, uint64_t length) noexcept;
[[nodiscard]] bool same_chunk_backing(const std::shared_ptr<const ChunkPackBacking> &left,
                                      const std::shared_ptr<const ChunkPackBacking> &right) noexcept;
[[nodiscard]] std::array<uint64_t, 8> chunk_backing_identity(const ChunkPackBacking &backing) noexcept;
[[nodiscard]] size_t                  chunk_backing_allocation_bound() noexcept;
[[nodiscard]] size_t                  chunk_pin_validation_bound() noexcept;

// Preparation spends no metadata I/O. Arm before the first mutation so an
// unknown StageReserved result still receives an exact, durable final release.
Status prepare_chunk_backing(const std::shared_ptr<const ChunkBackingDomain> &domain, detail::ChunkStageId stage,
                             detail::ChunkId chunk, uint32_t length, PagePurpose purpose,
                             const std::shared_ptr<AllocationBudget> &metadata, std::shared_ptr<ChunkPackBacking> *out);
Status pin_chunk_backing(const std::shared_ptr<const ChunkBackingDomain> &domain, detail::ChunkFixedPackRef pack,
                         detail::ChunkStageId pin, const std::shared_ptr<AllocationBudget> &metadata,
                         std::shared_ptr<const ChunkPackBacking> *out);

// Finite blocking backend steps; the caller must dispatch them on its bounded
// backend lane. Exact identities are never translated to synthetic file
// offsets.
Status read_chunk_backing(const ChunkPackBacking &backing, std::span<uint8_t> output,
                          const std::shared_ptr<AllocationBudget> &budget);
Status write_chunk_backing(ChunkPackBacking &backing, std::span<const uint8_t> input,
                           const std::shared_ptr<AllocationBudget> &budget);
} // namespace crowdb::tree
