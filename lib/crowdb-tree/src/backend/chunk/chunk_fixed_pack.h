// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/allocation_reservation.h"
#include "chunk_page_store.h"

#include <span>

namespace crowdb::tree
{
class ChunkPhysicalRegistry;
struct ChunkPhysicalPack;
} // namespace crowdb::tree

namespace crowdb::tree::detail
{
// Immutable identity carried by each page; readers never consult the active
// manifest. Stage/manifest ownership is held separately from the byte window.
struct ChunkFixedPackRef
{
    ChunkStageId stage;
    ChunkId      chunk;
    uint64_t     offset   = 0;
    uint32_t     length   = 0;
    uint32_t     checksum = 0;
    PagePurpose  purpose  = PagePurpose::kInvalid;
};

struct ChunkPackWindow
{
    explicit ChunkPackWindow(const std::shared_ptr<AllocationBudget> &budget)
        : bytes(std::vector<uint8_t, BudgetAllocator<uint8_t, AllocationBudget, AllocationKind::kIo>>::allocator_type(
              budget))
    {
    }

    std::vector<uint8_t, BudgetAllocator<uint8_t, AllocationBudget, AllocationKind::kIo>> bytes;
};

// Blocking finite backend steps. Call only on the host's bounded backend lane;
// no Tree CPU task may wait for catalog or transport completion.
class ChunkFixedPackStore
{
  public:
    ChunkFixedPackStore(uint64_t tree, uint64_t epoch, uint32_t mirrors, std::shared_ptr<RootCatalog> catalog,
                        std::shared_ptr<ChunkTransport>        transport,
                        std::shared_ptr<ChunkPhysicalRegistry> registry = {});
    [[nodiscard]] bool                         supported() const noexcept;
    [[nodiscard]] static std::optional<size_t> write_allocation_bound(size_t bytes) noexcept;
    [[nodiscard]] static std::optional<size_t> read_allocation_bound(size_t bytes) noexcept;
    [[nodiscard]] static std::optional<size_t> read_into_allocation_bound(size_t bytes) noexcept;
    Status write(ChunkStageId operation, ChunkId fixed, PagePurpose purpose, std::span<const uint8_t> input,
                 const std::shared_ptr<AllocationBudget> &budget, ChunkFixedPackRef *out,
                 std::shared_ptr<ChunkPhysicalPack>      *physical        = nullptr,
                 const std::shared_ptr<AllocationBudget> &physical_budget = {});
    Status read(const ChunkFixedPackRef &ref, const std::shared_ptr<AllocationBudget> &budget,
                std::shared_ptr<const ChunkPackWindow> *out) const;
    [[nodiscard]] Status read_into(const ChunkFixedPackRef &ref, const std::shared_ptr<AllocationBudget> &budget,
                                   std::span<uint8_t> output) const;

  private:
    uint64_t                               tree_;
    uint64_t                               epoch_;
    uint32_t                               mirrors_;
    std::shared_ptr<RootCatalog>           catalog_;
    std::shared_ptr<ChunkTransport>        transport_;
    std::shared_ptr<ChunkPhysicalRegistry> registry_;
};

Status encode_fixed_pack(ChunkId id, PagePurpose purpose, std::span<const uint8_t> input, std::span<uint8_t> framed);
Status decode_fixed_pack(const ChunkFixedPackRef &ref, std::span<const uint8_t> framed, std::span<uint8_t> output);
} // namespace crowdb::tree::detail
