// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "legacy_chunk_layout.h"
#include "runtime/tree_runtime_binding.h"

namespace crowdb::tree::detail
{
// Startup migration only. The immutable generation is pinned before any page
// I/O. Reads verify one public frame at a time and never instantiate a legacy
// writer, private executor, or mutable logical-address cache.
class LegacyChunkReadStore final : public PageStore
{
  public:
    LegacyChunkReadStore(std::shared_ptr<RootCatalog> catalog, std::shared_ptr<ChunkTransport> transport,
                         std::shared_ptr<AllocationReservation> metadata, uint64_t tree, uint64_t epoch,
                         uint32_t iu_size, uint32_t mirrors);
    Status open(uint64_t expected_generation);
    Status resolve_migration(uint64_t expected_generation, bool *committed);
    Status prepare_migration();
    Status release_pin();

    [[nodiscard]] uint64_t generation() const noexcept
    {
        return layout_.generation();
    }

    [[nodiscard]] uint64_t wal_offset() const noexcept
    {
        return layout_.wal_offset();
    }

    [[nodiscard]] const std::shared_ptr<RootCatalog> &catalog() const noexcept
    {
        return catalog_;
    }

    [[nodiscard]] const std::shared_ptr<ChunkTransport> &transport() const noexcept
    {
        return transport_;
    }

    [[nodiscard]] uint64_t size() const override
    {
        return layout_.logical_size();
    }

    [[nodiscard]] uint32_t iu_size() const override
    {
        return iu_size_;
    }

    Status read_at(uint64_t offset, uint8_t *data, size_t length) const override;
    Status write_at(uint64_t offset, const uint8_t *data, size_t length) override;
    Status sync() override;

  private:
    [[nodiscard]] Status read_pack(const ChunkPageRef &ref, uint64_t offset, std::span<uint8_t> output,
                                   std::span<uint8_t> scratch) const;
    Status read_frame(const ChunkPageRef &ref, size_t logical_offset, size_t logical_bytes, std::span<uint8_t> scratch,
                      std::span<const uint8_t> *payload) const;
    std::shared_ptr<RootCatalog>           catalog_;
    std::shared_ptr<ChunkTransport>        transport_;
    std::shared_ptr<AllocationReservation> metadata_;
    ChunkRootBytes                         manifest_;
    LegacyChunkLayout                      layout_;
    uint64_t                               tree_;
    uint64_t                               epoch_;
    uint32_t                               iu_size_;
    uint32_t                               mirrors_;
    bool                                   pinned_   = false;
    bool                                   verified_ = false;
};
} // namespace crowdb::tree::detail
