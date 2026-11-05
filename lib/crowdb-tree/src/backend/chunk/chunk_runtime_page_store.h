// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "chunk_backing.h"

namespace crowdb::tree::detail
{
struct ChunkRuntimeOptions
{
    uint64_t tree            = 0;
    uint64_t owner_epoch     = 0;
    uint64_t open_generation = 0;
    uint32_t iu_size         = 64 * 1024;
    uint32_t mirrors         = 2;
    uint64_t max_chunk_bytes = 256 * 1024 * 1024;
};

// This injected handle only identifies the service and root namespace. It
// never opens the legacy manifest layout or starts a private executor.
class ChunkRuntimePageStore final : public PageStore
{
  public:
    ChunkRuntimePageStore(ChunkRuntimeOptions options, std::shared_ptr<RootCatalog> catalog,
                          std::shared_ptr<ChunkTransport> transport, const std::shared_ptr<AllocationBudget> &budget);

    ChunkRuntimePageStore(ChunkRuntimeOptions options, std::shared_ptr<RootCatalog> catalog,
                          std::shared_ptr<ChunkTransport> transport, AllocationLease allocation,
                          const void *host_domain = nullptr);

    [[nodiscard]] const ChunkRuntimeOptions &options() const noexcept
    {
        return options_;
    }

    [[nodiscard]] bool matches_host(const void *host_domain) const noexcept
    {
        return host_domain_ == nullptr || host_domain_ == host_domain;
    }

    [[nodiscard]] const std::shared_ptr<RootCatalog> &catalog() const noexcept
    {
        return catalog_;
    }

    [[nodiscard]] const std::shared_ptr<ChunkTransport> &transport() const noexcept
    {
        return transport_;
    }

    [[nodiscard]] RuntimeFactory runtime_factory() const noexcept override;
    [[nodiscard]] bool           supported() const noexcept;
    Status                       make_domain(uint64_t identity, const std::shared_ptr<AllocationBudget> &budget,
                                             std::shared_ptr<const ChunkBackingDomain> *out) const;
    Status                       write_at(uint64_t offset, const uint8_t *data, size_t length) override;
    Status                       read_at(uint64_t offset, uint8_t *data, size_t length) const override;
    Status                       sync() override;

    [[nodiscard]] uint64_t size() const override
    {
        return 0;
    }

    [[nodiscard]] uint32_t iu_size() const override
    {
        return options_.iu_size;
    }

    [[nodiscard]] bool has_shared_ownership() const override
    {
        return true;
    }

    void request_wal_offset(uint64_t value) noexcept;

    [[nodiscard]] uint64_t requested_wal_offset() const noexcept
    {
        return wal_requested_.load(std::memory_order_acquire);
    }

    [[nodiscard]] uint64_t durable_wal_offset() const noexcept
    {
        return wal_committed_.load(std::memory_order_acquire);
    }

    [[nodiscard]] uint64_t manifest_generation() const noexcept
    {
        return generation_.load(std::memory_order_acquire);
    }

    void committed(uint64_t generation, uint64_t wal_offset) noexcept;

  private:
    AllocationLease                                allocation_;
    const void                                    *host_domain_ = nullptr;
    ChunkRuntimeOptions                            options_;
    std::shared_ptr<RootCatalog>                   catalog_;
    std::shared_ptr<ChunkTransport>                transport_;
    mutable std::shared_ptr<ChunkPhysicalRegistry> physical_registry_;
    std::atomic<uint64_t>                          wal_requested_{0};
    std::atomic<uint64_t>                          wal_committed_{0};
    std::atomic<uint64_t>                          generation_{0};
};
} // namespace crowdb::tree::detail
