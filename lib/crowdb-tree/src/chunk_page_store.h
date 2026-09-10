// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/async_page_store.h"
#include "crowdb-tree/page_store.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace crowdb::tree::detail
{

struct ChunkPageRef
{
    uint64_t chunk_id = 0;
    uint64_t offset   = 0;
    uint32_t length   = 0;
    uint32_t checksum = 0;
};

struct ChunkPagePack
{
    uint64_t                            ordinal = 0;
    ChunkPageRef                        ref;
    std::array<std::vector<uint8_t>, 3> mirrors;
};

struct ChunkReferenceSegment
{
    uint64_t                  first_ordinal = 0;
    uint32_t                  checksum      = 0;
    std::vector<ChunkPageRef> refs;
};

struct ChunkManifest
{
    uint64_t                           tree_id         = 0;
    uint64_t                           generation      = 0;
    uint64_t                           owner_epoch     = 0;
    uint64_t                           logical_size    = 0;
    uint64_t                           published_at_ms = 0;
    uint32_t                           checksum        = 0;
    std::vector<ChunkReferenceSegment> reference_segments;
    std::vector<ChunkPagePack>         packs;
};

class RootCatalog
{
  public:
    virtual ~RootCatalog() = default;

    [[nodiscard]] virtual std::shared_ptr<const ChunkManifest> load(uint64_t tree_id) const               = 0;
    [[nodiscard]] virtual std::shared_ptr<const ChunkManifest> load_generation(uint64_t tree_id,
                                                                               uint64_t generation) const = 0;
    virtual Status                 publish(uint64_t tree_id, uint64_t expected_generation, uint64_t owner_epoch,
                                           std::shared_ptr<const ChunkManifest> manifest)                 = 0;
    virtual uint64_t               reclaim_before(uint64_t tree_id, uint64_t generation)                  = 0;
    [[nodiscard]] virtual uint64_t retained_manifest_count(uint64_t tree_id) const                        = 0;
    [[nodiscard]] virtual uint64_t pinned_bytes(uint64_t tree_id) const                                   = 0;
    [[nodiscard]] virtual uint64_t oldest_pin_age_ms(uint64_t tree_id) const                              = 0;
};

// Test and embeddable catalog implementation. Its read side is one atomic
// immutable pointer load; publication uses generation CAS and never adds a
// mutex to page lookup.
class MemoryRootCatalog final : public RootCatalog
{
  public:
    explicit MemoryRootCatalog(uint64_t owner_epoch) : owner_epoch_(owner_epoch)
    {
    }

    [[nodiscard]] std::shared_ptr<const ChunkManifest> load(uint64_t tree_id) const override;
    Status publish(uint64_t tree_id, uint64_t expected_generation, uint64_t owner_epoch,
                   std::shared_ptr<const ChunkManifest> manifest) override;

    void set_owner_epoch(uint64_t epoch)
    {
        owner_epoch_.store(epoch, std::memory_order_release);
    }

    // Storage-fault seam used by checksum tests.
    void corrupt_active_pack(size_t pack_index, size_t byte_index);
    void corrupt_active_mirror(size_t pack_index, size_t mirror_index, size_t byte_index);

    [[nodiscard]] std::shared_ptr<const ChunkManifest> load_generation(uint64_t tree_id,
                                                                       uint64_t generation) const override;
    uint64_t                                           reclaim_before(uint64_t tree_id, uint64_t generation) override;
    [[nodiscard]] uint64_t                             retained_manifest_count(uint64_t tree_id) const override;
    [[nodiscard]] uint64_t                             pinned_bytes(uint64_t tree_id) const override;
    [[nodiscard]] uint64_t                             oldest_pin_age_ms(uint64_t tree_id) const override;

  private:
    std::atomic<uint64_t>                             owner_epoch_;
    std::atomic<std::shared_ptr<const ChunkManifest>> current_;
    using ManifestHistory = std::vector<std::shared_ptr<const ChunkManifest>>;
    std::atomic<std::shared_ptr<const ManifestHistory>> history_;
};

struct ChunkPageStoreStats
{
    uint64_t generations_published = 0;
    uint64_t packs_written         = 0;
    uint64_t pack_bytes_written    = 0;
    uint64_t pack_reads            = 0;
    uint64_t cache_hits            = 0;
    uint64_t layout_queries        = 0;
    uint64_t mirror_write_attempts = 0;
    uint64_t mirror_write_failures = 0;
    uint64_t retained_manifests    = 0;
    uint64_t pinned_bytes          = 0;
    uint64_t oldest_pin_age_ms     = 0;
    uint64_t orphan_bytes          = 0;
};

class ChunkPageStore final : public PageStore, public AsyncPageStore
{
  public:
    struct Config
    {
        uint64_t tree_id            = 0;
        uint64_t owner_epoch        = 0;
        size_t   pack_bytes         = 4U * 1024U * 1024U;
        uint32_t iu_size            = 1;
        uint32_t mirror_retry_limit = 2;
        uint64_t layout_validity_ms = 30'000;
    };

    ChunkPageStore(Config config, std::shared_ptr<RootCatalog> catalog);

    Status                 write_at(uint64_t off, const uint8_t *buf, size_t len) override;
    Status                 read_at(uint64_t off, uint8_t *buf, size_t len) const override;
    Status                 sync() override;
    [[nodiscard]] uint64_t size() const override;
    uint64_t               submit_read(PageAddr addr, void *buf, size_t len, AsyncCompletion on_complete) override;
    uint64_t submit_write(PageAddr addr, const void *buf, size_t len, AsyncCompletion on_complete) override;
    Status   submit_fsync(AsyncCompletion on_complete) override;
    void     cancel(uint64_t op_id) override;

    [[nodiscard]] uint32_t iu_size() const override
    {
        return config_.iu_size;
    }

    void inject_unavailable(bool unavailable)
    {
        unavailable_.store(unavailable, std::memory_order_release);
    }

    void inject_mirror_write_failures(uint8_t mirror_mask)
    {
        mirror_write_failure_mask_.store(mirror_mask, std::memory_order_release);
    }

    [[nodiscard]] ChunkPageStoreStats stats() const;
    uint64_t                          reclaim_orphans();

  private:
    Status                               materialize_active(std::vector<uint8_t> *out) const;
    Status                               build_manifest(std::shared_ptr<ChunkManifest> *out);
    std::shared_ptr<const ChunkManifest> load_layout() const;
    static const ChunkPageRef           *resolve_ordinal(const ChunkManifest &manifest, uint64_t ordinal);
    static uint32_t                      reference_segment_checksum(const ChunkReferenceSegment &segment);
    static uint32_t                      manifest_checksum(const ChunkManifest &manifest);

    Config                                                    config_;
    std::shared_ptr<RootCatalog>                              catalog_;
    std::vector<uint8_t>                                      staged_;
    bool                                                      staged_initialized_ = false;
    bool                                                      data_durable_       = false;
    bool                                                      anchor_dirty_       = false;
    std::atomic<bool>                                         unavailable_{false};
    std::atomic<uint8_t>                                      mirror_write_failure_mask_{0};
    mutable std::atomic<std::shared_ptr<const ChunkManifest>> cached_layout_;
    mutable std::atomic<uint64_t>                             layout_valid_until_ms_{0};
    std::atomic<uint64_t>                                     generations_published_{0};
    std::atomic<uint64_t>                                     packs_written_{0};
    std::atomic<uint64_t>                                     pack_bytes_written_{0};
    mutable std::atomic<uint64_t>                             pack_reads_{0};
    mutable std::atomic<uint64_t>                             cache_hits_{0};
    mutable std::atomic<uint64_t>                             layout_queries_{0};
    std::atomic<uint64_t>                                     mirror_write_attempts_{0};
    std::atomic<uint64_t>                                     mirror_write_failures_{0};
    std::atomic<uint64_t>                                     orphan_bytes_{0};
};

} // namespace crowdb::tree::detail
