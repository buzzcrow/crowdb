// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Private chunk-backend contracts.

#pragma once

#include "chunk_transport.h"
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
    ChunkId  chunk_id;
    uint64_t offset   = 0;
    uint32_t length   = 0;
    uint32_t checksum = 0;
};

struct ChunkPagePack
{
    uint64_t     ordinal        = 0;
    uint64_t     logical_offset = 0;
    ChunkPageRef ref;
};

struct ChunkReferenceSegment
{
    uint64_t object_id     = 0;
    uint64_t first_ordinal = 0;
    uint32_t ref_count     = 0;
    uint32_t checksum      = 0;
};

struct ChunkReferenceSegmentImage
{
    uint64_t                  object_id     = 0;
    uint64_t                  first_ordinal = 0;
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
    virtual Status persist_reference_segment(uint64_t                                          tree_id,
                                             std::shared_ptr<const ChunkReferenceSegmentImage> segment)   = 0;
    [[nodiscard]] virtual std::shared_ptr<const ChunkReferenceSegmentImage>
                     load_reference_segment(uint64_t tree_id, uint64_t object_id) const                    = 0;
    virtual uint64_t discard_reference_segments(uint64_t tree_id, const std::vector<uint64_t> &object_ids) = 0;
    virtual Status   publish(uint64_t tree_id, uint64_t expected_generation, uint64_t owner_epoch,
                             std::shared_ptr<const ChunkManifest> manifest)                                = 0;
    virtual uint64_t reclaim_before(uint64_t tree_id, uint64_t generation)                                 = 0;
    [[nodiscard]] virtual uint64_t retained_manifest_count(uint64_t tree_id) const                         = 0;
    [[nodiscard]] virtual uint64_t pinned_bytes(uint64_t tree_id) const                                    = 0;
    [[nodiscard]] virtual uint64_t oldest_pin_age_ms(uint64_t tree_id) const                               = 0;
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

    [[nodiscard]] std::shared_ptr<const ChunkManifest> load_generation(uint64_t tree_id,
                                                                       uint64_t generation) const override;
    uint64_t                                           reclaim_before(uint64_t tree_id, uint64_t generation) override;
    [[nodiscard]] uint64_t                             retained_manifest_count(uint64_t tree_id) const override;
    [[nodiscard]] uint64_t                             pinned_bytes(uint64_t tree_id) const override;
    [[nodiscard]] uint64_t                             oldest_pin_age_ms(uint64_t tree_id) const override;
    Status persist_reference_segment(uint64_t                                          tree_id,
                                     std::shared_ptr<const ChunkReferenceSegmentImage> segment) override;
    [[nodiscard]] std::shared_ptr<const ChunkReferenceSegmentImage>
             load_reference_segment(uint64_t tree_id, uint64_t object_id) const override;
    uint64_t discard_reference_segments(uint64_t tree_id, const std::vector<uint64_t> &object_ids) override;

    [[nodiscard]] uint64_t reference_segment_count(uint64_t tree_id) const;
    void                   corrupt_active_reference_segment(size_t segment_index, size_t ref_index);

  private:
    std::atomic<uint64_t>                             owner_epoch_;
    std::atomic<std::shared_ptr<const ChunkManifest>> current_;
    using ManifestHistory = std::vector<std::shared_ptr<const ChunkManifest>>;
    std::atomic<std::shared_ptr<const ManifestHistory>> history_;

    struct StoredReferenceSegment
    {
        uint64_t                                          tree_id = 0;
        std::shared_ptr<const ChunkReferenceSegmentImage> image;
    };

    using ReferenceSegmentStore = std::vector<StoredReferenceSegment>;
    std::atomic<std::shared_ptr<const ReferenceSegmentStore>> reference_segment_store_;
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
        uint64_t max_chunk_bytes    = 256U * 1024U * 1024U;
        uint32_t page_alignment     = 64U * 1024U;
        uint32_t iu_size            = 64U * 1024U;
        uint32_t mirror_retry_limit = 2;
        uint64_t layout_validity_ms = 30'000;
    };

    ChunkPageStore(Config config, std::shared_ptr<RootCatalog> catalog, std::shared_ptr<ChunkTransport> transport);

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
    Status          resolve_ordinal(const ChunkManifest &manifest, uint64_t ordinal, ChunkPageRef *out) const;
    static uint32_t reference_segment_checksum(const ChunkReferenceSegmentImage &segment);
    static uint32_t manifest_checksum(const ChunkManifest &manifest);
    static std::vector<uint64_t> reference_segment_ids(const ChunkManifest &manifest);

    Config                                                    config_;
    std::shared_ptr<RootCatalog>                              catalog_;
    std::shared_ptr<ChunkTransport>                           transport_;
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
    std::vector<uint64_t>                                     orphan_reference_segments_;
    ChunkId                                                   active_chunk_id_;
    uint64_t                                                  active_chunk_bytes_  = 0;
    uint64_t                                                  active_chunk_cursor_ = 0;
};

} // namespace crowdb::tree::detail
