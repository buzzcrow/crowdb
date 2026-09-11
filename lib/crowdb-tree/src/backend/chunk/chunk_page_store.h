// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Private chunk-backend contracts.

#pragma once

#include "chunk_cancellation.h"
#include "chunk_transport.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/page_store.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace crowdb::tree::detail
{

inline constexpr uint32_t kChunkManifestFormat = 3;

class ChunkAsyncExecutor;
class ChunkPackPipeline;
class ChunkPackPipelineImpl;

struct ChunkPageRef
{
    ChunkId  chunk_id;
    uint64_t offset   = 0;
    uint32_t length   = 0;
    uint32_t checksum = 0;
};

struct ChunkPagePack
{
    uint64_t     owner_tree_id  = 0;
    uint64_t     ordinal        = 0;
    uint64_t     logical_offset = 0;
    ChunkPageRef ref;
    bool         reused = false;
};

struct ChunkReferenceSegment
{
    uint64_t owner_tree_id = 0;
    uint64_t object_id     = 0;
    uint64_t first_ordinal = 0;
    uint32_t ref_count     = 0;
    uint32_t checksum      = 0;
    bool     reused        = false;
};

struct ChunkReferenceSegmentImage
{
    uint64_t                  object_id     = 0;
    uint64_t                  first_ordinal = 0;
    std::vector<ChunkPageRef> refs;
};

struct ChunkManifest
{
    uint32_t                           format_version    = 0;
    uint64_t                           tree_id           = 0;
    uint64_t                           generation        = 0;
    uint64_t                           owner_epoch       = 0;
    uint64_t                           logical_size      = 0;
    uint64_t                           published_at_ms   = 0;
    uint64_t                           packs_reused      = 0;
    uint64_t                           pack_bytes_reused = 0;
    uint32_t                           checksum          = 0;
    std::vector<ChunkReferenceSegment> reference_segments;
    std::vector<ChunkPagePack>         packs;
};

inline uint64_t chunk_pack_owner(const ChunkManifest &manifest, const ChunkPagePack &pack)
{
    if (manifest.format_version >= 2) {
        return pack.owner_tree_id;
    }
    // Older manifests did not distinguish same-lineage reuse from inherited
    // reuse. Conservatively materialize every reused pack on the next pass.
    return pack.reused ? UINT64_MAX : manifest.tree_id;
}

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
    virtual uint64_t allocate_reference_segment_id(uint64_t tree_id)                                       = 0;
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
    uint64_t allocate_reference_segment_id(uint64_t tree_id) override;
    uint64_t discard_reference_segments(uint64_t tree_id, const std::vector<uint64_t> &object_ids) override;

    [[nodiscard]] uint64_t reference_segment_count(uint64_t tree_id) const;
    void                   corrupt_active_reference_segment(size_t segment_index, size_t ref_index);
    void                   corrupt_active_pack_layout_for_tests(uint64_t tree_id);
    void                   downgrade_active_manifest_for_tests(uint64_t tree_id);
    void                   block_next_publish_for_tests();
    void                   wait_for_blocked_publish_for_tests() const;
    void                   release_blocked_publish_for_tests();

  private:
    std::atomic<uint64_t> owner_epoch_;
    std::atomic<uint64_t> next_reference_segment_id_{1};
    using ManifestDirectory = std::vector<std::shared_ptr<const ChunkManifest>>;
    using ManifestHistory   = std::vector<std::shared_ptr<const ChunkManifest>>;

    struct CatalogState
    {
        ManifestDirectory current;
        ManifestHistory   history;
        bool              reclaiming = false;
    };

    std::atomic<std::shared_ptr<const CatalogState>> state_;

    struct StoredReferenceSegment
    {
        uint64_t                                          tree_id = 0;
        std::shared_ptr<const ChunkReferenceSegmentImage> image;
    };

    using ReferenceSegmentStore = std::vector<StoredReferenceSegment>;
    std::atomic<std::shared_ptr<const ReferenceSegmentStore>> reference_segment_store_;
    std::atomic<bool>                                         block_next_publish_{false};
    mutable std::atomic<bool>                                 publish_blocked_{false};
    std::atomic<bool>                                         release_publish_{false};
};

struct ChunkPageStoreStats
{
    uint64_t generations_published           = 0;
    uint64_t packs_written                   = 0;
    uint64_t pack_bytes_written              = 0;
    uint64_t packs_reused                    = 0;
    uint64_t pack_bytes_reused               = 0;
    uint64_t pack_reads                      = 0;
    uint64_t cache_hits                      = 0;
    uint64_t layout_queries                  = 0;
    uint64_t mirror_write_attempts           = 0;
    uint64_t mirror_write_failures           = 0;
    uint64_t retained_manifests              = 0;
    uint64_t pinned_bytes                    = 0;
    uint64_t oldest_pin_age_ms               = 0;
    uint64_t orphan_bytes                    = 0;
    uint64_t materialization_passes          = 0;
    uint64_t materialization_failures        = 0;
    uint64_t materialization_packs_written   = 0;
    uint64_t materialization_bytes_written   = 0;
    uint64_t shared_packs                    = 0;
    uint64_t rpc_operations                  = 0;
    uint64_t rpc_latency_ns                  = 0;
    uint64_t diskio_operations               = 0;
    uint64_t diskio_latency_ns               = 0;
    uint64_t coalesced_reads                 = 0;
    uint64_t coalesced_read_bytes            = 0;
    uint64_t completion_wakeups              = 0;
    uint64_t materialization_scan_bytes      = 0;
    uint64_t shared_metadata_segments        = 0;
    uint64_t materialized_metadata_segments  = 0;
    uint64_t manifest_publication_latency_ns = 0;
    uint64_t recovery_latency_ns             = 0;
};

class ChunkPageStore final : public PageStore, public AsyncPageStore
{
  public:
    struct Config
    {
        uint64_t tree_id                        = 0;
        uint64_t owner_epoch                    = 0;
        size_t   pack_bytes                     = 4U * 1024U * 1024U;
        uint64_t max_chunk_bytes                = 256U * 1024U * 1024U;
        uint32_t page_alignment                 = 64U * 1024U;
        uint32_t iu_size                        = 64U * 1024U;
        uint32_t mirror_retry_limit             = 2;
        uint64_t layout_validity_ms             = 30'000;
        size_t   max_pending_ops                = 256;
        size_t   max_concurrent_packs           = 8;
        uint64_t materialization_bytes_per_pass = 64U * 1024U * 1024U;
    };

    ChunkPageStore(Config config, std::shared_ptr<RootCatalog> catalog, std::shared_ptr<ChunkTransport> transport);
    ~ChunkPageStore() override;

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

    Status encode_mapping_location(uint64_t addr, uint32_t logical_len, uint64_t *word) const override;
    Status decode_mapping_location(uint64_t word, uint64_t *addr, uint32_t *physical_len) const override;

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
    Status                            materialize_ownership(uint64_t *bytes_written, bool *complete) override;
    void set_materialization_live_extents(std::vector<std::pair<uint64_t, uint64_t>> extents) override;

    // Seed an unpublished destination from one immutable source generation.
    // Byte-identical packs are referenced directly by the next manifest;
    // changed packs are written through the normal mirror pipeline.
    Status inherit_snapshot_from(const PageStore &source) override;

    [[nodiscard]] bool has_inherited_snapshot() const override
    {
        return inherited_manifest_ != nullptr;
    }

    [[nodiscard]] bool inherited_snapshot_matches(const PageStore &source) const override;
    [[nodiscard]] bool has_shared_ownership() const override;

  private:
    friend class ChunkAsyncExecutor;
    friend class ChunkPackPipeline;
    friend class ChunkPackPipelineImpl;
    friend class MemoryRootCatalog;

    struct CachedPack
    {
        ChunkPageRef                                ref;
        std::shared_ptr<const std::vector<uint8_t>> bytes;
    };

    Status materialize_active(std::vector<uint8_t> *out) const;
    Status build_manifest(uint64_t expected_generation, std::shared_ptr<ChunkManifest> *out, uint64_t *new_pack_bytes,
                          ChunkCancellation cancellation = {});
    Status read_at_cancellable(uint64_t off, uint8_t *buf, size_t len, ChunkCancellation cancellation) const;
    Status sync_cancellable(ChunkCancellation cancellation);
    std::shared_ptr<void> start_sync_cancellable(ChunkCancellation cancellation, AsyncCompletion completion);
    Status                finish_sync_cancellable(const std::shared_ptr<void> &state, ChunkCancellation cancellation,
                                                  Status io_status);
    Status                read_pack(const ChunkPageRef &ref, std::shared_ptr<const std::vector<uint8_t>> *out,
                                    ChunkCancellation cancellation) const;
    Status                load_layout(std::shared_ptr<const ChunkManifest> *out) const;
    Status                validate_manifest(const ChunkManifest &manifest, const RootCatalog &catalog) const;
    Status                persist_reference_segments(ChunkManifest *manifest, const ChunkManifest *reuse_base);
    [[nodiscard]] std::shared_ptr<const ChunkManifest> reuse_base_manifest() const;
    [[nodiscard]] const ChunkPagePack        *find_reusable_pack(const ChunkManifest &base, uint64_t logical_offset,
                                                                 uint32_t length, uint32_t checksum,
                                                                 ChunkCancellation cancellation = {}) const;
    [[nodiscard]] static const ChunkPagePack *find_pack_at(const ChunkManifest &base, uint64_t logical_offset,
                                                           uint32_t length);
    static uint32_t                           reference_segment_checksum(const ChunkReferenceSegmentImage &segment);
    static uint32_t                           manifest_checksum(const ChunkManifest &manifest);
    static std::vector<uint64_t>              reference_segment_ids(const ChunkManifest &manifest);
    [[nodiscard]] bool                        pack_is_live(const ChunkPagePack &pack) const;
    [[nodiscard]] bool                        range_was_written(uint64_t offset, uint64_t length) const;
    void                                      record_completion_wakeup();

    Config                                                    config_;
    std::shared_ptr<RootCatalog>                              catalog_;
    std::shared_ptr<ChunkTransport>                           transport_;
    std::vector<uint8_t>                                      staged_;
    std::vector<std::pair<uint64_t, uint64_t>>                dirty_ranges_;
    std::vector<std::pair<uint64_t, uint64_t>>                materialization_live_extents_;
    bool                                                      staged_initialized_ = false;
    bool                                                      data_durable_       = false;
    bool                                                      anchor_dirty_       = false;
    std::atomic<bool>                                         unavailable_{false};
    std::atomic<uint8_t>                                      mirror_write_failure_mask_{0};
    mutable std::atomic<std::shared_ptr<const ChunkManifest>> cached_layout_;
    mutable std::shared_ptr<const CachedPack>                 cached_pack_;
    mutable std::atomic<uint64_t>                             layout_valid_until_ms_{0};
    std::atomic<uint64_t>                                     generations_published_{0};
    std::atomic<uint64_t>                                     packs_written_{0};
    std::atomic<uint64_t>                                     pack_bytes_written_{0};
    std::atomic<uint64_t>                                     packs_reused_{0};
    std::atomic<uint64_t>                                     pack_bytes_reused_{0};
    mutable std::atomic<uint64_t>                             pack_reads_{0};
    mutable std::atomic<uint64_t>                             cache_hits_{0};
    mutable std::atomic<uint64_t>                             layout_queries_{0};
    std::atomic<uint64_t>                                     mirror_write_attempts_{0};
    std::atomic<uint64_t>                                     mirror_write_failures_{0};
    std::atomic<uint64_t>                                     orphan_bytes_{0};
    std::atomic<uint64_t>                                     materialization_passes_{0};
    std::atomic<uint64_t>                                     materialization_failures_{0};
    std::atomic<uint64_t>                                     materialization_packs_written_{0};
    std::atomic<uint64_t>                                     materialization_bytes_written_{0};
    mutable std::atomic<uint64_t>                             rpc_operations_{0};
    mutable std::atomic<uint64_t>                             rpc_latency_ns_{0};
    mutable std::atomic<uint64_t>                             diskio_operations_{0};
    mutable std::atomic<uint64_t>                             diskio_latency_ns_{0};
    mutable std::atomic<uint64_t>                             coalesced_reads_{0};
    mutable std::atomic<uint64_t>                             coalesced_read_bytes_{0};
    std::atomic<uint64_t>                                     completion_wakeups_{0};
    std::atomic<uint64_t>                                     materialization_scan_bytes_{0};
    std::atomic<uint64_t>                                     materialized_metadata_segments_{0};
    std::atomic<uint64_t>                                     manifest_publication_latency_ns_{0};
    mutable std::atomic<uint64_t>                             recovery_latency_ns_{0};
    mutable std::atomic<bool>                                 recovery_recorded_{false};
    std::vector<uint64_t>                                     orphan_reference_segments_;
    ChunkId                                                   active_chunk_id_;
    uint64_t                                                  active_chunk_bytes_  = 0;
    uint64_t                                                  active_chunk_cursor_ = 0;
    std::shared_ptr<const ChunkManifest>                      inherited_manifest_;
    std::shared_ptr<RootCatalog>                              inherited_catalog_;
    std::unique_ptr<ChunkAsyncExecutor>                       async_executor_;
};

} // namespace crowdb::tree::detail
