// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Chunk-backend implementation.

#include "chunk_page_store.h"

#include "c_api_internal.h"
#include "chunk_async_executor.h"
#include "chunk_c_api_internal.h"
#include "chunk_pack_pipeline.h"
#include "crowdb-common/crc32c.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <unordered_set>

namespace crowdb::tree::detail
{
namespace
{

constexpr uint64_t kAnchorRegionBytes    = 8192;
constexpr size_t   kReferencesPerSegment = 256;
constexpr uint64_t kMaxChunkBytes        = 256U * 1024U * 1024U;

uint32_t update_u64(uint32_t crc, uint64_t value)
{
    return crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&value), sizeof(value));
}

uint64_t monotonic_millis()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

uint64_t reference_segment_bytes(const ChunkReferenceSegmentImage &segment)
{
    return sizeof(segment.object_id) + sizeof(segment.first_ordinal) + segment.refs.size() * sizeof(ChunkPageRef);
}

struct PackIdentity
{
    ChunkId  chunk_id;
    uint64_t offset;
    uint32_t length;
    uint32_t checksum;

    bool operator==(const PackIdentity &) const = default;
};

struct PackIdentityHash
{
    size_t operator()(const PackIdentity &pack) const
    {
        size_t hash = std::hash<uint64_t>{}(pack.chunk_id.high);
        hash ^= std::hash<uint64_t>{}(pack.chunk_id.low) + 0x9e3779b9U + (hash << 6U) + (hash >> 2U);
        hash ^= std::hash<uint64_t>{}(pack.offset) + 0x9e3779b9U + (hash << 6U) + (hash >> 2U);
        hash ^= std::hash<uint32_t>{}(pack.length) + 0x9e3779b9U + (hash << 6U) + (hash >> 2U);
        hash ^= std::hash<uint32_t>{}(pack.checksum) + 0x9e3779b9U + (hash << 6U) + (hash >> 2U);
        return hash;
    }
};

struct ReferenceSegmentIdentity
{
    uint64_t owner_tree_id;
    uint64_t object_id;

    bool operator==(const ReferenceSegmentIdentity &) const = default;
};

struct ReferenceSegmentIdentityHash
{
    size_t operator()(const ReferenceSegmentIdentity &segment) const
    {
        size_t hash = std::hash<uint64_t>{}(segment.owner_tree_id);
        hash ^= std::hash<uint64_t>{}(segment.object_id) + 0x9e3779b9U + (hash << 6U) + (hash >> 2U);
        return hash;
    }
};

uint64_t reference_segment_owner(const ChunkManifest &manifest, const ChunkReferenceSegment &segment)
{
    return manifest.format_version == 0 ? manifest.tree_id : segment.owner_tree_id;
}

PackIdentity pack_identity(const ChunkPagePack &pack)
{
    return {.chunk_id = pack.ref.chunk_id,
            .offset   = pack.ref.offset,
            .length   = pack.ref.length,
            .checksum = pack.ref.checksum};
}

} // namespace

std::shared_ptr<const ChunkManifest> MemoryRootCatalog::load(uint64_t tree_id) const
{
    auto state = state_.load(std::memory_order_acquire);
    if (state == nullptr) {
        return nullptr;
    }
    auto found = std::find_if(state->current.begin(), state->current.end(),
                              [tree_id](const auto &manifest) { return manifest->tree_id == tree_id; });
    return found == state->current.end() ? nullptr : *found;
}

Status MemoryRootCatalog::publish(uint64_t tree_id, uint64_t expected_generation, uint64_t owner_epoch,
                                  std::shared_ptr<const ChunkManifest> manifest)
{
    if (manifest == nullptr || manifest->tree_id != tree_id || manifest->owner_epoch != owner_epoch) {
        return Status::invalid_argument("chunk manifest identity mismatch");
    }
    if (expected_generation == std::numeric_limits<uint64_t>::max() ||
        manifest->generation != expected_generation + 1) {
        return Status::invalid_argument("chunk manifest generation does not follow its publication fence");
    }
    if (owner_epoch_.load(std::memory_order_acquire) != owner_epoch) {
        return Status::unavailable("chunk root publication fenced by owner epoch");
    }
    if (block_next_publish_.exchange(false, std::memory_order_acq_rel)) {
        publish_blocked_.store(true, std::memory_order_release);
        publish_blocked_.notify_all();
        release_publish_.wait(false, std::memory_order_acquire);
        publish_blocked_.store(false, std::memory_order_release);
    }
    auto state = state_.load(std::memory_order_acquire);
    for (;;) {
        auto           found = state == nullptr
                                 ? ManifestDirectory::const_iterator{}
                                 : std::find_if(state->current.begin(), state->current.end(),
                                                [tree_id](const auto &entry) { return entry->tree_id == tree_id; });
        const uint64_t current_generation =
            state == nullptr || found == state->current.end() ? 0 : (*found)->generation;
        if (current_generation != expected_generation) {
            return Status::unavailable("chunk root publication lost generation race");
        }
        auto next = state == nullptr ? std::make_shared<CatalogState>() : std::make_shared<CatalogState>(*state);
        if (state == nullptr || found == state->current.end()) {
            next->current.push_back(manifest);
        }
        else {
            next->current[static_cast<size_t>(found - state->current.begin())] = manifest;
        }
        next->history.push_back(manifest);
        if (state_.compare_exchange_weak(state, next, std::memory_order_release, std::memory_order_acquire)) {
            return Status::Ok();
        }
    }
}

std::shared_ptr<const ChunkManifest> MemoryRootCatalog::load_generation(uint64_t tree_id, uint64_t generation) const
{
    auto state = state_.load(std::memory_order_acquire);
    if (state == nullptr) {
        return nullptr;
    }
    for (const auto &manifest : state->history) {
        if (manifest->tree_id == tree_id && manifest->generation == generation) {
            return manifest;
        }
    }
    return nullptr;
}

Status MemoryRootCatalog::persist_reference_segment(uint64_t                                          tree_id,
                                                    std::shared_ptr<const ChunkReferenceSegmentImage> segment)
{
    if (segment == nullptr || segment->object_id == 0) {
        return Status::invalid_argument("chunk reference segment identity is invalid");
    }
    auto current = reference_segment_store_.load(std::memory_order_acquire);
    for (;;) {
        auto       next = current == nullptr ? std::make_shared<ReferenceSegmentStore>()
                                             : std::make_shared<ReferenceSegmentStore>(*current);
        const auto duplicate =
            std::find_if(next->begin(), next->end(),
                         [tree_id, object_id = segment->object_id](const StoredReferenceSegment &stored) {
                             return stored.tree_id == tree_id && stored.image->object_id == object_id;
                         });
        if (duplicate != next->end()) {
            return Status::invalid_argument("chunk reference segment identity already exists");
        }
        next->push_back({.tree_id = tree_id, .image = segment});
        if (reference_segment_store_.compare_exchange_weak(current, next, std::memory_order_release,
                                                           std::memory_order_acquire)) {
            return Status::Ok();
        }
    }
}

std::shared_ptr<const ChunkReferenceSegmentImage> MemoryRootCatalog::load_reference_segment(uint64_t tree_id,
                                                                                            uint64_t object_id) const
{
    auto store = reference_segment_store_.load(std::memory_order_acquire);
    if (store == nullptr) {
        return nullptr;
    }
    const auto found = std::find_if(store->begin(), store->end(), [tree_id, object_id](const auto &stored) {
        return stored.tree_id == tree_id && stored.image->object_id == object_id;
    });
    return found == store->end() ? nullptr : found->image;
}

uint64_t MemoryRootCatalog::allocate_reference_segment_id(uint64_t)
{
    return next_reference_segment_id_.fetch_add(1, std::memory_order_relaxed);
}

void MemoryRootCatalog::block_next_publish_for_tests()
{
    release_publish_.store(false, std::memory_order_release);
    publish_blocked_.store(false, std::memory_order_release);
    block_next_publish_.store(true, std::memory_order_release);
}

void MemoryRootCatalog::wait_for_blocked_publish_for_tests() const
{
    publish_blocked_.wait(false, std::memory_order_acquire);
}

void MemoryRootCatalog::release_blocked_publish_for_tests()
{
    release_publish_.store(true, std::memory_order_release);
    release_publish_.notify_all();
}

uint64_t MemoryRootCatalog::discard_reference_segments(uint64_t tree_id, const std::vector<uint64_t> &object_ids)
{
    if (object_ids.empty()) {
        return 0;
    }
    const std::unordered_set<uint64_t> discarded_ids(object_ids.begin(), object_ids.end());
    auto                               current = reference_segment_store_.load(std::memory_order_acquire);
    for (;;) {
        if (current == nullptr) {
            return 0;
        }
        auto     next      = std::make_shared<ReferenceSegmentStore>();
        uint64_t discarded = 0;
        for (const StoredReferenceSegment &stored : *current) {
            if (stored.tree_id == tree_id && discarded_ids.contains(stored.image->object_id)) {
                discarded += reference_segment_bytes(*stored.image);
            }
            else {
                next->push_back(stored);
            }
        }
        if (reference_segment_store_.compare_exchange_weak(current, next, std::memory_order_release,
                                                           std::memory_order_acquire)) {
            return discarded;
        }
    }
}

uint64_t MemoryRootCatalog::reference_segment_count(uint64_t tree_id) const
{
    auto store = reference_segment_store_.load(std::memory_order_acquire);
    return store == nullptr ? 0 : std::count_if(store->begin(), store->end(), [tree_id](const auto &stored) {
        return stored.tree_id == tree_id;
    });
}

void MemoryRootCatalog::corrupt_active_reference_segment(size_t segment_index, size_t ref_index)
{
    auto state = state_.load(std::memory_order_acquire);
    if (state == nullptr || state->current.empty()) {
        return;
    }
    auto manifest = state->current.back();
    if (segment_index >= manifest->reference_segments.size()) {
        return;
    }
    const ChunkReferenceSegment &descriptor    = manifest->reference_segments[segment_index];
    const uint64_t               owner_tree_id = reference_segment_owner(*manifest, descriptor);
    const uint64_t               object_id     = descriptor.object_id;
    auto                         current       = reference_segment_store_.load(std::memory_order_acquire);
    for (;;) {
        if (current == nullptr) {
            return;
        }
        auto next  = std::make_shared<ReferenceSegmentStore>(*current);
        auto found = std::find_if(next->begin(), next->end(), [owner_tree_id, object_id](auto &stored) {
            return stored.tree_id == owner_tree_id && stored.image->object_id == object_id;
        });
        if (found == next->end() || ref_index >= found->image->refs.size()) {
            return;
        }
        auto corrupt = std::make_shared<ChunkReferenceSegmentImage>(*found->image);
        corrupt->refs[ref_index].checksum ^= 0xffU;
        found->image = std::move(corrupt);
        if (reference_segment_store_.compare_exchange_weak(current, next, std::memory_order_release,
                                                           std::memory_order_acquire)) {
            return;
        }
    }
}

void MemoryRootCatalog::corrupt_active_pack_layout_for_tests(uint64_t tree_id)
{
    auto state = state_.load(std::memory_order_acquire);
    for (;;) {
        if (state == nullptr) {
            return;
        }
        auto found = std::find_if(state->current.begin(), state->current.end(),
                                  [tree_id](const auto &manifest) { return manifest->tree_id == tree_id; });
        if (found == state->current.end() || (*found)->packs.empty()) {
            return;
        }
        auto corrupt = std::make_shared<ChunkManifest>(**found);
        ++corrupt->packs.front().logical_offset;
        corrupt->checksum = ChunkPageStore::manifest_checksum(*corrupt);
        auto next         = std::make_shared<CatalogState>(*state);
        next->current[static_cast<size_t>(found - state->current.begin())] = std::move(corrupt);
        if (state_.compare_exchange_weak(state, next, std::memory_order_release, std::memory_order_acquire)) {
            return;
        }
    }
}

void MemoryRootCatalog::downgrade_active_manifest_for_tests(uint64_t tree_id)
{
    auto state = state_.load(std::memory_order_acquire);
    for (;;) {
        if (state == nullptr) {
            return;
        }
        auto found = std::find_if(state->current.begin(), state->current.end(),
                                  [tree_id](const auto &manifest) { return manifest->tree_id == tree_id; });
        if (found == state->current.end()) {
            return;
        }
        auto legacy            = std::make_shared<ChunkManifest>(**found);
        legacy->format_version = 0;
        for (ChunkReferenceSegment &segment : legacy->reference_segments) {
            segment.owner_tree_id = 0;
            segment.reused        = false;
        }
        legacy->checksum                                                   = ChunkPageStore::manifest_checksum(*legacy);
        auto next                                                          = std::make_shared<CatalogState>(*state);
        next->current[static_cast<size_t>(found - state->current.begin())] = std::move(legacy);
        if (state_.compare_exchange_weak(state, next, std::memory_order_release, std::memory_order_acquire)) {
            return;
        }
    }
}

uint64_t MemoryRootCatalog::reclaim_before(uint64_t tree_id, uint64_t generation)
{
    auto                                              state = state_.load(std::memory_order_acquire);
    std::vector<std::shared_ptr<const ChunkManifest>> candidates;
    for (;;) {
        if (state == nullptr) {
            return 0;
        }
        if (state->reclaiming) {
            return 0;
        }
        const auto     current = std::find_if(state->current.begin(), state->current.end(),
                                              [tree_id](const auto &entry) { return entry->tree_id == tree_id; });
        const uint64_t fallback_generation =
            current == state->current.end() || (*current)->generation == 0 ? 0 : (*current)->generation - 1;
        auto next = std::make_shared<CatalogState>(*state);
        next->history.clear();
        candidates.clear();
        for (const auto &manifest : state->history) {
            const bool eligible = manifest->tree_id == tree_id && manifest->generation < generation &&
                                  manifest->generation < fallback_generation && manifest.use_count() == 1;
            if (!eligible) {
                next->history.push_back(manifest);
                continue;
            }
            candidates.push_back(manifest);
        }
        if (candidates.empty()) {
            return 0;
        }
        next->reclaiming = true;
        if (state_.compare_exchange_weak(state, next, std::memory_order_release, std::memory_order_acquire)) {
            break;
        }
    }

    std::vector<std::shared_ptr<const ChunkManifest>> reclaimable;
    std::vector<std::shared_ptr<const ChunkManifest>> repinned;
    for (const auto &manifest : candidates) {
        // A reader that loaded the prior atomic CatalogState owns that entire
        // snapshot until it has copied its manifest pin. After the CAS, this
        // function is the prior state's sole owner unless a reader crossed
        // publication. Each candidate also has two local manifest owners: the
        // prior state and this vector.
        (state.use_count() > 1 || manifest.use_count() > 2 ? repinned : reclaimable).push_back(manifest);
    }
    if (!repinned.empty()) {
        auto current_state = state_.load(std::memory_order_acquire);
        for (;;) {
            auto next = std::make_shared<CatalogState>(*current_state);
            for (const auto &manifest : repinned) {
                const bool present = std::any_of(next->history.begin(), next->history.end(), [&](const auto &entry) {
                    return entry->tree_id == manifest->tree_id && entry->generation == manifest->generation;
                });
                if (!present) {
                    next->history.push_back(manifest);
                }
            }
            if (state_.compare_exchange_weak(current_state, next, std::memory_order_release,
                                             std::memory_order_acquire)) {
                break;
            }
        }
    }

    std::vector<ReferenceSegmentIdentity>              candidate_segments;
    std::unordered_set<PackIdentity, PackIdentityHash> candidate_packs;
    for (const auto &manifest : reclaimable) {
        for (const ChunkPagePack &pack : manifest->packs) {
            candidate_packs.insert(pack_identity(pack));
        }
        for (const ChunkReferenceSegment &segment : manifest->reference_segments) {
            candidate_segments.push_back(
                {.owner_tree_id = reference_segment_owner(*manifest, segment), .object_id = segment.object_id});
        }
    }
    uint64_t                                                                   reclaimed = 0;
    std::unordered_set<ReferenceSegmentIdentity, ReferenceSegmentIdentityHash> live_segments;
    std::unordered_set<PackIdentity, PackIdentityHash>                         live_packs;
    auto live_state = state_.load(std::memory_order_acquire);
    if (live_state != nullptr) {
        auto remember_live = [&](const auto &manifests) {
            for (const auto &manifest : manifests) {
                for (const ChunkPagePack &pack : manifest->packs) {
                    live_packs.insert(pack_identity(pack));
                }
                for (const ChunkReferenceSegment &segment : manifest->reference_segments) {
                    live_segments.insert(
                        {.owner_tree_id = reference_segment_owner(*manifest, segment), .object_id = segment.object_id});
                }
            }
        };
        remember_live(live_state->history);
        remember_live(live_state->current);
    }
    for (const PackIdentity &pack : candidate_packs) {
        if (!live_packs.contains(pack)) {
            reclaimed += static_cast<uint64_t>(pack.length) * 3;
        }
    }
    candidate_segments.erase(
        std::remove_if(candidate_segments.begin(), candidate_segments.end(),
                       [&live_segments](const auto &segment) { return live_segments.contains(segment); }),
        candidate_segments.end());
    while (!candidate_segments.empty()) {
        const uint64_t        owner_tree_id = candidate_segments.back().owner_tree_id;
        std::vector<uint64_t> object_ids;
        for (const ReferenceSegmentIdentity &segment : candidate_segments) {
            if (segment.owner_tree_id == owner_tree_id) {
                object_ids.push_back(segment.object_id);
            }
        }
        reclaimed += discard_reference_segments(owner_tree_id, object_ids);
        candidate_segments.erase(
            std::remove_if(candidate_segments.begin(), candidate_segments.end(),
                           [owner_tree_id](const auto &segment) { return segment.owner_tree_id == owner_tree_id; }),
            candidate_segments.end());
    }

    auto current_state = state_.load(std::memory_order_acquire);
    for (;;) {
        auto next        = std::make_shared<CatalogState>(*current_state);
        next->reclaiming = false;
        if (state_.compare_exchange_weak(current_state, next, std::memory_order_release, std::memory_order_acquire)) {
            break;
        }
    }
    return reclaimed;
}

uint64_t MemoryRootCatalog::retained_manifest_count(uint64_t tree_id) const
{
    auto state = state_.load(std::memory_order_acquire);
    if (state == nullptr) {
        return 0;
    }
    return std::count_if(state->history.begin(), state->history.end(),
                         [tree_id](const auto &manifest) { return manifest->tree_id == tree_id; });
}

uint64_t MemoryRootCatalog::pinned_bytes(uint64_t tree_id) const
{
    auto state = state_.load(std::memory_order_acquire);
    if (state == nullptr) {
        return 0;
    }
    auto     current = std::find_if(state->current.begin(), state->current.end(),
                                    [tree_id](const auto &entry) { return entry->tree_id == tree_id; });
    uint64_t bytes   = 0;
    for (const auto &manifest : state->history) {
        if ((current == state->current.end() || manifest != *current) && manifest->tree_id == tree_id &&
            manifest.use_count() > 1) {
            bytes += manifest->logical_size;
        }
    }
    return bytes;
}

uint64_t MemoryRootCatalog::oldest_pin_age_ms(uint64_t tree_id) const
{
    auto state = state_.load(std::memory_order_acquire);
    if (state == nullptr) {
        return 0;
    }
    auto     current = std::find_if(state->current.begin(), state->current.end(),
                                    [tree_id](const auto &entry) { return entry->tree_id == tree_id; });
    uint64_t oldest  = 0;
    for (const auto &manifest : state->history) {
        if ((current == state->current.end() || manifest != *current) && manifest->tree_id == tree_id &&
            manifest.use_count() > 1 && (oldest == 0 || manifest->published_at_ms < oldest)) {
            oldest = manifest->published_at_ms;
        }
    }
    return oldest == 0 ? 0 : monotonic_millis() - oldest;
}

ChunkPageStore::ChunkPageStore(Config config, std::shared_ptr<RootCatalog> catalog,
                               std::shared_ptr<ChunkTransport> transport)
    : config_(config),
      catalog_(std::move(catalog)),
      transport_(std::move(transport))
{
    if (config_.pack_bytes == 0) {
        config_.pack_bytes = 4U * 1024U * 1024U;
    }
    if (config_.iu_size == 0) {
        config_.iu_size = 64U * 1024U;
    }
    if (config_.max_chunk_bytes == 0) {
        config_.max_chunk_bytes = 256U * 1024U * 1024U;
    }
    if (config_.page_alignment == 0) {
        config_.page_alignment = 64U * 1024U;
    }
    if (config_.max_pending_ops == 0) {
        config_.max_pending_ops = 256;
    }
    if (config_.max_concurrent_packs == 0) {
        config_.max_concurrent_packs = 8;
    }
    if (transport_ == nullptr) {
        transport_ = std::make_shared<MemoryChunkTransport>();
    }
    config_.max_chunk_bytes = std::min(config_.max_chunk_bytes, kMaxChunkBytes);
    config_.pack_bytes      = std::min<uint64_t>(config_.pack_bytes, config_.max_chunk_bytes);
    async_executor_         = std::make_unique<ChunkAsyncExecutor>(this, config_.max_pending_ops);
}

ChunkPageStore::~ChunkPageStore() = default;

Status ChunkPageStore::inherit_snapshot_from(const ChunkPageStore &source)
{
    if (config_.tree_id == source.config_.tree_id) {
        return Status::invalid_argument("chunk snapshot inheritance requires a distinct destination tree");
    }
    if (staged_initialized_ || catalog_->load(config_.tree_id) != nullptr) {
        return Status::invalid_argument("chunk snapshot inheritance requires an unpublished destination");
    }
    if (transport_.get() != source.transport_.get() || catalog_.get() != source.catalog_.get()) {
        return Status::Ok();
    }
    auto manifest = source.catalog_->load(source.config_.tree_id);
    if (manifest == nullptr) {
        inherited_manifest_.reset();
        inherited_catalog_.reset();
        return Status::Ok();
    }
    Status manifest_status = source.validate_manifest(*manifest, *source.catalog_);
    if (!manifest_status.ok()) {
        return manifest_status;
    }
    inherited_manifest_ = std::move(manifest);
    inherited_catalog_  = source.catalog_;
    return Status::Ok();
}

std::shared_ptr<const ChunkManifest> ChunkPageStore::reuse_base_manifest() const
{
    auto current = catalog_->load(config_.tree_id);
    return current == nullptr ? inherited_manifest_ : current;
}

const ChunkPagePack *ChunkPageStore::find_reusable_pack(const ChunkManifest &base, uint64_t logical_offset,
                                                        uint32_t length, uint32_t checksum,
                                                        ChunkCancellation cancellation) const
{
    auto found =
        std::lower_bound(base.packs.begin(), base.packs.end(), logical_offset,
                         [](const ChunkPagePack &pack, uint64_t offset) { return pack.logical_offset < offset; });
    if (found == base.packs.end() || found->logical_offset != logical_offset || found->ref.length != length ||
        found->ref.checksum != checksum) {
        return nullptr;
    }
    std::shared_ptr<const std::vector<uint8_t>> persisted;
    Status                                      status = read_pack(found->ref, &persisted, cancellation);
    if (!status.ok() || persisted->size() != length ||
        std::memcmp(persisted->data(), staged_.data() + logical_offset, length) != 0) {
        return nullptr;
    }
    return &*found;
}

Status ChunkPageStore::validate_manifest(const ChunkManifest &manifest, const RootCatalog &catalog) const
{
    if (manifest.format_version > kChunkManifestFormat || manifest.checksum != manifest_checksum(manifest)) {
        return Status::corruption("chunk manifest checksum mismatch");
    }
    const size_t expected_segments = (manifest.packs.size() + kReferencesPerSegment - 1) / kReferencesPerSegment;
    if (manifest.reference_segments.size() != expected_segments) {
        return Status::corruption("chunk reference segment directory is incomplete");
    }
    uint64_t logical_offset    = 0;
    uint64_t reused_packs      = 0;
    uint64_t reused_pack_bytes = 0;
    for (size_t index = 0; index < manifest.reference_segments.size(); ++index) {
        const uint64_t first = index * kReferencesPerSegment;
        const uint32_t count = static_cast<uint32_t>(
            std::min<size_t>(kReferencesPerSegment, manifest.packs.size() - static_cast<size_t>(first)));
        const ChunkReferenceSegment &segment = manifest.reference_segments[index];
        if (segment.first_ordinal != first || segment.ref_count != count) {
            return Status::corruption("chunk reference segment directory coverage is invalid");
        }
        auto image = catalog.load_reference_segment(reference_segment_owner(manifest, segment), segment.object_id);
        if (image == nullptr || image->object_id != segment.object_id || image->first_ordinal != first ||
            image->refs.size() != count || reference_segment_checksum(*image) != segment.checksum) {
            return Status::corruption("chunk reference segment image is missing or corrupt");
        }
        for (size_t offset = 0; offset < count; ++offset) {
            const size_t         ordinal = static_cast<size_t>(first) + offset;
            const ChunkPagePack &pack    = manifest.packs[ordinal];
            const ChunkPageRef  &ref     = image->refs[offset];
            if (pack.ordinal != ordinal || pack.logical_offset != logical_offset || pack.ref.chunk_id.empty() ||
                pack.ref.length == 0 || logical_offset > manifest.logical_size ||
                pack.ref.length > manifest.logical_size - logical_offset || ref.chunk_id != pack.ref.chunk_id ||
                ref.offset != pack.ref.offset || ref.length != pack.ref.length || ref.checksum != pack.ref.checksum) {
                return Status::corruption("chunk manifest page-pack coverage or reference is invalid");
            }
            logical_offset += pack.ref.length;
            if (pack.reused) {
                ++reused_packs;
                reused_pack_bytes += pack.ref.length;
            }
        }
    }
    if (logical_offset != manifest.logical_size || reused_packs != manifest.packs_reused ||
        reused_pack_bytes != manifest.pack_bytes_reused) {
        return Status::corruption("chunk manifest coverage or reuse counters are inconsistent");
    }
    return Status::Ok();
}

Status ChunkPageStore::materialize_active(std::vector<uint8_t> *out) const
{
    auto manifest = reuse_base_manifest();
    if (manifest == nullptr) {
        out->clear();
        return Status::Ok();
    }
    if (manifest->owner_epoch > config_.owner_epoch) {
        return Status::corruption("chunk manifest checksum or epoch is invalid");
    }
    const RootCatalog &manifest_catalog =
        manifest == inherited_manifest_ && inherited_catalog_ != nullptr ? *inherited_catalog_ : *catalog_;
    Status manifest_status = validate_manifest(*manifest, manifest_catalog);
    if (!manifest_status.ok()) {
        return manifest_status;
    }
    out->assign(manifest->logical_size, 0);
    for (const ChunkPagePack &pack : manifest->packs) {
        const ChunkPageRef &ref = pack.ref;
        if (pack.logical_offset > out->size() || ref.length > out->size() - pack.logical_offset) {
            return Status::corruption("chunk page pack checksum or bounds are invalid");
        }
        std::vector<uint8_t> valid_mirror(ref.length);
        bool                 found            = false;
        bool                 mirror_responded = false;
        for (uint32_t mirror = 0; mirror < 3; ++mirror) {
            Status read_status =
                transport_->read_mirror(ref.chunk_id, mirror, ref.offset, valid_mirror.data(), valid_mirror.size());
            if (!read_status.ok()) {
                continue;
            }
            mirror_responded = true;
            if (ref.checksum == crowdb::common::crc32c(valid_mirror.data(), valid_mirror.size())) {
                found = true;
                break;
            }
        }
        if (!found) {
            return mirror_responded ? Status::corruption("chunk page pack has no valid mirror")
                                    : Status::unavailable("chunk page pack mirrors are unavailable");
        }
        std::memcpy(out->data() + pack.logical_offset, valid_mirror.data(), valid_mirror.size());
    }
    return Status::Ok();
}

Status ChunkPageStore::write_at(uint64_t off, const uint8_t *buf, size_t len)
{
    if (buf == nullptr && len != 0) {
        return Status::invalid_argument("chunk page write has null buffer");
    }
    if (off > std::numeric_limits<size_t>::max() || len > std::numeric_limits<size_t>::max() - off) {
        return Status::resource_exhausted("chunk page write exceeds address space");
    }
    if (!staged_initialized_) {
        Status status = materialize_active(&staged_);
        if (!status.ok()) {
            return status;
        }
        staged_initialized_ = true;
    }
    const size_t end = static_cast<size_t>(off) + len;
    if (end > staged_.size()) {
        staged_.resize(end, 0);
    }
    if (len != 0) {
        std::memcpy(staged_.data() + off, buf, len);
    }
    if (end > kAnchorRegionBytes) {
        data_durable_ = false;
    }
    if (off < kAnchorRegionBytes) {
        anchor_dirty_ = true;
    }
    return Status::Ok();
}

Status ChunkPageStore::read_at(uint64_t off, uint8_t *buf, size_t len) const
{
    return read_at_cancellable(off, buf, len, {});
}

Status ChunkPageStore::read_pack(const ChunkPageRef &ref, std::shared_ptr<const std::vector<uint8_t>> *out,
                                 ChunkCancellation cancellation) const
{
    if (cancellation.cancelled()) {
        return Status::unavailable("chunk page read cancelled");
    }
    const bool use_cache = cancellation.cancelled_id != nullptr;
    auto       cached    = use_cache ? cached_pack_ : nullptr;
    if (cached != nullptr && cached->ref.chunk_id == ref.chunk_id && cached->ref.offset == ref.offset &&
        cached->ref.length == ref.length && cached->ref.checksum == ref.checksum) {
        if (cancellation.cancelled()) {
            return Status::unavailable("chunk page read cancelled");
        }
        *out = cached->bytes;
        return Status::Ok();
    }
    pack_reads_.fetch_add(1, std::memory_order_relaxed);
    auto valid_mirror     = std::make_shared<std::vector<uint8_t>>(ref.length);
    bool mirror_responded = false;
    for (uint32_t mirror = 0; mirror < 3; ++mirror) {
        if (cancellation.cancelled()) {
            return Status::unavailable("chunk page read cancelled");
        }
        Status read_status =
            transport_->read_mirror(ref.chunk_id, mirror, ref.offset, valid_mirror->data(), valid_mirror->size());
        if (!read_status.ok()) {
            continue;
        }
        mirror_responded = true;
        if (ref.checksum == crowdb::common::crc32c(valid_mirror->data(), valid_mirror->size())) {
            auto entry = std::make_shared<CachedPack>(CachedPack{.ref = ref, .bytes = valid_mirror});
            if (use_cache) {
                cached_pack_ = std::move(entry);
            }
            *out = std::move(valid_mirror);
            return cancellation.cancelled() ? Status::unavailable("chunk page read cancelled") : Status::Ok();
        }
    }
    return mirror_responded ? Status::corruption("chunk page pack checksum mismatch on every mirror")
                            : Status::unavailable("chunk page pack mirrors are unavailable");
}

Status ChunkPageStore::read_at_cancellable(uint64_t off, uint8_t *buf, size_t len, ChunkCancellation cancellation) const
{
    if (unavailable_.load(std::memory_order_acquire)) {
        return Status::unavailable("chunk mirrors unavailable after bounded retries");
    }
    if (buf == nullptr && len != 0) {
        return Status::invalid_argument("chunk page read has null buffer");
    }
    std::shared_ptr<const ChunkManifest> manifest;
    Status                               layout_status = load_layout(&manifest);
    if (!layout_status.ok()) {
        return layout_status;
    }
    if (manifest == nullptr || off > manifest->logical_size || len > manifest->logical_size - off) {
        return Status::unavailable("chunk page range is not present in the published manifest");
    }
    size_t copied = 0;
    for (const ChunkPagePack &pack : manifest->packs) {
        const ChunkPageRef &ref      = pack.ref;
        const uint64_t      pack_end = pack.logical_offset + ref.length;
        const uint64_t      read_end = off + len;
        if (pack_end <= off || pack.logical_offset >= read_end) {
            continue;
        }
        std::shared_ptr<const std::vector<uint8_t>> valid_mirror;
        Status                                      read_status = read_pack(ref, &valid_mirror, cancellation);
        if (!read_status.ok()) {
            return read_status;
        }
        const uint64_t begin = std::max(off, pack.logical_offset);
        const uint64_t end   = std::min(read_end, pack_end);
        std::memcpy(buf + (begin - off), valid_mirror->data() + (begin - pack.logical_offset), end - begin);
        copied += end - begin;
    }
    if (cancellation.cancelled()) {
        return Status::unavailable("chunk page read cancelled");
    }
    if (copied != len) {
        return Status::corruption("chunk manifest has a page-pack coverage gap");
    }
    return Status::Ok();
}

Status ChunkPageStore::load_layout(std::shared_ptr<const ChunkManifest> *out) const
{
    const uint64_t now    = monotonic_millis();
    auto           cached = cached_layout_.load(std::memory_order_acquire);
    if (cached != nullptr && now < layout_valid_until_ms_.load(std::memory_order_acquire)) {
        cache_hits_.fetch_add(1, std::memory_order_relaxed);
        *out = std::move(cached);
        return Status::Ok();
    }
    auto manifest = catalog_->load(config_.tree_id);
    layout_queries_.fetch_add(1, std::memory_order_relaxed);
    if (manifest != nullptr) {
        Status status = validate_manifest(*manifest, *catalog_);
        if (!status.ok()) {
            return status;
        }
    }
    cached_layout_.store(manifest, std::memory_order_release);
    layout_valid_until_ms_.store(now + config_.layout_validity_ms, std::memory_order_release);
    *out = std::move(manifest);
    return Status::Ok();
}

uint32_t ChunkPageStore::reference_segment_checksum(const ChunkReferenceSegmentImage &segment)
{
    uint32_t crc = update_u64(0, segment.object_id);
    crc          = update_u64(crc, segment.first_ordinal);
    for (const ChunkPageRef &ref : segment.refs) {
        crc = update_u64(crc, ref.chunk_id.high);
        crc = update_u64(crc, ref.chunk_id.low);
        crc = update_u64(crc, ref.offset);
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&ref.length), sizeof(ref.length));
        crc =
            crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&ref.checksum), sizeof(ref.checksum));
    }
    return crc;
}

uint32_t ChunkPageStore::manifest_checksum(const ChunkManifest &manifest)
{
    uint32_t crc = 0;
    if (manifest.format_version != 0) {
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&manifest.format_version),
                                            sizeof(manifest.format_version));
    }
    crc = update_u64(crc, manifest.tree_id);
    crc = update_u64(crc, manifest.generation);
    crc = update_u64(crc, manifest.owner_epoch);
    crc = update_u64(crc, manifest.logical_size);
    crc = update_u64(crc, manifest.published_at_ms);
    crc = update_u64(crc, manifest.packs_reused);
    crc = update_u64(crc, manifest.pack_bytes_reused);
    for (const ChunkPagePack &pack : manifest.packs) {
        crc = update_u64(crc, pack.ordinal);
        crc = update_u64(crc, pack.logical_offset);
        crc = update_u64(crc, pack.ref.chunk_id.high);
        crc = update_u64(crc, pack.ref.chunk_id.low);
        crc = update_u64(crc, pack.ref.offset);
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&pack.ref.length),
                                            sizeof(pack.ref.length));
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&pack.ref.checksum),
                                            sizeof(pack.ref.checksum));
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&pack.reused), sizeof(pack.reused));
    }
    for (const ChunkReferenceSegment &segment : manifest.reference_segments) {
        if (manifest.format_version != 0) {
            crc = update_u64(crc, reference_segment_owner(manifest, segment));
        }
        crc = update_u64(crc, segment.object_id);
        crc = update_u64(crc, segment.first_ordinal);
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&segment.ref_count),
                                            sizeof(segment.ref_count));
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&segment.checksum),
                                            sizeof(segment.checksum));
        if (manifest.format_version != 0) {
            crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&segment.reused),
                                                sizeof(segment.reused));
        }
    }
    return crc;
}

Status ChunkPageStore::persist_reference_segments(ChunkManifest *manifest, const ChunkManifest *reuse_base)
{
    for (size_t first = 0; first < manifest->packs.size(); first += kReferencesPerSegment) {
        const size_t end = std::min(first + kReferencesPerSegment, manifest->packs.size());
        if (reuse_base != nullptr && first / kReferencesPerSegment < reuse_base->reference_segments.size()) {
            const ChunkReferenceSegment &candidate = reuse_base->reference_segments[first / kReferencesPerSegment];
            const bool                   same =
                candidate.first_ordinal == first && candidate.ref_count == end - first &&
                end <= reuse_base->packs.size() &&
                std::equal(manifest->packs.begin() + first, manifest->packs.begin() + end,
                           reuse_base->packs.begin() + first, [](const auto &left, const auto &right) {
                               return left.ref.chunk_id == right.ref.chunk_id && left.ref.offset == right.ref.offset &&
                                      left.ref.length == right.ref.length && left.ref.checksum == right.ref.checksum;
                           });
            if (same) {
                ChunkReferenceSegment shared = candidate;
                shared.owner_tree_id         = reference_segment_owner(*reuse_base, candidate);
                shared.reused                = true;
                manifest->reference_segments.push_back(shared);
                continue;
            }
        }

        auto segment       = std::make_shared<ChunkReferenceSegmentImage>();
        segment->object_id = catalog_->allocate_reference_segment_id(config_.tree_id);
        if (segment->object_id == 0) {
            catalog_->discard_reference_segments(config_.tree_id, reference_segment_ids(*manifest));
            return Status::resource_exhausted("chunk reference segment identity is exhausted");
        }
        segment->first_ordinal = first;
        for (size_t index = first; index < end; ++index) {
            segment->refs.push_back(manifest->packs[index].ref);
        }
        ChunkReferenceSegment descriptor{
            .owner_tree_id = config_.tree_id,
            .object_id     = segment->object_id,
            .first_ordinal = segment->first_ordinal,
            .ref_count     = static_cast<uint32_t>(segment->refs.size()),
            .checksum      = reference_segment_checksum(*segment),
            .reused        = false,
        };
        Status persist_status = catalog_->persist_reference_segment(config_.tree_id, std::move(segment));
        if (!persist_status.ok()) {
            catalog_->discard_reference_segments(config_.tree_id, reference_segment_ids(*manifest));
            return persist_status;
        }
        manifest->reference_segments.push_back(descriptor);
    }
    return Status::Ok();
}

Status ChunkPageStore::build_manifest(uint64_t expected_generation, std::shared_ptr<ChunkManifest> *out,
                                      uint64_t *new_pack_bytes, ChunkCancellation cancellation)
{
    *new_pack_bytes = 0;
    if (expected_generation == std::numeric_limits<uint64_t>::max()) {
        return Status::resource_exhausted("chunk manifest generation is exhausted");
    }
    if (!orphan_reference_segments_.empty()) {
        catalog_->discard_reference_segments(config_.tree_id, orphan_reference_segments_);
        orphan_reference_segments_.clear();
    }
    auto manifest             = std::make_shared<ChunkManifest>();
    manifest->format_version  = kChunkManifestFormat;
    manifest->tree_id         = config_.tree_id;
    manifest->generation      = expected_generation + 1;
    manifest->owner_epoch     = config_.owner_epoch;
    manifest->logical_size    = staged_.size();
    manifest->published_at_ms = monotonic_millis();
    auto reuse_base           = reuse_base_manifest();
    if (reuse_base != nullptr) {
        const RootCatalog &reuse_catalog =
            reuse_base == inherited_manifest_ && inherited_catalog_ != nullptr ? *inherited_catalog_ : *catalog_;
        Status status = validate_manifest(*reuse_base, reuse_catalog);
        if (!status.ok()) {
            return status;
        }
    }
    uint64_t offset = 0;
    while (offset < staged_.size()) {
        if (cancellation.cancelled()) {
            return Status::unavailable("chunk manifest build cancelled");
        }
        const size_t   length   = std::min(config_.pack_bytes, staged_.size() - static_cast<size_t>(offset));
        const uint32_t checksum = crowdb::common::crc32c(staged_.data() + offset, length);
        if (reuse_base != nullptr) {
            const ChunkPagePack *reused =
                find_reusable_pack(*reuse_base, offset, static_cast<uint32_t>(length), checksum);
            if (reused != nullptr) {
                manifest->packs.push_back(
                    {.ordinal = manifest->packs.size(), .logical_offset = offset, .ref = reused->ref, .reused = true});
                ++manifest->packs_reused;
                manifest->pack_bytes_reused += length;
                offset += length;
                continue;
            }
        }
        if (!active_chunk_id_.empty() && length > config_.max_chunk_bytes - active_chunk_bytes_) {
            Status seal_status = transport_->seal_chunk(active_chunk_id_, config_.owner_epoch, active_chunk_cursor_);
            if (!seal_status.ok()) {
                return seal_status;
            }
            active_chunk_id_     = {};
            active_chunk_bytes_  = 0;
            active_chunk_cursor_ = 0;
        }
        if (active_chunk_id_.empty()) {
            const uint64_t packs_per_chunk = (config_.max_chunk_bytes + config_.pack_bytes - 1) / config_.pack_bytes;
            const uint64_t physical_pack_bytes = round_up_to_iu(config_.pack_bytes, config_.page_alignment);
            if (packs_per_chunk > std::numeric_limits<uint64_t>::max() / physical_pack_bytes) {
                return Status::resource_exhausted("chunk page framing exceeds address space");
            }
            Status allocate_status = transport_->allocate_mirror_chunk(packs_per_chunk * physical_pack_bytes,
                                                                       config_.owner_epoch, &active_chunk_id_);
            if (!allocate_status.ok()) {
                return allocate_status;
            }
        }

        ChunkPagePack pack;
        pack.ordinal                         = manifest->packs.size();
        pack.logical_offset                  = offset;
        pack.ref.chunk_id                    = active_chunk_id_;
        pack.ref.offset                      = active_chunk_cursor_;
        pack.ref.length                      = static_cast<uint32_t>(length);
        pack.ref.checksum                    = checksum;
        const size_t         physical_length = round_up_to_iu(length, config_.page_alignment);
        std::vector<uint8_t> framed(physical_length, 0);
        std::memcpy(framed.data(), staged_.data() + offset, length);
        *new_pack_bytes += length;
        for (uint32_t mirror = 0; mirror < 3; ++mirror) {
            bool written = false;
            for (uint32_t attempt = 0; attempt <= config_.mirror_retry_limit; ++attempt) {
                if (cancellation.cancelled()) {
                    return Status::unavailable("chunk manifest build cancelled");
                }
                mirror_write_attempts_.fetch_add(1, std::memory_order_relaxed);
                if ((mirror_write_failure_mask_.load(std::memory_order_acquire) & (1U << mirror)) == 0) {
                    Status write_status = transport_->write_mirror(active_chunk_id_, mirror, active_chunk_cursor_,
                                                                   framed.data(), framed.size());
                    if (write_status.ok()) {
                        written = true;
                        break;
                    }
                }
                mirror_write_failures_.fetch_add(1, std::memory_order_relaxed);
            }
            if (!written) {
                return Status::unavailable("chunk mirror write unavailable after bounded retries");
            }
        }
        if (cancellation.cancelled()) {
            return Status::unavailable("chunk manifest build cancelled");
        }
        Status advance_status =
            transport_->advance_write(active_chunk_id_, active_chunk_cursor_, active_chunk_cursor_ + physical_length);
        if (!advance_status.ok()) {
            return advance_status;
        }
        active_chunk_bytes_ += length;
        active_chunk_cursor_ += physical_length;
        manifest->packs.push_back(std::move(pack));
        offset += length;
    }
    Status segment_status = persist_reference_segments(manifest.get(), reuse_base.get());
    if (!segment_status.ok()) {
        return segment_status;
    }
    manifest->checksum = manifest_checksum(*manifest);
    *out               = std::move(manifest);
    return Status::Ok();
}

std::vector<uint64_t> ChunkPageStore::reference_segment_ids(const ChunkManifest &manifest)
{
    std::vector<uint64_t> ids;
    ids.reserve(manifest.reference_segments.size());
    for (const ChunkReferenceSegment &segment : manifest.reference_segments) {
        if (!segment.reused) {
            ids.push_back(segment.object_id);
        }
    }
    return ids;
}

Status ChunkPageStore::sync()
{
    return sync_cancellable({});
}

Status ChunkPageStore::sync_cancellable(ChunkCancellation cancellation)
{
    if (!staged_initialized_) {
        return Status::Ok();
    }
    if (!anchor_dirty_) {
        data_durable_ = true;
        return Status::Ok();
    }
    if (!data_durable_) {
        return Status::internal_error("chunk root cannot publish before data durability barrier");
    }
    auto                           prior               = catalog_->load(config_.tree_id);
    const uint64_t                 expected_generation = prior == nullptr ? 0 : prior->generation;
    std::shared_ptr<ChunkManifest> manifest;
    uint64_t                       new_pack_bytes = 0;
    Status build_status = build_manifest(expected_generation, &manifest, &new_pack_bytes, cancellation);
    if (!build_status.ok()) {
        orphan_bytes_.fetch_add(new_pack_bytes, std::memory_order_relaxed);
        return build_status;
    }
    if (cancellation.cancelled()) {
        orphan_bytes_.fetch_add(manifest->logical_size - manifest->pack_bytes_reused, std::memory_order_relaxed);
        auto ids = reference_segment_ids(*manifest);
        orphan_reference_segments_.insert(orphan_reference_segments_.end(), ids.begin(), ids.end());
        return Status::unavailable("chunk root publication cancelled");
    }
    Status validation_status = validate_manifest(*manifest, *catalog_);
    if (!validation_status.ok()) {
        orphan_bytes_.fetch_add(manifest->logical_size - manifest->pack_bytes_reused, std::memory_order_relaxed);
        auto ids = reference_segment_ids(*manifest);
        orphan_reference_segments_.insert(orphan_reference_segments_.end(), ids.begin(), ids.end());
        return validation_status;
    }
    Status status = catalog_->publish(config_.tree_id, expected_generation, config_.owner_epoch, manifest);
    if (!status.ok()) {
        orphan_bytes_.fetch_add(manifest->logical_size - manifest->pack_bytes_reused, std::memory_order_relaxed);
        auto ids = reference_segment_ids(*manifest);
        orphan_reference_segments_.insert(orphan_reference_segments_.end(), ids.begin(), ids.end());
        return status;
    }
    generations_published_.fetch_add(1, std::memory_order_relaxed);
    packs_written_.fetch_add(manifest->packs.size() - manifest->packs_reused, std::memory_order_relaxed);
    pack_bytes_written_.fetch_add(manifest->logical_size - manifest->pack_bytes_reused, std::memory_order_relaxed);
    packs_reused_.fetch_add(manifest->packs_reused, std::memory_order_relaxed);
    pack_bytes_reused_.fetch_add(manifest->pack_bytes_reused, std::memory_order_relaxed);
    cached_layout_.store(manifest, std::memory_order_release);
    layout_valid_until_ms_.store(monotonic_millis() + config_.layout_validity_ms, std::memory_order_release);
    staged_initialized_ = false;
    staged_.clear();
    inherited_manifest_.reset();
    inherited_catalog_.reset();
    data_durable_ = false;
    anchor_dirty_ = false;
    return Status::Ok();
}

std::shared_ptr<void> ChunkPageStore::start_sync_cancellable(ChunkCancellation cancellation, AsyncCompletion completion)
{
    if (!staged_initialized_) {
        completion.complete(Status::Ok());
        return {};
    }
    if (!anchor_dirty_) {
        data_durable_ = true;
        completion.complete(Status::Ok());
        return {};
    }
    if (!data_durable_) {
        completion.complete(Status::internal_error("chunk root cannot publish before data durability barrier"));
        return {};
    }
    if (!orphan_reference_segments_.empty()) {
        catalog_->discard_reference_segments(config_.tree_id, orphan_reference_segments_);
        orphan_reference_segments_.clear();
    }
    auto           prior               = catalog_->load(config_.tree_id);
    const uint64_t expected_generation = prior == nullptr ? 0 : prior->generation;
    Status         start_status;
    auto           state = ChunkPackPipeline::start(this, expected_generation, cancellation, completion, &start_status);
    if (!start_status.ok()) {
        completion.complete(std::move(start_status));
    }
    return state;
}

Status ChunkPageStore::finish_sync_cancellable(const std::shared_ptr<void> &state, ChunkCancellation cancellation,
                                               Status io_status)
{
    if (state == nullptr) {
        return io_status;
    }
    return std::static_pointer_cast<ChunkPackPipeline>(state)->finish(cancellation, std::move(io_status));
}

uint64_t ChunkPageStore::size() const
{
    if (staged_initialized_) {
        return staged_.size();
    }
    auto manifest = catalog_->load(config_.tree_id);
    return manifest == nullptr ? 0 : manifest->logical_size;
}

uint64_t ChunkPageStore::submit_read(PageAddr addr, void *buf, size_t len, AsyncCompletion on_complete)
{
    const uint64_t operation_id = async_executor_->submit({.kind       = ChunkAsyncExecutor::Kind::kRead,
                                                           .addr       = addr,
                                                           .buffer     = buf,
                                                           .length     = len,
                                                           .completion = on_complete});
    if (operation_id == 0) {
        on_complete.complete(Status::resource_exhausted("chunk async operation queue is full"));
    }
    return operation_id;
}

uint64_t ChunkPageStore::submit_write(PageAddr addr, const void *buf, size_t len, AsyncCompletion on_complete)
{
    const uint64_t operation_id = async_executor_->submit({.kind         = ChunkAsyncExecutor::Kind::kWrite,
                                                           .addr         = addr,
                                                           .const_buffer = buf,
                                                           .length       = len,
                                                           .completion   = on_complete});
    if (operation_id == 0) {
        on_complete.complete(Status::resource_exhausted("chunk async operation queue is full"));
    }
    return operation_id;
}

Status ChunkPageStore::submit_fsync(AsyncCompletion on_complete)
{
    const uint64_t operation_id =
        async_executor_->submit({.kind = ChunkAsyncExecutor::Kind::kFsync, .completion = on_complete});
    if (operation_id == 0) {
        on_complete.complete(Status::resource_exhausted("chunk async operation queue is full"));
    }
    return Status::Ok();
}

void ChunkPageStore::cancel(uint64_t operation_id)
{
    async_executor_->cancel(operation_id);
}

ChunkPageStoreStats ChunkPageStore::stats() const
{
    return {
        .generations_published = generations_published_.load(std::memory_order_relaxed),
        .packs_written         = packs_written_.load(std::memory_order_relaxed),
        .pack_bytes_written    = pack_bytes_written_.load(std::memory_order_relaxed),
        .packs_reused          = packs_reused_.load(std::memory_order_relaxed),
        .pack_bytes_reused     = pack_bytes_reused_.load(std::memory_order_relaxed),
        .pack_reads            = pack_reads_.load(std::memory_order_relaxed),
        .cache_hits            = cache_hits_.load(std::memory_order_relaxed),
        .layout_queries        = layout_queries_.load(std::memory_order_relaxed),
        .mirror_write_attempts = mirror_write_attempts_.load(std::memory_order_relaxed),
        .mirror_write_failures = mirror_write_failures_.load(std::memory_order_relaxed),
        .retained_manifests    = catalog_->retained_manifest_count(config_.tree_id),
        .pinned_bytes          = catalog_->pinned_bytes(config_.tree_id),
        .oldest_pin_age_ms     = catalog_->oldest_pin_age_ms(config_.tree_id),
        .orphan_bytes          = orphan_bytes_.load(std::memory_order_relaxed),
    };
}

uint64_t ChunkPageStore::reclaim_orphans()
{
    catalog_->discard_reference_segments(config_.tree_id, orphan_reference_segments_);
    orphan_reference_segments_.clear();
    return orphan_bytes_.exchange(0, std::memory_order_acq_rel);
}

} // namespace crowdb::tree::detail

ct_status ct_memory_root_catalog_open(uint64_t owner_epoch, ct_root_catalog **out)
{
    if (out == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    auto handle       = std::make_unique<ct_root_catalog>();
    handle->catalog   = std::make_shared<crowdb::tree::detail::MemoryRootCatalog>(owner_epoch);
    handle->transport = std::make_shared<crowdb::tree::detail::MemoryChunkTransport>();
    *out              = handle.release();
    return static_cast<ct_status>(crowdb::tree::Code::kOk);
}

void ct_root_catalog_free(ct_root_catalog *catalog)
{
    delete catalog;
}

ct_status ct_chunk_page_store_open(const ct_chunk_page_store_options *options, ct_root_catalog *catalog,
                                   ct_page_store **out)
{
    if (options == nullptr || catalog == nullptr || catalog->catalog == nullptr || out == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    auto handle    = std::make_unique<ct_page_store>();
    handle->bundle = std::make_shared<PageStoreBundle>();
    auto store     = std::make_unique<crowdb::tree::detail::ChunkPageStore>(
        crowdb::tree::detail::ChunkPageStore::Config{.tree_id              = options->tree_id,
                                                     .owner_epoch          = options->owner_epoch,
                                                     .pack_bytes           = options->pack_bytes,
                                                     .iu_size              = options->iu_size,
                                                     .max_concurrent_packs = options->max_concurrent_packs},
        catalog->catalog, catalog->transport);
    handle->bundle->async_store_view = store.get();
    handle->bundle->store            = std::move(store);
    handle->bundle->backend_label    = "chunk";
    *out                             = handle.release();
    return static_cast<ct_status>(crowdb::tree::Code::kOk);
}

ct_status ct_chunk_page_store_open_with_transport(const ct_chunk_page_store_options *options, ct_root_catalog *catalog,
                                                  ct_chunk_transport *transport, ct_page_store **out)
{
    if (options == nullptr || catalog == nullptr || catalog->catalog == nullptr || transport == nullptr ||
        transport->transport == nullptr || out == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    auto handle    = std::make_unique<ct_page_store>();
    handle->bundle = std::make_shared<PageStoreBundle>();
    auto store     = std::make_unique<crowdb::tree::detail::ChunkPageStore>(
        crowdb::tree::detail::ChunkPageStore::Config{.tree_id              = options->tree_id,
                                                     .owner_epoch          = options->owner_epoch,
                                                     .pack_bytes           = options->pack_bytes,
                                                     .iu_size              = options->iu_size,
                                                     .max_concurrent_packs = options->max_concurrent_packs},
        catalog->catalog, transport->transport);
    handle->bundle->async_store_view = store.get();
    handle->bundle->store            = std::move(store);
    handle->bundle->backend_label    = "chunk";
    *out                             = handle.release();
    return static_cast<ct_status>(crowdb::tree::Code::kOk);
}

ct_status ct_chunk_page_store_get_stats(const ct_page_store *store, ct_chunk_page_store_stats *out)
{
    if (store == nullptr || store->bundle == nullptr || out == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    auto *chunk = dynamic_cast<crowdb::tree::detail::ChunkPageStore *>(store->bundle->store.get());
    if (chunk == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    const auto stats = chunk->stats();
    *out             = {.generations_published = stats.generations_published,
                        .packs_written         = stats.packs_written,
                        .pack_bytes_written    = stats.pack_bytes_written,
                        .packs_reused          = stats.packs_reused,
                        .pack_bytes_reused     = stats.pack_bytes_reused,
                        .pack_reads            = stats.pack_reads,
                        .cache_hits            = stats.cache_hits,
                        .layout_queries        = stats.layout_queries,
                        .mirror_write_attempts = stats.mirror_write_attempts,
                        .mirror_write_failures = stats.mirror_write_failures,
                        .retained_manifests    = stats.retained_manifests,
                        .pinned_bytes          = stats.pinned_bytes,
                        .oldest_pin_age_ms     = stats.oldest_pin_age_ms,
                        .orphan_bytes          = stats.orphan_bytes};
    return static_cast<ct_status>(crowdb::tree::Code::kOk);
}

uint64_t ct_chunk_page_store_reclaim_orphans(ct_page_store *store)
{
    if (store == nullptr || store->bundle == nullptr) {
        return 0;
    }
    auto *chunk = dynamic_cast<crowdb::tree::detail::ChunkPageStore *>(store->bundle->store.get());
    return chunk == nullptr ? 0 : chunk->reclaim_orphans();
}

uint64_t ct_root_catalog_reclaim_before(ct_root_catalog *catalog, uint64_t tree_id, uint64_t generation)
{
    if (catalog == nullptr || catalog->catalog == nullptr) {
        return 0;
    }
    return catalog->catalog->reclaim_before(tree_id, generation);
}
