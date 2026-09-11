// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Chunk-backend implementation.

#include "chunk_page_store.h"

#include "c_api_internal.h"
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
    return sizeof(segment.object_id) + sizeof(segment.first_ordinal) +
           segment.refs.size() * (sizeof(uint64_t) * 2 + sizeof(uint32_t) * 2);
}

} // namespace

std::shared_ptr<const ChunkManifest> MemoryRootCatalog::load(uint64_t tree_id) const
{
    auto current = current_.load(std::memory_order_acquire);
    if (current != nullptr && current->tree_id == tree_id) {
        return current;
    }
    return nullptr;
}

Status MemoryRootCatalog::publish(uint64_t tree_id, uint64_t expected_generation, uint64_t owner_epoch,
                                  std::shared_ptr<const ChunkManifest> manifest)
{
    if (manifest == nullptr || manifest->tree_id != tree_id || manifest->owner_epoch != owner_epoch) {
        return Status::invalid_argument("chunk manifest identity mismatch");
    }
    if (owner_epoch_.load(std::memory_order_acquire) != owner_epoch) {
        return Status::unavailable("chunk root publication fenced by owner epoch");
    }
    auto           expected           = current_.load(std::memory_order_acquire);
    const uint64_t current_generation = expected == nullptr ? 0 : expected->generation;
    if (current_generation != expected_generation) {
        return Status::unavailable("chunk root publication lost generation race");
    }
    if (!current_.compare_exchange_strong(expected, std::move(manifest), std::memory_order_release,
                                          std::memory_order_acquire)) {
        return Status::unavailable("chunk root publication lost compare-exchange");
    }
    auto history = history_.load(std::memory_order_acquire);
    auto next = history == nullptr ? std::make_shared<ManifestHistory>() : std::make_shared<ManifestHistory>(*history);
    next->push_back(current_.load(std::memory_order_acquire));
    history_.store(std::move(next), std::memory_order_release);
    return Status::Ok();
}

void MemoryRootCatalog::corrupt_active_pack(size_t pack_index, size_t byte_index)
{
    auto current = current_.load(std::memory_order_acquire);
    if (current == nullptr || pack_index >= current->packs.size() ||
        byte_index >= current->packs[pack_index].mirrors[0].size()) {
        return;
    }
    auto corrupt = std::make_shared<ChunkManifest>(*current);
    for (auto &mirror : corrupt->packs[pack_index].mirrors) {
        mirror[byte_index] ^= 0xffU;
    }
    current_.store(std::move(corrupt), std::memory_order_release);
}

void MemoryRootCatalog::corrupt_active_mirror(size_t pack_index, size_t mirror_index, size_t byte_index)
{
    auto current = current_.load(std::memory_order_acquire);
    if (current == nullptr || pack_index >= current->packs.size() ||
        mirror_index >= current->packs[pack_index].mirrors.size() ||
        byte_index >= current->packs[pack_index].mirrors[mirror_index].size()) {
        return;
    }
    auto corrupt = std::make_shared<ChunkManifest>(*current);
    corrupt->packs[pack_index].mirrors[mirror_index][byte_index] ^= 0xffU;
    current_.store(std::move(corrupt), std::memory_order_release);
}

std::shared_ptr<const ChunkManifest> MemoryRootCatalog::load_generation(uint64_t tree_id, uint64_t generation) const
{
    auto history = history_.load(std::memory_order_acquire);
    if (history == nullptr) {
        return nullptr;
    }
    for (const auto &manifest : *history) {
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
    auto manifest = current_.load(std::memory_order_acquire);
    if (manifest == nullptr || segment_index >= manifest->reference_segments.size()) {
        return;
    }
    const uint64_t object_id = manifest->reference_segments[segment_index].object_id;
    auto           current   = reference_segment_store_.load(std::memory_order_acquire);
    for (;;) {
        if (current == nullptr) {
            return;
        }
        auto next  = std::make_shared<ReferenceSegmentStore>(*current);
        auto found = std::find_if(next->begin(), next->end(), [tree_id = manifest->tree_id, object_id](auto &stored) {
            return stored.tree_id == tree_id && stored.image->object_id == object_id;
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

uint64_t MemoryRootCatalog::reclaim_before(uint64_t tree_id, uint64_t generation)
{
    auto history = history_.load(std::memory_order_acquire);
    if (history == nullptr) {
        return 0;
    }
    auto           current             = current_.load(std::memory_order_acquire);
    auto           retained            = std::make_shared<ManifestHistory>();
    uint64_t       reclaimed           = 0;
    const uint64_t fallback_generation = current == nullptr || current->generation == 0 ? 0 : current->generation - 1;
    std::vector<uint64_t> candidate_segments;
    for (const auto &manifest : *history) {
        const bool eligible = manifest->tree_id == tree_id && manifest->generation < generation &&
                              manifest->generation < fallback_generation && manifest.use_count() == 1;
        if (!eligible) {
            retained->push_back(manifest);
            continue;
        }
        for (const ChunkPagePack &pack : manifest->packs) {
            for (const auto &mirror : pack.mirrors) {
                reclaimed += mirror.size();
            }
        }
        for (const ChunkReferenceSegment &segment : manifest->reference_segments) {
            candidate_segments.push_back(segment.object_id);
        }
    }
    history_.store(std::move(retained), std::memory_order_release);
    auto                         retained_history = history_.load(std::memory_order_acquire);
    std::unordered_set<uint64_t> live_segments;
    if (retained_history != nullptr) {
        for (const auto &manifest : *retained_history) {
            for (const ChunkReferenceSegment &segment : manifest->reference_segments) {
                live_segments.insert(segment.object_id);
            }
        }
    }
    candidate_segments.erase(
        std::remove_if(candidate_segments.begin(), candidate_segments.end(),
                       [&live_segments](uint64_t object_id) { return live_segments.contains(object_id); }),
        candidate_segments.end());
    reclaimed += discard_reference_segments(tree_id, candidate_segments);
    return reclaimed;
}

uint64_t MemoryRootCatalog::retained_manifest_count(uint64_t tree_id) const
{
    auto history = history_.load(std::memory_order_acquire);
    if (history == nullptr) {
        return 0;
    }
    return std::count_if(history->begin(), history->end(),
                         [tree_id](const auto &manifest) { return manifest->tree_id == tree_id; });
}

uint64_t MemoryRootCatalog::pinned_bytes(uint64_t tree_id) const
{
    auto history = history_.load(std::memory_order_acquire);
    auto current = current_.load(std::memory_order_acquire);
    if (history == nullptr) {
        return 0;
    }
    uint64_t bytes = 0;
    for (const auto &manifest : *history) {
        if (manifest != current && manifest->tree_id == tree_id && manifest.use_count() > 1) {
            bytes += manifest->logical_size;
        }
    }
    return bytes;
}

uint64_t MemoryRootCatalog::oldest_pin_age_ms(uint64_t tree_id) const
{
    auto history = history_.load(std::memory_order_acquire);
    auto current = current_.load(std::memory_order_acquire);
    if (history == nullptr) {
        return 0;
    }
    uint64_t oldest = 0;
    for (const auto &manifest : *history) {
        if (manifest != current && manifest->tree_id == tree_id && manifest.use_count() > 1 &&
            (oldest == 0 || manifest->published_at_ms < oldest)) {
            oldest = manifest->published_at_ms;
        }
    }
    return oldest == 0 ? 0 : monotonic_millis() - oldest;
}

ChunkPageStore::ChunkPageStore(Config config, std::shared_ptr<RootCatalog> catalog)
    : config_(config),
      catalog_(std::move(catalog))
{
    if (config_.pack_bytes == 0) {
        config_.pack_bytes = 4U * 1024U * 1024U;
    }
    if (config_.iu_size == 0) {
        config_.iu_size = 1;
    }
}

Status ChunkPageStore::materialize_active(std::vector<uint8_t> *out) const
{
    auto manifest = catalog_->load(config_.tree_id);
    if (manifest == nullptr) {
        out->clear();
        return Status::Ok();
    }
    if (manifest->owner_epoch > config_.owner_epoch || manifest_checksum(*manifest) != manifest->checksum) {
        return Status::corruption("chunk manifest checksum or epoch is invalid");
    }
    out->assign(manifest->logical_size, 0);
    for (const ChunkPagePack &pack : manifest->packs) {
        ChunkPageRef ref;
        Status       resolve_status = resolve_ordinal(*manifest, pack.ordinal, &ref);
        if (!resolve_status.ok() || ref.chunk_id != pack.ref.chunk_id || ref.offset != pack.ref.offset ||
            ref.length != pack.ref.length || ref.checksum != pack.ref.checksum || ref.offset > out->size() ||
            ref.length > out->size() - ref.offset) {
            return Status::corruption("chunk page pack checksum or bounds are invalid");
        }
        const std::vector<uint8_t> *valid_mirror = nullptr;
        for (const auto &mirror : pack.mirrors) {
            if (mirror.size() == ref.length && ref.checksum == crowdb::common::crc32c(mirror.data(), mirror.size())) {
                valid_mirror = &mirror;
                break;
            }
        }
        if (valid_mirror == nullptr) {
            return Status::corruption("chunk page pack has no valid mirror");
        }
        std::memcpy(out->data() + ref.offset, valid_mirror->data(), valid_mirror->size());
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
    if (unavailable_.load(std::memory_order_acquire)) {
        return Status::unavailable("chunk mirrors unavailable after bounded retries");
    }
    if (buf == nullptr && len != 0) {
        return Status::invalid_argument("chunk page read has null buffer");
    }
    auto manifest = load_layout();
    if (manifest == nullptr || off > manifest->logical_size || len > manifest->logical_size - off) {
        return Status::unavailable("chunk page range is not present in the published manifest");
    }
    if (manifest_checksum(*manifest) != manifest->checksum) {
        return Status::corruption("chunk manifest checksum mismatch");
    }
    size_t copied = 0;
    for (const ChunkPagePack &pack : manifest->packs) {
        ChunkPageRef ref;
        Status       resolve_status = resolve_ordinal(*manifest, pack.ordinal, &ref);
        if (!resolve_status.ok() || ref.chunk_id != pack.ref.chunk_id || ref.offset != pack.ref.offset ||
            ref.length != pack.ref.length || ref.checksum != pack.ref.checksum) {
            return Status::corruption("chunk page reference segment is invalid");
        }
        const uint64_t pack_end = ref.offset + ref.length;
        const uint64_t read_end = off + len;
        if (pack_end <= off || ref.offset >= read_end) {
            continue;
        }
        pack_reads_.fetch_add(1, std::memory_order_relaxed);
        const std::vector<uint8_t> *valid_mirror = nullptr;
        for (const auto &mirror : pack.mirrors) {
            if (mirror.size() == ref.length && ref.checksum == crowdb::common::crc32c(mirror.data(), mirror.size())) {
                valid_mirror = &mirror;
                break;
            }
        }
        if (valid_mirror == nullptr) {
            return Status::corruption("chunk page pack checksum mismatch on every mirror");
        }
        const uint64_t begin = std::max(off, ref.offset);
        const uint64_t end   = std::min(read_end, pack_end);
        std::memcpy(buf + (begin - off), valid_mirror->data() + (begin - ref.offset), end - begin);
        copied += end - begin;
    }
    if (copied != len) {
        return Status::corruption("chunk manifest has a page-pack coverage gap");
    }
    return Status::Ok();
}

std::shared_ptr<const ChunkManifest> ChunkPageStore::load_layout() const
{
    const uint64_t now    = monotonic_millis();
    auto           cached = cached_layout_.load(std::memory_order_acquire);
    if (cached != nullptr && now < layout_valid_until_ms_.load(std::memory_order_acquire)) {
        cache_hits_.fetch_add(1, std::memory_order_relaxed);
        return cached;
    }
    auto manifest = catalog_->load(config_.tree_id);
    layout_queries_.fetch_add(1, std::memory_order_relaxed);
    cached_layout_.store(manifest, std::memory_order_release);
    layout_valid_until_ms_.store(now + config_.layout_validity_ms, std::memory_order_release);
    return manifest;
}

uint32_t ChunkPageStore::reference_segment_checksum(const ChunkReferenceSegmentImage &segment)
{
    uint32_t crc = update_u64(0, segment.object_id);
    crc          = update_u64(crc, segment.first_ordinal);
    for (const ChunkPageRef &ref : segment.refs) {
        crc = update_u64(crc, ref.chunk_id);
        crc = update_u64(crc, ref.offset);
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&ref.length), sizeof(ref.length));
        crc =
            crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&ref.checksum), sizeof(ref.checksum));
    }
    return crc;
}

Status ChunkPageStore::resolve_ordinal(const ChunkManifest &manifest, uint64_t ordinal, ChunkPageRef *out) const
{
    const uint64_t segment_index = ordinal / kReferencesPerSegment;
    if (segment_index >= manifest.reference_segments.size()) {
        return Status::corruption("chunk reference ordinal has no directory entry");
    }
    const ChunkReferenceSegment &descriptor = manifest.reference_segments[segment_index];
    if (descriptor.first_ordinal != segment_index * kReferencesPerSegment || descriptor.ref_count == 0 ||
        descriptor.ref_count > kReferencesPerSegment) {
        return Status::corruption("chunk reference segment directory entry is invalid");
    }
    auto segment = catalog_->load_reference_segment(manifest.tree_id, descriptor.object_id);
    if (segment == nullptr || segment->object_id != descriptor.object_id ||
        segment->first_ordinal != descriptor.first_ordinal || segment->refs.size() != descriptor.ref_count ||
        reference_segment_checksum(*segment) != descriptor.checksum) {
        return Status::corruption("chunk reference segment image is missing or corrupt");
    }
    const uint64_t offset = ordinal - segment->first_ordinal;
    if (offset >= segment->refs.size()) {
        return Status::corruption("chunk reference ordinal exceeds its segment image");
    }
    *out = segment->refs[offset];
    return Status::Ok();
}

uint32_t ChunkPageStore::manifest_checksum(const ChunkManifest &manifest)
{
    uint32_t crc = 0;
    crc          = update_u64(crc, manifest.tree_id);
    crc          = update_u64(crc, manifest.generation);
    crc          = update_u64(crc, manifest.owner_epoch);
    crc          = update_u64(crc, manifest.logical_size);
    crc          = update_u64(crc, manifest.published_at_ms);
    for (const ChunkReferenceSegment &segment : manifest.reference_segments) {
        crc = update_u64(crc, segment.object_id);
        crc = update_u64(crc, segment.first_ordinal);
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&segment.ref_count),
                                            sizeof(segment.ref_count));
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&segment.checksum),
                                            sizeof(segment.checksum));
    }
    return crc;
}

Status ChunkPageStore::build_manifest(std::shared_ptr<ChunkManifest> *out)
{
    if (!orphan_reference_segments_.empty()) {
        catalog_->discard_reference_segments(config_.tree_id, orphan_reference_segments_);
        orphan_reference_segments_.clear();
    }
    auto prior                = catalog_->load(config_.tree_id);
    auto manifest             = std::make_shared<ChunkManifest>();
    manifest->tree_id         = config_.tree_id;
    manifest->generation      = prior == nullptr ? 1 : prior->generation + 1;
    manifest->owner_epoch     = config_.owner_epoch;
    manifest->logical_size    = staged_.size();
    manifest->published_at_ms = monotonic_millis();
    uint64_t offset           = 0;
    while (offset < staged_.size()) {
        const size_t  length = std::min(config_.pack_bytes, staged_.size() - static_cast<size_t>(offset));
        ChunkPagePack pack;
        pack.ordinal      = manifest->packs.size();
        pack.ref.chunk_id = (manifest->generation << 32U) | manifest->packs.size();
        pack.ref.offset   = offset;
        pack.ref.length   = static_cast<uint32_t>(length);
        pack.ref.checksum = crowdb::common::crc32c(staged_.data() + offset, length);
        for (size_t mirror = 0; mirror < pack.mirrors.size(); ++mirror) {
            bool written = false;
            for (uint32_t attempt = 0; attempt <= config_.mirror_retry_limit; ++attempt) {
                mirror_write_attempts_.fetch_add(1, std::memory_order_relaxed);
                if ((mirror_write_failure_mask_.load(std::memory_order_acquire) & (1U << mirror)) == 0) {
                    pack.mirrors[mirror].assign(staged_.begin() + offset, staged_.begin() + offset + length);
                    written = true;
                    break;
                }
                mirror_write_failures_.fetch_add(1, std::memory_order_relaxed);
            }
            if (!written) {
                return Status::unavailable("chunk mirror write unavailable after bounded retries");
            }
        }
        manifest->packs.push_back(std::move(pack));
        offset += length;
    }
    for (size_t first = 0; first < manifest->packs.size(); first += kReferencesPerSegment) {
        auto segment           = std::make_shared<ChunkReferenceSegmentImage>();
        segment->object_id     = (manifest->generation << 32U) | (first / kReferencesPerSegment + 1);
        segment->first_ordinal = first;
        const size_t end       = std::min(first + kReferencesPerSegment, manifest->packs.size());
        for (size_t index = first; index < end; ++index) {
            segment->refs.push_back(manifest->packs[index].ref);
        }
        ChunkReferenceSegment descriptor{
            .object_id     = segment->object_id,
            .first_ordinal = segment->first_ordinal,
            .ref_count     = static_cast<uint32_t>(segment->refs.size()),
            .checksum      = reference_segment_checksum(*segment),
        };
        Status persist_status = catalog_->persist_reference_segment(config_.tree_id, std::move(segment));
        if (!persist_status.ok()) {
            catalog_->discard_reference_segments(config_.tree_id, reference_segment_ids(*manifest));
            return persist_status;
        }
        manifest->reference_segments.push_back(descriptor);
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
        ids.push_back(segment.object_id);
    }
    return ids;
}

Status ChunkPageStore::sync()
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
    std::shared_ptr<ChunkManifest> manifest;
    Status                         build_status = build_manifest(&manifest);
    if (!build_status.ok()) {
        orphan_bytes_.fetch_add(staged_.size(), std::memory_order_relaxed);
        return build_status;
    }
    auto           prior               = catalog_->load(config_.tree_id);
    const uint64_t expected_generation = prior == nullptr ? 0 : prior->generation;
    Status         status = catalog_->publish(config_.tree_id, expected_generation, config_.owner_epoch, manifest);
    if (!status.ok()) {
        orphan_bytes_.fetch_add(staged_.size(), std::memory_order_relaxed);
        auto ids = reference_segment_ids(*manifest);
        orphan_reference_segments_.insert(orphan_reference_segments_.end(), ids.begin(), ids.end());
        return status;
    }
    generations_published_.fetch_add(1, std::memory_order_relaxed);
    packs_written_.fetch_add(manifest->packs.size(), std::memory_order_relaxed);
    pack_bytes_written_.fetch_add(staged_.size(), std::memory_order_relaxed);
    cached_layout_.store(manifest, std::memory_order_release);
    layout_valid_until_ms_.store(monotonic_millis() + config_.layout_validity_ms, std::memory_order_release);
    staged_initialized_ = false;
    staged_.clear();
    data_durable_ = false;
    anchor_dirty_ = false;
    return Status::Ok();
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
    on_complete.complete(read_at(addr, static_cast<uint8_t *>(buf), len));
    return 0;
}

uint64_t ChunkPageStore::submit_write(PageAddr addr, const void *buf, size_t len, AsyncCompletion on_complete)
{
    on_complete.complete(write_at(addr, static_cast<const uint8_t *>(buf), len));
    return 0;
}

Status ChunkPageStore::submit_fsync(AsyncCompletion on_complete)
{
    Status status = sync();
    on_complete.complete(status);
    return Status::Ok();
}

void ChunkPageStore::cancel(uint64_t)
{
}

ChunkPageStoreStats ChunkPageStore::stats() const
{
    return {
        .generations_published = generations_published_.load(std::memory_order_relaxed),
        .packs_written         = packs_written_.load(std::memory_order_relaxed),
        .pack_bytes_written    = pack_bytes_written_.load(std::memory_order_relaxed),
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

struct ct_root_catalog
{
    std::shared_ptr<crowdb::tree::detail::RootCatalog> catalog;
};

ct_status ct_memory_root_catalog_open(uint64_t owner_epoch, ct_root_catalog **out)
{
    if (out == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    auto handle     = std::make_unique<ct_root_catalog>();
    handle->catalog = std::make_shared<crowdb::tree::detail::MemoryRootCatalog>(owner_epoch);
    *out            = handle.release();
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
        crowdb::tree::detail::ChunkPageStore::Config{.tree_id     = options->tree_id,
                                                     .owner_epoch = options->owner_epoch,
                                                     .pack_bytes  = options->pack_bytes,
                                                     .iu_size     = options->iu_size},
        catalog->catalog);
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
