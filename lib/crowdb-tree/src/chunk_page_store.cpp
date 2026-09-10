// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "chunk_page_store.h"

#include "c_api_internal.h"
#include "crowdb-common/crc32c.h"

#include <algorithm>
#include <cstring>
#include <limits>

namespace crowdb::tree::detail
{
namespace
{

constexpr uint64_t kAnchorRegionBytes = 8192;

uint32_t update_u64(uint32_t crc, uint64_t value)
{
    return crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&value), sizeof(value));
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
    return Status::Ok();
}

void MemoryRootCatalog::corrupt_active_pack(size_t pack_index, size_t byte_index)
{
    auto current = current_.load(std::memory_order_acquire);
    if (current == nullptr || pack_index >= current->packs.size() ||
        byte_index >= current->packs[pack_index].bytes.size()) {
        return;
    }
    auto corrupt = std::make_shared<ChunkManifest>(*current);
    corrupt->packs[pack_index].bytes[byte_index] ^= 0xffU;
    current_.store(std::move(corrupt), std::memory_order_release);
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
        if (pack.ref.length != pack.bytes.size() ||
            pack.ref.checksum != crowdb::common::crc32c(pack.bytes.data(), pack.bytes.size()) ||
            pack.ref.offset > out->size() || pack.bytes.size() > out->size() - pack.ref.offset) {
            return Status::corruption("chunk page pack checksum or bounds are invalid");
        }
        std::memcpy(out->data() + pack.ref.offset, pack.bytes.data(), pack.bytes.size());
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
    auto manifest = catalog_->load(config_.tree_id);
    if (manifest == nullptr || off > manifest->logical_size || len > manifest->logical_size - off) {
        return Status::unavailable("chunk page range is not present in the published manifest");
    }
    if (manifest_checksum(*manifest) != manifest->checksum) {
        return Status::corruption("chunk manifest checksum mismatch");
    }
    size_t copied = 0;
    for (const ChunkPagePack &pack : manifest->packs) {
        const uint64_t pack_end = pack.ref.offset + pack.ref.length;
        const uint64_t read_end = off + len;
        if (pack_end <= off || pack.ref.offset >= read_end) {
            continue;
        }
        pack_reads_.fetch_add(1, std::memory_order_relaxed);
        if (pack.ref.checksum != crowdb::common::crc32c(pack.bytes.data(), pack.bytes.size())) {
            return Status::corruption("chunk page pack checksum mismatch");
        }
        const uint64_t begin = std::max(off, pack.ref.offset);
        const uint64_t end   = std::min(read_end, pack_end);
        std::memcpy(buf + (begin - off), pack.bytes.data() + (begin - pack.ref.offset), end - begin);
        copied += end - begin;
    }
    if (copied != len) {
        return Status::corruption("chunk manifest has a page-pack coverage gap");
    }
    cache_hits_.fetch_add(1, std::memory_order_relaxed);
    return Status::Ok();
}

uint32_t ChunkPageStore::manifest_checksum(const ChunkManifest &manifest)
{
    uint32_t crc = 0;
    crc          = update_u64(crc, manifest.tree_id);
    crc          = update_u64(crc, manifest.generation);
    crc          = update_u64(crc, manifest.owner_epoch);
    crc          = update_u64(crc, manifest.logical_size);
    for (const ChunkPagePack &pack : manifest.packs) {
        crc = update_u64(crc, pack.ref.chunk_id);
        crc = update_u64(crc, pack.ref.offset);
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&pack.ref.length),
                                            sizeof(pack.ref.length));
        crc = crowdb::common::crc32c_update(crc, reinterpret_cast<const uint8_t *>(&pack.ref.checksum),
                                            sizeof(pack.ref.checksum));
    }
    return crc;
}

std::shared_ptr<const ChunkManifest> ChunkPageStore::build_manifest() const
{
    auto prior             = catalog_->load(config_.tree_id);
    auto manifest          = std::make_shared<ChunkManifest>();
    manifest->tree_id      = config_.tree_id;
    manifest->generation   = prior == nullptr ? 1 : prior->generation + 1;
    manifest->owner_epoch  = config_.owner_epoch;
    manifest->logical_size = staged_.size();
    uint64_t offset        = 0;
    while (offset < staged_.size()) {
        const size_t  length = std::min(config_.pack_bytes, staged_.size() - static_cast<size_t>(offset));
        ChunkPagePack pack;
        pack.ref.chunk_id = (manifest->generation << 32U) | manifest->packs.size();
        pack.ref.offset   = offset;
        pack.ref.length   = static_cast<uint32_t>(length);
        pack.bytes.assign(staged_.begin() + offset, staged_.begin() + offset + length);
        pack.ref.checksum = crowdb::common::crc32c(pack.bytes.data(), pack.bytes.size());
        manifest->packs.push_back(std::move(pack));
        offset += length;
    }
    manifest->checksum = manifest_checksum(*manifest);
    return manifest;
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
    auto           manifest            = build_manifest();
    auto           prior               = catalog_->load(config_.tree_id);
    const uint64_t expected_generation = prior == nullptr ? 0 : prior->generation;
    Status         status = catalog_->publish(config_.tree_id, expected_generation, config_.owner_epoch, manifest);
    if (!status.ok()) {
        orphan_bytes_.fetch_add(staged_.size(), std::memory_order_relaxed);
        return status;
    }
    generations_published_.fetch_add(1, std::memory_order_relaxed);
    packs_written_.fetch_add(manifest->packs.size(), std::memory_order_relaxed);
    pack_bytes_written_.fetch_add(staged_.size(), std::memory_order_relaxed);
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

ChunkPageStoreStats ChunkPageStore::stats() const
{
    return {
        .generations_published = generations_published_.load(std::memory_order_relaxed),
        .packs_written         = packs_written_.load(std::memory_order_relaxed),
        .pack_bytes_written    = pack_bytes_written_.load(std::memory_order_relaxed),
        .pack_reads            = pack_reads_.load(std::memory_order_relaxed),
        .cache_hits            = cache_hits_.load(std::memory_order_relaxed),
        .orphan_bytes          = orphan_bytes_.load(std::memory_order_relaxed),
    };
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
    auto handle           = std::make_unique<ct_page_store>();
    handle->bundle        = std::make_shared<PageStoreBundle>();
    handle->bundle->store = std::make_unique<crowdb::tree::detail::ChunkPageStore>(
        crowdb::tree::detail::ChunkPageStore::Config{.tree_id     = options->tree_id,
                                                     .owner_epoch = options->owner_epoch,
                                                     .pack_bytes  = options->pack_bytes,
                                                     .iu_size     = options->iu_size},
        catalog->catalog);
    handle->bundle->backend_label = "chunk";
    *out                          = handle.release();
    return static_cast<ct_status>(crowdb::tree::Code::kOk);
}
