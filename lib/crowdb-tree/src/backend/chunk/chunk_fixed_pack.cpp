// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_fixed_pack.h"

#include "btree/allocation_bound.h"
#include "chunk_backing.h"
#include "chunk_frame.h"
#include "chunk_physical_registry.h"
#include "crowdb-common/crc32c.h"

namespace crowdb::tree::detail
{
ChunkFixedPackStore::ChunkFixedPackStore(uint64_t tree, uint64_t epoch, uint32_t mirrors,
                                         std::shared_ptr<RootCatalog>           catalog,
                                         std::shared_ptr<ChunkTransport>        transport,
                                         std::shared_ptr<ChunkPhysicalRegistry> registry)
    : tree_(tree),
      epoch_(epoch),
      mirrors_(mirrors),
      catalog_(std::move(catalog)),
      transport_(std::move(transport)),
      registry_(std::move(registry))
{
}

bool ChunkFixedPackStore::supported() const noexcept
{
    return tree_ != 0 && epoch_ != 0 && mirrors_ != 0 && mirrors_ <= kMaxMirrorCopies && catalog_ &&
           catalog_->stage_schema_supported() && transport_ && transport_->fixed_staging_supported();
}

Status ChunkFixedPackStore::write(ChunkStageId operation, ChunkId fixed, PagePurpose purpose,
                                  std::span<const uint8_t> input, const std::shared_ptr<AllocationBudget> &budget,
                                  ChunkFixedPackRef *out, std::shared_ptr<ChunkPhysicalPack> *physical,
                                  const std::shared_ptr<AllocationBudget> &physical_budget)
{
    const Write item{.input = input, .out = out};
    return write_many(operation, fixed, purpose, std::span(&item, 1), budget, physical, physical_budget);
}

Status ChunkFixedPackStore::write_many(ChunkStageId operation, ChunkId fixed, PagePurpose purpose,
                                       std::span<const Write> inputs, const std::shared_ptr<AllocationBudget> &budget,
                                       std::shared_ptr<ChunkPhysicalPack>      *physical,
                                       const std::shared_ptr<AllocationBudget> &physical_budget)
try {
    const auto context = ChunkIoContext::capture();
    if (!supported()) {
        return Status::not_supported("fixed staged chunk writes are unavailable");
    }
    if (!budget || inputs.empty() || inputs.size() > kMaxStagePacks || operation.owner_epoch != epoch_) {
        return Status::invalid_argument();
    }
    size_t bytes = 0;
    for (const auto &item : inputs) {
        const auto framed = chunk_framed_length(item.input.size());
        if (item.out == nullptr || !write_allocation_bound(item.input.size()) || framed > kMaxChunkPackBytes - bytes) {
            return Status::invalid_argument("aggregate exceeds Chunk pack geometry");
        }
        bytes += framed;
    }
    auto framed = std::allocate_shared<ChunkPackWindow>(MetadataAllocator<ChunkPackWindow>(budget), budget);
    framed->bytes.resize(bytes);
    size_t offset = 0;
    for (const auto &item : inputs) {
        const auto length = chunk_framed_length(item.input.size());
        auto status = encode_fixed_pack(fixed, purpose, item.input, std::span(framed->bytes).subspan(offset, length));
        if (!status.ok()) {
            return status;
        }
        offset += length;
    }
    ChunkStageRecord reserved{.tree_id             = tree_,
                              .id                  = operation,
                              .revision            = 1,
                              .chunk_high          = fixed.high,
                              .chunk_low           = fixed.low,
                              .capacity            = framed->bytes.size(),
                              .manifest_generation = 0,
                              .purpose             = static_cast<uint32_t>(purpose),
                              .state               = ChunkStageState::Reserved,
                              .packs = decltype(ChunkStageRecord::packs)(MetadataAllocator<ChunkStagePack>(budget))};
    auto             status = validate_stage_record(reserved);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<ChunkPhysicalPack> observed;
    ChunkPhysicalAdmission             physical_admission;
    if (registry_) {
        if (physical == nullptr) {
            return Status::invalid_argument();
        }
        status = registry_->prepare(operation, fixed, physical_budget ? physical_budget : budget, &observed,
                                    &physical_admission);
        if (!status.ok()) {
            return status;
        }
        *physical = observed;
    }
    // This durable edge must precede even submission of AllocateChunk.
    status = catalog_->compare_stage(tree_, epoch_, {}, reserved);
    if (!status.ok()) {
        return status;
    }
    physical_admission.accept();
    status = transport_->allocate_fixed_chunk(fixed, framed->bytes.size(), epoch_, purpose);
    if (registry_) {
        ChunkAllocationIdentity receipt;
        auto                    inspected = transport_->inspect_fixed_allocation(fixed, epoch_, &receipt);
        if (inspected.ok()) {
            inspected = registry_->observe(observed, receipt, budget);
        }
        if (status.ok() && !inspected.ok()) {
            return inspected;
        }
    }
    if (!status.ok()) {
        return status;
    }
    status = transport_->verify_fixed_read_fence(fixed, mirrors_, context);
    if (!status.ok()) {
        return status;
    }
    auto writing = reserved;
    writing.revision++;
    writing.state = ChunkStageState::Writing;
    offset        = 0;
    for (const auto &item : inputs) {
        writing.packs.push_back({.offset   = offset,
                                 .length   = static_cast<uint32_t>(item.input.size()),
                                 .checksum = crowdb::common::crc32c(item.input.data(), item.input.size())});
        offset += chunk_framed_length(item.input.size());
    }
    status = catalog_->compare_stage(tree_, epoch_, reserved, writing);
    if (!status.ok()) {
        return status;
    }
    for (uint32_t mirror = 0; mirror < mirrors_; ++mirror) {
        status = transport_->write_fixed_mirror(fixed, mirror, 0, framed->bytes.data(), framed->bytes.size(), context);
        if (!status.ok()) {
            return status;
        }
    }
    status = transport_->advance_write(fixed, 0, framed->bytes.size());
    if (!status.ok()) {
        return status;
    }
    status = transport_->seal_chunk(fixed, epoch_, framed->bytes.size());
    if (!status.ok()) {
        return status;
    }
    auto reloadable = writing;
    reloadable.revision++;
    reloadable.state = ChunkStageState::Reloadable;
    status           = catalog_->compare_stage(tree_, epoch_, writing, reloadable);
    if (!status.ok()) {
        return status;
    }
    for (size_t index = 0; index < inputs.size(); ++index) {
        const auto &pack   = writing.packs[index];
        *inputs[index].out = {.stage    = operation,
                              .chunk    = fixed,
                              .offset   = pack.offset,
                              .length   = pack.length,
                              .checksum = pack.checksum,
                              .purpose  = purpose};
    }
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status ChunkFixedPackStore::read(const ChunkFixedPackRef &ref, const std::shared_ptr<AllocationBudget> &budget,
                                 std::shared_ptr<const ChunkPackWindow> *out) const
try {
    if (!budget || out == nullptr || *out || !read_allocation_bound(ref.length)) {
        return Status::invalid_argument();
    }
    auto result = std::allocate_shared<ChunkPackWindow>(MetadataAllocator<ChunkPackWindow>(budget), budget);
    result->bytes.resize(ref.length);
    auto status = read_into(ref, budget, result->bytes);
    if (status.ok()) {
        *out = std::move(result);
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status ChunkFixedPackStore::read_into(const ChunkFixedPackRef &ref, const std::shared_ptr<AllocationBudget> &budget,
                                      std::span<uint8_t> output) const
try {
    const auto context = ChunkIoContext::capture();
    if (!transport_ || !budget || output.size() != ref.length || !read_into_allocation_bound(ref.length)) {
        return Status::invalid_argument();
    }
    auto framed = std::allocate_shared<ChunkPackWindow>(MetadataAllocator<ChunkPackWindow>(budget), budget);
    framed->bytes.resize(chunk_framed_length(ref.length));
    if (ref.offset > UINT64_MAX - framed->bytes.size()) {
        return Status::corruption();
    }
    bool responded = false;
    for (uint32_t mirror = 0; mirror < mirrors_; ++mirror) {
        auto status = transport_->read_fixed_mirror(ref.chunk, mirror, ref.offset, framed->bytes.data(),
                                                    framed->bytes.size(), context);
        if (!status.ok()) {
            continue;
        }
        responded = true;
        status    = decode_fixed_pack(ref, framed->bytes, output);
        if (status.ok()) {
            return status;
        }
    }
    return responded ? Status::corruption("no mirror supplied the fixed pack")
                     : Status::unavailable("fixed pack is unavailable on every mirror");
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
} // namespace crowdb::tree::detail
