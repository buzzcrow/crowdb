// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_protection_callback.h"

namespace crowdb::tree::detail
{
namespace pc = protection_callback;

CallbackRootCatalog::CallbackRootCatalog(ct_root_catalog_callbacks callbacks, ct_root_catalog_staging_callbacks staging,
                                         ct_root_catalog_protection_callbacks protection, void *context)
    : protection_(protection),
      staging_(staging),
      callbacks_(callbacks),
      context_(context)
{
}

ChunkProtectionCatalog *CallbackRootCatalog::protection() noexcept
{
    return stage_schema_supported() && pc::supported(protection_) ? this : nullptr;
}

Status CallbackRootCatalog::check_read_owner() const
{
    return pc::supported(protection_) ? pc::status(protection_.check_reader(context_)) : Status::not_supported();
}

Status CallbackRootCatalog::writer_quiesced(uint64_t tree, uint64_t epoch)
{
    if (!pc::supported(protection_)) {
        return Status::not_supported();
    }
    if (tree == 0 || epoch == 0) {
        return Status::invalid_argument();
    }
    return pc::status(protection_.writer_quiesced(context_, tree, epoch));
}

namespace
{
template <class Bytes>
Status load_root(const ct_root_catalog_callbacks &callbacks, const ct_root_catalog_protection_callbacks &protection,
                 void *context, uint64_t tree, uint64_t generation, Bytes *out, int kind = 0)
{
    if (!pc::supported(protection)) {
        return Status::not_supported();
    }
    if (tree == 0 || out == nullptr) {
        return Status::invalid_argument();
    }
    const uint8_t *data   = nullptr;
    size_t         length = 0;
    if (kind == 0) {
        kind = generation == 0 ? CT_ROOT_CATALOG_CURRENT_MANIFEST : CT_ROOT_CATALOG_MANIFEST;
    }
    const auto result = callbacks.load(context, kind, tree, generation, &data, &length);

    struct Blob
    {
        const ct_root_catalog_callbacks &callbacks;
        void                            *context;
        const uint8_t                   *data;
        size_t                           length;

        ~Blob()
        {
            if (data != nullptr) {
                callbacks.free_blob(context, data, length);
            }
        }
    } blob{.callbacks = callbacks, .context = context, .data = data, .length = length};

    auto status = pc::status(result);
    if (!status.ok()) {
        return status;
    }
    if (data == nullptr || length == 0 || length > kMaxCandidateManifestBytes) {
        return Status::corruption();
    }
    out->assign(data, data + length);
    return Status::Ok();
}

} // namespace

Status CallbackRootCatalog::load_root_bytes(uint64_t tree, uint64_t generation, std::vector<uint8_t> *out) const
{
    return load_root(callbacks_, protection_, context_, tree, generation, out);
}

Status CallbackRootCatalog::load_root_owned(uint64_t tree, uint64_t generation, ChunkRootBytes *out) const
try {
    if (out == nullptr || !out->get_allocator().owner()) {
        return Status::invalid_argument();
    }
    return load_root(callbacks_, protection_, context_, tree, generation, out);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status CallbackRootCatalog::load_legacy_reference_owned(uint64_t tree, uint64_t object, ChunkRootBytes *out) const
try {
    if (object == 0 || out == nullptr || !out->get_allocator().owner()) {
        return Status::invalid_argument();
    }
    return load_root(callbacks_, protection_, context_, tree, object, out, CT_ROOT_CATALOG_REFERENCE_SEGMENT);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status CallbackRootCatalog::prepare_legacy_migration(uint64_t tree, uint64_t generation,
                                                     std::span<const uint8_t> manifest)
{
    if (!pc::supported(protection_)) {
        return Status::not_supported();
    }
    if (tree == 0 || generation == 0 || manifest.empty() || manifest.size() > kMaxCandidateManifestBytes) {
        return Status::invalid_argument();
    }
    return pc::status(callbacks_.store(context_, CT_ROOT_CATALOG_LEGACY_MIGRATION, tree, generation, manifest.data(),
                                       manifest.size()));
}

Status CallbackRootCatalog::load_legacy_migration_owned(uint64_t tree, uint64_t generation, ChunkRootBytes *out) const
try {
    if (generation == 0 || out == nullptr || !out->get_allocator().owner()) {
        return Status::invalid_argument();
    }
    return load_root(callbacks_, protection_, context_, tree, generation, out, CT_ROOT_CATALOG_LEGACY_MIGRATION);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status CallbackRootCatalog::begin_candidate(uint64_t tree, const ChunkManifestCandidate &candidate)
{
    if (!pc::supported(protection_)) {
        return Status::not_supported();
    }
    if (tree == 0 || !pc::valid(candidate.id) || candidate.expected_generation == UINT64_MAX ||
        candidate.reference_count > kMaxCandidateReferences || candidate.manifest.empty() ||
        candidate.manifest.size() > kMaxCandidateManifestBytes ||
        (candidate.reference_count == 0 && candidate.reference_checksum != 0)) {
        return Status::invalid_argument();
    }
    return pc::status(protection_.begin(context_, tree, pc::external(candidate.id), candidate.expected_generation,
                                        candidate.reference_count, candidate.reference_checksum,
                                        candidate.manifest.data(), candidate.manifest.size()));
}

Status CallbackRootCatalog::load_candidate(uint64_t tree, ChunkStageId id,
                                           std::optional<ChunkCandidateStatus> *out) const
{
    if (!pc::supported(protection_)) {
        return Status::not_supported();
    }
    if (tree == 0 || !pc::valid(id) || out == nullptr) {
        return Status::invalid_argument();
    }
    ct_chunk_candidate_status wire{};
    auto                      status = pc::status(protection_.load(context_, tree, pc::external(id), &wire));
    if (status.code() == Code::kNotFound) {
        out->reset();
        return Status::Ok();
    }
    if (!status.ok()) {
        return status;
    }
    ChunkCandidateStatus candidate;
    status = pc::decode(wire, &candidate);
    if (!status.ok() || candidate.id != id) {
        return Status::corruption();
    }
    *out = candidate;
    return Status::Ok();
}

Status CallbackRootCatalog::list_candidates(uint64_t tree, std::optional<ChunkStageId> after, uint32_t limit,
                                            std::vector<ChunkCandidateStatus> *out) const
{
    if (!pc::supported(protection_)) {
        return Status::not_supported();
    }
    if (tree == 0 || limit == 0 || limit > kMaxStageScan || out == nullptr || (after && !pc::valid(*after))) {
        return Status::invalid_argument();
    }
    std::array<ct_chunk_candidate_status, kMaxStageScan> wire{};
    size_t                                               count  = limit;
    const auto                                           cursor = pc::external(after.value_or(ChunkStageId{}));
    auto status = pc::status(protection_.list(context_, tree, after ? &cursor : nullptr, limit, wire.data(), &count));
    if (!status.ok()) {
        return status;
    }
    if (count > limit) {
        return Status::corruption();
    }
    std::vector<ChunkCandidateStatus> result;
    result.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        ChunkCandidateStatus value;
        status = pc::decode(wire[i], &value);
        if (!status.ok() || (after && value.id <= *after)) {
            return Status::corruption();
        }
        after = value.id;
        result.push_back(value);
    }
    *out = std::move(result);
    return Status::Ok();
}
} // namespace crowdb::tree::detail
