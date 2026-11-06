// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <span>

namespace crowdb::tree
{
class BackingMetricExtent;
struct OverflowBackingSet;
struct ChunkPackBacking;
[[nodiscard]] bool chunk_backing_matches(const ChunkPackBacking &, uint64_t domain, uint64_t length) noexcept;
[[nodiscard]] bool same_chunk_backing(const std::shared_ptr<const ChunkPackBacking> &,
                                      const std::shared_ptr<const ChunkPackBacking> &) noexcept;
[[nodiscard]] std::array<uint64_t, 8> chunk_backing_identity(const ChunkPackBacking &) noexcept;
enum class BackingStorage : uint8_t { kDurableBlob, kResidentFrame, kChunkPack };
enum class BackingDurability : uint8_t { kReloadable, kCheckpointCommitted, kResident };

struct BackingCapabilities
{
    bool independent_reload = false;
    bool staged_lifetime    = false;

    [[nodiscard]] bool can_stage() const
    {
        return independent_reload && staged_lifetime;
    }
};

// A readable version, not a promise that a write will eventually finish. The
// backend owner protects every extent/overflow object reachable from this page.
// Only a completed checkpoint changes the durable frontier.
struct BackingRef
{
    bool retire_step() noexcept
    {
        return true;
    }

    uint64_t                    backend_id = 0;
    uint64_t                    address    = 0;
    uint64_t                    length     = 0;
    uint64_t                    content_id = 0;
    uint64_t                    generation = 0;
    uint64_t                    checksum   = 0;
    uint32_t                    encoding   = 0;
    BackingDurability           durability = BackingDurability::kReloadable;
    std::shared_ptr<const void> owner;
    // Exact allocation protection, without transitive page dependencies. GC
    // can rebind a retained frame to a new dependency graph without retaining
    // the old graph. It is never sufficient as a complete primary owner.
    std::shared_ptr<const void>          physical_owner;
    std::shared_ptr<BackingMetricExtent> physical_metrics = nullptr;
    // Exact immutable physical dependencies of this primary frame. Keeping a
    // leaf readable must not pin unrelated pages from its checkpoint.
    std::shared_ptr<const OverflowBackingSet> overflow;

    // An imported raw frame has an allocation owner, not a file extent. It
    // never qualifies for checkpoint reuse or backend garbage collection.
    BackingStorage                          storage = BackingStorage::kDurableBlob;
    std::span<const uint8_t>                resident_frame;
    std::shared_ptr<const ChunkPackBacking> chunk;

    [[nodiscard]] bool reloadable() const
    {
        return length != 0 && owner != nullptr &&
               (storage == BackingStorage::kDurableBlob ||
                (storage == BackingStorage::kChunkPack && address == 0 && chunk &&
                 chunk_backing_matches(*chunk, backend_id, length)));
    }

    [[nodiscard]] bool readable() const
    {
        return reloadable() || (storage == BackingStorage::kResidentFrame && owner != nullptr &&
                                !resident_frame.empty() && length == 0 && address == 0 && backend_id == 0);
    }
};

// A nonwrapping physical interval selected for relocation. A zero length
// leaves ordinary clean-primary and overflow reuse unchanged.
struct BackingExclusion
{
    uint64_t address = 0;
    uint64_t length  = 0;

    [[nodiscard]] bool overlaps(uint64_t begin, uint64_t bytes) const noexcept
    {
        return length != 0 && bytes != 0 && (begin >= address ? begin - address < length : address - begin < bytes);
    }

    [[nodiscard]] bool overlaps(const BackingRef &backing) const noexcept
    {
        return backing.storage == BackingStorage::kDurableBlob && backing.reloadable() &&
               overlaps(backing.address, backing.length);
    }
};
} // namespace crowdb::tree
