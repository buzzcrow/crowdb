// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Bounded portable tuple and native frame streams over an immutable root.
// Both formats retain owned windows and install through a private generation.
#pragma once

#include "crowdb-tree/memory_budget.h"
#include "crowdb-tree/slice.h"
#include "crowdb-tree/status.h"

#include <cstdint>
#include <memory>
#include <span>
#include <string>

namespace crowdb::tree
{

class Crowdbtree;
class Snapshot;
class RuntimeSnapshotImport;
class PublishedByteExport;

enum class snapshot_format : uint8_t {
    kPortable = 0, // versioned logical tuple stream
    kNative   = 1, // raw frame images and an immutable root descriptor
};

// Default export chunk size (fixed ≤1 MiB chunks).
inline constexpr size_t kSnapshotChunkBytes = 1U << 20;
// Per-window credit reserved for one foreign-language owner handle.
inline constexpr size_t kSnapshotChunkHandleBytes = 1024;

// A retained engine-owned chunk. The capacity stays charged until the last
// copy of owner is released, independently of the stream or Tree lifetime.
struct SnapshotChunk
{
    std::span<const uint8_t>          bytes;
    std::shared_ptr<const void>       owner;
    std::shared_ptr<AllocationBudget> allocation_budget = nullptr;
};

// A resumable export over an immutable point-in-time view. Runtime sources
// use bounded owned chunks and retain no whole serialized stream. A bounded
// preflight computes exact length and CRC before the first emitted chunk.
class SnapshotExport
{
  public:
    // Compatibility adapter for an already materialized caller-owned view.
    // Retain the immutable logical view and encode it
    // incrementally. The constructor performs a metadata-only pass for the
    // total length and CRC, but never materializes the serialized stream.
    SnapshotExport(std::shared_ptr<Snapshot> snapshot, size_t chunk_bytes);
    explicit SnapshotExport(std::shared_ptr<PublishedByteExport> runtime);

    // Compatibility adapter for an already encoded caller-owned stream.
    // Tree exports use the bounded PublishedByteExport constructor.
    SnapshotExport(std::string stream, size_t chunk_bytes, uint64_t at_slot)
        : stream_(std::move(stream)),
          chunk_bytes_(chunk_bytes == 0 ? kSnapshotChunkBytes : chunk_bytes),
          at_slot_(at_slot),
          total_bytes_(stream_.size())
    {
    }

    // Copy the next chunk into *out (<= chunk_bytes). Sets *done=true once the
    // final chunk has been returned (an empty stream returns one empty final
    // chunk).
    Status next_chunk(std::string *out, bool *done);
    // Independently owned capacity remains accounted across chunk copies.
    Status next_owned(SnapshotChunk *out, bool *done);

    [[nodiscard]] uint64_t at_slot() const
    {
        return at_slot_;
    }

    [[nodiscard]] size_t total_bytes() const
    {
        return total_bytes_;
    }

    [[nodiscard]] size_t chunk_bytes() const
    {
        return chunk_bytes_;
    }

    [[nodiscard]] uint32_t final_crc32c() const
    {
        return final_crc32c_;
    }

    [[nodiscard]] size_t offset() const
    {
        return pos_;
    }

  private:
    Status next_portable_chunk(std::string *out, bool *done);

    std::shared_ptr<Snapshot>            snapshot_;
    std::string                          stream_;
    size_t                               pos_ = 0;
    size_t                               chunk_bytes_;
    uint64_t                             at_slot_;
    size_t                               total_bytes_  = 0;
    uint32_t                             final_crc32c_ = 0;
    size_t                               piece_index_  = 0;
    size_t                               piece_offset_ = 0;
    std::shared_ptr<PublishedByteExport> runtime_;
    AllocationLease                      facade_credit_;
};

// Capture the published materialized frontier. Both formats preserve that
// fixed root while subsequent writes and checkpoints proceed.
Status snapshot_export_begin(Crowdbtree &tree, snapshot_format fmt, size_t chunk_bytes,
                             std::unique_ptr<SnapshotExport> *out);

// Convenience: dump a whole snapshot to a `.ctsnap` file (loops next_chunk).
Status snapshot_dump_to_file(Crowdbtree &tree, snapshot_format fmt, const std::string &path);

// Stage bounded native frames or portable tuples and atomically install the
// validated generation on finish, after checking the whole-stream CRC.
class SnapshotImport
{
  public:
    explicit SnapshotImport(Crowdbtree &tree);
    ~SnapshotImport();

    SnapshotImport(const SnapshotImport &)            = delete;
    SnapshotImport &operator=(const SnapshotImport &) = delete;
    SnapshotImport(SnapshotImport &&)                 = delete;
    SnapshotImport &operator=(SnapshotImport &&)      = delete;

    // feed the next chunk of bytes (order matters; chunks are concatenated).
    Status feed(Slice chunk);

    // Parse + verify the accumulated stream, then replace the engine state.
    // Returns the snapshot's slot via *out_at_slot (if non-null).
    Status finish(uint64_t *out_at_slot);

  private:
    std::shared_ptr<RuntimeSnapshotImport> runtime_;
    Crowdbtree                            &tree_;
};

// Convenience: load a `.ctsnap` file into `tree`.
Status snapshot_load_from_file(Crowdbtree &tree, const std::string &path);

} // namespace crowdb::tree
