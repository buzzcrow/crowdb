// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Snapshot export / import.
//
// The streaming byte form is the primitive: it feeds the network snapshot
// service for new-member install without ever touching local disk. Dumping to a
// `.ctsnap` file is just streaming into a file writer.
//
// v1 ships the **portable** format only: a versioned header, then key-sorted
// `(klen,key,slot,kind,vlen,value)` tuples (including tombstones), then a
// whole-stream CRC32C. It is deterministic and engine-independent, so an export
// re-imports identically on any backend / page size and is comparable against
// the in-mem oracle. The native frame-dump format is deferred.
//
// Key work: portable stream encode/decode, chunked export, CRC-checked import,
// file convenience wrappers.
#pragma once

#include "crowdb-tree/slice.h"
#include "crowdb-tree/status.h"

#include <cstdint>
#include <memory>
#include <string>

namespace crowdb::tree
{

class Crowdbtree;
class Snapshot;

enum class snapshot_format : uint8_t {
    kPortable = 0, // v1 default: portable tuple stream
    kNative   = 1, // deferred: raw frame images + remapped manifest
};

// default_env export chunk size (fixed ≤1 MiB chunks).
inline constexpr size_t kSnapshotChunkBytes = 1U << 20;

// A resumable, chunked export over an immutable point-in-time view. The whole
// stream is materialized once at begin (deterministic), then sliced into chunks
// of at most `chunk_bytes`, so chunk boundaries are stable across exports.
class SnapshotExport
{
  public:
    // Portable export: retain the immutable logical view and encode it
    // incrementally. The constructor performs a metadata-only pass for the
    // total length and CRC, but never materializes the serialized stream.
    SnapshotExport(std::shared_ptr<Snapshot> snapshot, size_t chunk_bytes);

    // Native export still uses an owned encoded stream. Native incremental
    // frame export is separate work from the portable install protocol.
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

    std::shared_ptr<Snapshot> snapshot_;
    std::string               stream_;
    size_t                    pos_ = 0;
    size_t                    chunk_bytes_;
    uint64_t                  at_slot_;
    size_t                    total_bytes_  = 0;
    uint32_t                  final_crc32c_ = 0;
    size_t                    piece_index_  = 0;
    size_t                    piece_offset_ = 0;
};

// Begin a snapshot export of `tree`: exports the current durable view (at the
// engine's last_applied_slot, recorded in the stream header). Both kPortable
// and kNative (plan-tree #16) are supported. (Historical/arbitrary-slot
// export is deferred until path-copy COW RootVersions exist; there is no
// slot selector.)
Status snapshot_export_begin(Crowdbtree &tree, snapshot_format fmt, size_t chunk_bytes,
                             std::unique_ptr<SnapshotExport> *out);

// Convenience: dump a whole snapshot to a `.ctsnap` file (loops next_chunk).
Status snapshot_dump_to_file(Crowdbtree &tree, snapshot_format fmt, const std::string &path);

// Accumulates a portable stream and atomically installs it into `tree` on
// finish (verifying the whole-stream CRC first).
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
    struct PortableState;

    Status                         finish_native(const uint8_t *p, size_t len, uint64_t *out_at_slot);
    Crowdbtree                    &tree_;
    std::unique_ptr<PortableState> portable_;
    std::string                    buf_;
    bool                           format_selected_ = false;
    bool                           native_          = false;
};

// Convenience: load a `.ctsnap` file into `tree`.
Status snapshot_load_from_file(Crowdbtree &tree, const std::string &path);

} // namespace crowdb::tree
