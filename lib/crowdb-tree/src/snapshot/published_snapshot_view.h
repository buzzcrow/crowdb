// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/snapshot/snapshot.h"
#include "snapshot/published_snapshot_materialize.h"

#include <stdexcept>

namespace crowdb::tree
{
// Compatibility methods return references/bools and therefore report failure
// by exception. ABI adapters preserve this status without unwinding into FFI.
class SnapshotReadError final : public std::runtime_error
{
  public:
    explicit SnapshotReadError(const Status &status) : std::runtime_error(status.to_string()), code_(status.code())
    {
    }

    [[nodiscard]] Code code() const noexcept
    {
        return code_;
    }

  private:
    Code code_;
};

// Compatibility facade: point reads use the fixed immutable publication;
// whole-result methods explicitly materialize within a caller-supplied cap.
class PublishedSnapshotView final : public Snapshot
{
  public:
    static Status create(std::shared_ptr<PublishedSnapshotContext> context, size_t max_bytes,
                         std::shared_ptr<Snapshot> *out);
    ~PublishedSnapshotView() override;
    [[nodiscard]] const std::vector<leaf_entry> &entries() const override;
    [[nodiscard]] bool get(Slice key, uint64_t *out_slot, std::string *out_value) const override;

  private:
    PublishedSnapshotView(std::shared_ptr<PublishedSnapshotContext> context, size_t max_bytes);
    std::shared_ptr<PublishedSnapshotContext>   context_;
    size_t                                      max_bytes_;
    mutable std::atomic<SnapshotMaterialized *> materialized_{nullptr};
};

// Captures published L1 exactly as snapshot_view historically did: it never
// hands off the live source table or advances coverage.
Status create_published_snapshot(const std::shared_ptr<TreeCore> &core, std::shared_ptr<Snapshot> *out);
} // namespace crowdb::tree
