// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/btree/range_rebuild.h"
#include "tree_core.h"

namespace crowdb::tree
{
struct RuntimeRangeImport
{
    ~RuntimeRangeImport()
    {
        if (credit) {
            credit->seal();
        }
    }

    std::shared_ptr<detail::AllocationReservation> credit;
    std::shared_ptr<ReadablePageCatalog>           catalog;
    RangeRebuildStats                              stats;
};

struct NativeImportShape;
Status reserve_range_shape(TreeCore &destination, const NativeImportShape &shape, bool portable,
                           RuntimeRangeImport &staging);
Status copy_snapshot_range(const std::shared_ptr<PublishedSnapshotContext> &snapshot, TreeCore &destination,
                           const KeyRange &range, RuntimeRangeImport &staging);
Status copy_native_range(Crowdbtree &source, TreeCore &destination, RuntimeRangeImport &staging);
Status copy_portable_range(TreeCore &source, TreeCore &destination, const KeyRange &range, RuntimeRangeImport &staging);
Status rebuild_range_runtime(Crowdbtree &source, const KeyRange &range, Config destination_options,
                             std::unique_ptr<Crowdbtree> *out, RangeRebuildStats *stats);
} // namespace crowdb::tree
