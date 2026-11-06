// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "runtime_range_rebuild.h"

#include "portable_import.h"
#include "snapshot/recovered_state.h"
#include "tree_core_access.h"

namespace crowdb::tree
{
Status reserve_range_shape(TreeCore &destination, const NativeImportShape &shape, bool portable,
                           RuntimeRangeImport &staging)
{
    const auto loads = resident_import_load_limits(destination.load_limits(), shape.max_frame_bytes);
    const auto depth = std::min<size_t>(64, shape.primary_pages);
    auto       bound =
        detail::AllocationBound()
            .add(native_import_allocation_bound(shape))
            .add(RecoveredStateOperation::allocation_bound(shape.frames, shape.primary_pages, shape.max_key_bytes))
            .add(AsyncStorePageLoader::primary_reserve_bound(loads, depth, destination.binding()->cpu()->worker_count(),
                                                             1))
            .add(detail::shared_allocation_bound<detail::AllocationReservation>())
            .add(TreeOperationLifetime::ticket_allocation_bound());
    if (portable) {
        bound.add(PortableImportBuilder::allocation_bound(shape));
    }
    if (!bound.value()) {
        return Status::resource_exhausted("range import allocation bound overflow");
    }
    return destination.prepare_import_credit(*bound.value(), &staging.credit);
}

Status rebuild_range_runtime(Crowdbtree &source, const KeyRange &range, Config destination_options,
                             std::unique_ptr<Crowdbtree> *out, RangeRebuildStats *stats)
try {
    auto *source_core = TreeCoreAccess::get(source);
    if (source_core == nullptr) {
        return Status::not_supported("runtime range rebuild requires a runtime source");
    }
    if (out == nullptr) {
        return Status::invalid_argument("range rebuild needs an output");
    }
    // A new binding supplies the destination's independent account. Neither
    // its page directory nor any mutable source catalog is inherited.
    destination_options.key_range = range;
    std::unique_ptr<Crowdbtree> destination;
    auto                        status = Crowdbtree::open(destination_options, &destination);
    if (!status.ok()) {
        return status;
    }
    auto              *core = TreeCoreAccess::get(*destination);
    RuntimeRangeImport staging;
    if (!range.is_bounded() && !source_core->config().key_range.is_bounded() &&
        source_core->config().frame_bytes == destination_options.frame_bytes) {
        status = copy_native_range(source, *core, staging);
    }
    else {
        status = copy_portable_range(*source_core, *core, range, staging);
    }
    if (!status.ok()) {
        return status;
    }
    status = core->install_readable(std::move(staging.catalog), staging.credit);
    if (!status.ok()) {
        return status;
    }
    status = destination->snapshot();
    if (!status.ok()) {
        return status;
    }
    if (stats != nullptr) {
        *stats = staging.stats;
    }
    *out = std::move(destination);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("range rebuild staging allocation failed");
}
} // namespace crowdb::tree
