// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "native_import.h"
#include "portable_import.h"
#include "portable_import_plan.h"
#include "runtime_range_rebuild.h"
#include "snapshot/operation_wait.h"
#include "snapshot/recovered_state.h"
#include "tree_core_internal.h"

namespace crowdb::tree
{
namespace
{
struct SealImport
{
    std::shared_ptr<detail::AllocationReservation> credit;

    ~SealImport()
    {
        if (credit) {
            credit->seal();
        }
    }
};
} // namespace

Status TreeCore::prepare_import_credit(std::shared_ptr<detail::AllocationReservation> *out)
{
    return prepare_import_credit(SIZE_MAX, out);
}

Status TreeCore::prepare_import_credit(size_t requested, std::shared_ptr<detail::AllocationReservation> *out)
try {
    if (out == nullptr || *out) {
        return Status::invalid_argument();
    }
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("Tree import ingress closed or replacing");
    }
    // Diagnostic reserved/allocated samples are not coherent during credit
    // conversion. Plan from admission usage and include shared host pressure.
    const auto available = detail::ResourceBudget::available(binding()->account(), detail::BudgetClass::kReclaim);
    if (requested == SIZE_MAX) {
        requested = available;
    }
    if (requested > available) {
        return Status::resource_exhausted("import staging exceeds available reclaim capacity");
    }
    auto bytes = detail::ResourceBudget::try_reserve(binding()->account(), detail::BudgetClass::kReclaim, requested);
    if (!bytes) {
        return Status::resource_exhausted("import staging admission unavailable");
    }
    *out = detail::AllocationReservation::create(std::move(*bytes));
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("import admission allocation failed");
}

Status TreeCore::install_readable(std::shared_ptr<const ReadablePageCatalog>            catalog,
                                  const std::shared_ptr<detail::AllocationReservation> &credit)
{
    std::shared_ptr<const PublishedTreeState> candidate;
    auto                                      status = prepare_readable(std::move(catalog), credit, &candidate);
    return status.ok() ? install_generation(std::move(candidate)) : status;
}

Status TreeCore::prepare_readable(std::shared_ptr<const ReadablePageCatalog>            catalog,
                                  const std::shared_ptr<detail::AllocationReservation> &credit,
                                  std::shared_ptr<const PublishedTreeState>            *out)
try {
    SealImport seal{credit};
    if (!catalog || !credit || credit->account() != binding()->account() || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    std::shared_ptr<const PublishedTreeState> candidate;
    {
        State::Ingress ingress(*state_);
        if (ingress.owner == nullptr) {
            return Status::unavailable("Tree import ingress closed or replacing");
        }
        auto io = detail::ResourceBudget::try_reserve(binding()->account(), detail::BudgetClass::kIo, 1);
        if (!io) {
            return Status::resource_exhausted("import validation IO admission unavailable");
        }
        auto       io_credit = detail::reserved_shared<detail::AllocationReservation>(credit, std::move(*io));
        SealImport seal_io{io_credit};
        std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
        auto                                           status = state_->lifetime->acquire(credit, &ticket);
        if (!status.ok()) {
            return status;
        }
        size_t max_frame_bytes = 0;
        bool   all_resident    = true;
        for (const auto &[pid, frame] : catalog->frames) {
            (void)pid;
            max_frame_bytes = std::max<size_t>(max_frame_bytes, frame.frame_bytes);
            all_resident    = all_resident && frame.backing && frame.backing->storage == BackingStorage::kResidentFrame;
        }
        auto loads = all_resident ? resident_import_load_limits(state_->loads,
                                                                std::max<size_t>(max_frame_bytes, config().frame_bytes))
                                  : state_->loads;
        auto loader = detail::reserved_shared<AsyncStorePageLoader>(
            credit, ticket->backend(async_backend()), binding()->cpu(), binding()->lane(), loads,
            OverflowBackingResolver{}, PageLoadBudget{.bytes = credit, .io = io_credit});
        std::shared_ptr<RecoveredStateOperation> operation;
        status = RecoveredStateOperation::create_readable(catalog, std::move(loader), binding()->cpu(),
                                                          binding()->lane(), {}, credit, &operation);
        if (!status.ok()) {
            return status;
        }
        status = detail::wait_snapshot_operation(operation);
        if (!status.ok()) {
            return status;
        }
        candidate = operation->result().state;
    }
    // The candidate retains exact page dependencies; discard the staging catalog
    // before returning unused admission and reserving replacement controls.
    catalog.reset();
    credit->seal();
    *out = std::move(candidate);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("import validation allocation failed");
}

Status TreeCore::install_native(std::vector<NativeFrame> frames, NativeStreamInfo info)
try {
    SealImport budget;
    auto       status = prepare_import_credit(&budget.credit);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<ReadablePageCatalog> catalog;
    status = build_native_catalog(frames, info, config(), state_->loads, {}, budget.credit, &catalog);
    return status.ok() ? install_readable(std::move(catalog), budget.credit) : status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("native import staging allocation failed");
}

Status TreeCore::install_portable(std::vector<leaf_entry> entries, uint64_t coverage)
try {
    PortableImportPlan plan(config(), state_->loads);
    for (const auto &entry : entries) {
        const CellView cell{Slice(entry.cell)};
        if (!cell.valid()) {
            return Status::invalid_argument("portable import cell has no complete header");
        }
        const auto status = plan.append(entry.key.size(), cell.value().size(), cell.is_tombstone());
        if (!status.ok()) {
            return status;
        }
    }
    const auto shape = plan.finish();
    if (!shape) {
        return Status::resource_exhausted("portable import shape exceeds its bounded frame namespace");
    }
    RuntimeRangeImport budget;
    auto               status = reserve_range_shape(*this, *shape, true, budget);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<ReadablePageCatalog> catalog;
    {
        PortableImportBuilder builder(config(), state_->loads, coverage, budget.credit);
        for (const auto &entry : entries) {
            status = builder.append(Slice(entry.key), Slice(entry.cell));
            if (!status.ok()) {
                return status;
            }
        }
        status = builder.finish(&catalog);
    }
    return status.ok() ? install_readable(std::move(catalog), budget.credit) : status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("portable import staging allocation failed");
}
} // namespace crowdb::tree
