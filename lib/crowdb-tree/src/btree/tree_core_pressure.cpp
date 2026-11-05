// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "tree_core_checkpoint_internal.h"

namespace crowdb::tree::checkpoint_detail
{
namespace
{
std::optional<size_t> control_bound() noexcept
{
    using namespace detail;
    return AllocationBound(CheckpointOperation::control_allocation_bound())
        .add(shared_allocation_bound<Preparation>())
        .add(shared_allocation_bound<AsyncWork>())
        .add_product(2, shared_allocation_bound<Credits>())
        .add(shared_allocation_bound<AllocationReservation>())
        .value();
}

size_t finish_bound() noexcept
{
    return CheckpointOperation::finish_allocation_bound() +
           detail::shared_allocation_bound<detail::AllocationReservation>();
}
} // namespace

std::optional<size_t> pressure_control_bound() noexcept
{
    return detail::AllocationBound().add(control_bound()).add(finish_bound()).value();
}

Status prepare_pressure(TreeCore &core, std::shared_ptr<TreeOperationLifetime::Ticket> ticket, uint64_t leaf,
                        detail::ResourceBudget::Credit credit, std::shared_ptr<ApplyDispatcher> cpu_progress,
                        std::shared_ptr<PublicationLane> publication, std::shared_ptr<ApplyProgress> progress,
                        std::shared_ptr<ApplyWaiters> waiters, PageMaintenanceOperationOptions structure_options,
                        std::shared_ptr<CheckpointOperation> *out)
{
    using namespace detail;
    const auto required = pressure_control_bound();
    const auto controls = control_bound();
    if (!required || !controls || !ticket || !cpu_progress || !publication || !progress || out == nullptr || *out ||
        (!core.local_backend() && !core.chunk_backend()) || credit.amount() != *required || credit.allocated() ||
        !credit.belongs_to(core.binding()->account(), BudgetClass::kReclaim)) {
        return Status::invalid_argument("pressure writeback requires its prepaid control and owned local backend");
    }
    auto control = credit.split(*controls);
    if (!control) {
        return Status::internal_error("pressure control split failed");
    }
    auto metadata                                   = AllocationReservation::create(std::move(*control));
    auto preparation                                = reserved_shared<Preparation>(metadata);
    preparation->control                            = metadata;
    preparation->finish                             = AllocationReservation::create(std::move(credit));
    preparation->ticket                             = std::move(ticket);
    preparation->core                               = &core;
    preparation->options                            = defaults(core);
    preparation->options.mode                       = CheckpointMode::Writeback;
    preparation->options.publication                = std::move(publication);
    preparation->options.write.frames.selected_page = leaf;
    preparation->structure_options                  = std::move(structure_options);
    preparation->cpu_progress                       = std::move(cpu_progress);
    preparation->waiters                            = std::move(waiters);
    auto           work                             = reserved_shared<AsyncWork>(metadata, preparation);
    PageLoadBudget budget{.bytes = metadata, .io = {}};
    budget.cpu_progress = preparation->cpu_progress;
    return CheckpointOperation::create(
        preparation, core.publisher(), core.local_backend(), preparation->ticket->backend(core.async_backend()),
        core.binding()->cpu(), core.binding()->lane(), preparation->options, std::move(budget), preparation->finish, {},
        {}, {}, out, {},
        [work](const PublishedTreeState &view, const CapturedWritePlan *plan, CheckpointWork *output) {
            return work->prepare(view, plan, output);
        },
        {}, [work] { work->cancel(); }, core.chunk_backend(),
        [preparation](std::shared_ptr<const PublishedTreeState> view, uint64_t pid,
                      std::shared_ptr<PageSplitOperation> *operation) {
            return preparation->structure(std::move(view), pid, operation);
        });
}
} // namespace crowdb::tree::checkpoint_detail
