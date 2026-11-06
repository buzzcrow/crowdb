// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/block_account.h"
#include "backend/local/local_backend_state.h"
#include "backend/local/local_block_gc.h"
#include "btree/allocation_bound.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "crowdb-common/log.h"

namespace crowdb::tree::checkpoint_detail
{
void reclaim_free_blocks(TreeCore &core, std::shared_ptr<detail::AllocationReservation> metadata,
                         std::shared_ptr<AllocationBudget> backend) noexcept
try {
    auto       *store = core.config().page_store;
    const auto &local = core.local_backend();
    const auto  unit  = store != nullptr ? local_block_gc_unit(*store) : 0;
    if (unit == 0 || !local || !local->ledger) {
        return;
    }
    const LocalBlockGcLimits limits;
    if (unit > limits.max_deleted_bytes) {
        return;
    }
    using namespace detail;
    const auto gc_bound = local_block_gc_metadata_bound(limits);
    if (!gc_bound) {
        return;
    }
    const auto bound = AllocationBound(gc_bound.value()).add(shared_allocation_bound<AllocationReservation>()).value();
    if (!bound) {
        return;
    }
    const bool owned_credit = !metadata;
    if (!metadata) {
        auto reserved = ResourceBudget::try_reserve(core.binding()->account(), BudgetClass::kReclaim, *bound);
        if (!reserved) {
            return;
        }
        metadata = AllocationReservation::create(std::move(*reserved));
    }

    struct Seal
    {
        std::shared_ptr<AllocationReservation> credit;
        std::shared_ptr<AllocationBudget>      backend;

        ~Seal()
        {
            seal_backend_budget(backend);
            if (credit) {
                credit->seal();
            }
        }
    } seal{.credit = owned_credit ? metadata : nullptr, .backend = {}};

    if (!backend) {
        auto status =
            reserve_backend_operation(store, core.binding()->account(),
                                      {.workers = core.binding()->backend_cpu()->worker_count()}, &seal.backend);
        if (!status.ok()) {
            return;
        }
        backend = seal.backend;
    }

    auto       cursor = local->gc_cursor.load(std::memory_order_relaxed);
    const auto result = collect_free_local_blocks(*store, *local->ledger, cursor, limits, metadata, backend);
    (void)local->gc_cursor.compare_exchange_strong(cursor, result.complete ? 0 : result.next_block,
                                                   std::memory_order_relaxed);
    if (!result.status.ok() && result.status.code() != Code::kResourceExhausted &&
        result.status.code() != Code::kUnavailable) {
        CRB_LOG_WARN("post-checkpoint block cleanup deferred after failure: {}", result.status.to_string());
    }
}
catch (const std::bad_alloc &) {
    // The durable checkpoint is already committed. Retained leases and the
    // cursor make a later bounded cleanup pass safe after allocation failure.
    return;
}
catch (const std::exception &error) {
    CRB_LOG_WARN("post-checkpoint block cleanup deferred after exception: {}", error.what());
}
} // namespace crowdb::tree::checkpoint_detail
