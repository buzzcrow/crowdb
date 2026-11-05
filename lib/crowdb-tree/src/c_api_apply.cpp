// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_apply_operation.h"
#include "btree/tree_core_access.h"
#include "btree/tree_core_batch.h"
#include "c_api_future_internal.h"
#include "c_api_internal.h"
#include "crowdb-common/log.h"
#include "crowdb-tree/apply_c_api.h"

using namespace crowdb::tree;

namespace
{
struct ApplyCall : std::enable_shared_from_this<ApplyCall>
{
    using Future = std::unique_ptr<ct_future_handle, decltype(&free_ct_future_handle)>;

    ApplyCall(std::shared_ptr<TreeApplyOperation> operation, std::shared_ptr<CompletionSignal> signal)
        : metadata(operation->allocation_budget()),
          operation(std::move(operation)),
          result(make_ct_future(metadata)),
          future(make_ct_future_handle(result), free_ct_future_handle),
          signal(std::move(signal))
    {
        result->kind = ct_future_impl::Kind::kApply;
    }

    static void completed(void *context, detail::CallbackSignal signal, Status status) noexcept
    {
        auto &call     = *static_cast<ApplyCall *>(context);
        auto  retained = std::move(call.self);
        call.result->status =
            static_cast<ct_status>(signal == detail::CallbackSignal::kStopped ? Code::kUnavailable : status.code());
        call.operation.reset();
        call.result->done.store(true, std::memory_order_release);
        c_api_notify_completion(call.signal);
    }

    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<TreeApplyOperation>            operation;
    ct_future_handle                               result;
    Future                                         future;
    std::shared_ptr<CompletionSignal>              signal;
    std::shared_ptr<ApplyCall>                     self;
    std::atomic_flag                               submitted = ATOMIC_FLAG_INIT;
};

struct ReplayInputOwner
{
    ReplayInputOwner(void (*release)(void *), void *argument) : drop(release), context(argument)
    {
    }

    ReplayInputOwner(const ReplayInputOwner &)            = delete;
    ReplayInputOwner &operator=(const ReplayInputOwner &) = delete;

    ~ReplayInputOwner()
    {
        if (drop != nullptr) {
            drop(context);
        }
    }

    AllocationLease control;
    void (*drop)(void *);
    void *context;
};

using PermitHandle = std::shared_ptr<ApplyCall>;

PermitHandle *handle(ct_apply_permit *permit) noexcept
{
    return reinterpret_cast<PermitHandle *>(permit);
}
} // namespace

int32_t ct_apply_prepare(ct_tree *tree, const ct_kv_ref *operations, size_t count, int32_t gap_repair,
                         ct_apply_permit **out)
try {
    if (tree == nullptr || out == nullptr || (count != 0 && operations == nullptr) ||
        (gap_repair != 0 && gap_repair != 1)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    *out       = nullptr;
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (core == nullptr) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    for (size_t index = 0; index < count; ++index) {
        const auto &input = operations[index];
        if ((input.key == nullptr && input.key_len != 0) || (input.value == nullptr && input.value_len != 0) ||
            input.kind > 1) {
            return static_cast<int32_t>(Code::kInvalidArgument);
        }
    }
    constexpr size_t retained = detail::shared_allocation_bound<ApplyCall>() +
                                detail::shared_allocation_bound<ct_future_impl>() + sizeof(ct_future_handle) +
                                sizeof(PermitHandle);
    std::shared_ptr<TreeApplyOperation> operation;
    auto                                status = core->prepare_apply(
        count,
        [operations](size_t index) {
            const auto &input = operations[index];
            return CoreWriteInput{.key   = Slice(input.key, input.key_len),
                                  .value = Slice(input.value, input.value_len),
                                  .flags = input.kind};
        },
        gap_repair != 0 ? detail::BudgetClass::kGapRepair : detail::BudgetClass::kAdmittedWriter, retained, &operation);
    if (!status.ok()) {
        if (status.code() == Code::kInvalidArgument) {
            CRB_LOG_WARN("apply admission rejected {} operations: {}", count, status.to_string());
        }
        return static_cast<int32_t>(status.code());
    }
    auto metadata = operation->allocation_budget();
    auto call     = detail::reserved_shared<ApplyCall>(metadata, std::move(operation), c_api_completion_signal(tree));
    detail::ReservationAllocator<PermitHandle> allocator(metadata);
    auto                                      *result = allocator.allocate(1);
    std::construct_at(result, std::move(call));
    *out = reinterpret_cast<ct_apply_permit *>(result);
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<int32_t>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int32_t>(Code::kInternal);
}

int32_t ct_apply_arm(ct_apply_permit *permit)
{
    if (permit == nullptr || !(*handle(permit))->operation) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    return static_cast<int32_t>((*handle(permit))->operation->arm().code());
}

int32_t ct_apply_submit(ct_apply_permit *permit, uint64_t slot, ct_future **out)
{
    if (permit == nullptr || out == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    *out      = nullptr;
    auto call = *handle(permit);
    if (call->submitted.test_and_set(std::memory_order_acq_rel)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    call->self = call;
    *out       = reinterpret_cast<ct_future *>(call->future.release());
    call->operation->start(slot, &ApplyCall::completed, call.get());
    return 0;
}

void ct_apply_permit_free(ct_apply_permit *permit)
{
    if (permit != nullptr) {
        auto                                      *owned = handle(permit);
        detail::ReservationAllocator<PermitHandle> allocator((*owned)->metadata);
        std::destroy_at(owned);
        allocator.deallocate(owned, 1);
    }
}

int32_t ct_apply_replay(ct_tree *tree, uint64_t slot, size_t count, ct_replay_read read, void (*drop_context)(void *),
                        void *context, size_t owner_control_bytes, ct_future **out)
try {
    ReplayInputOwner incoming{drop_context, context};
    if (tree == nullptr || out == nullptr || drop_context == nullptr || read == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    *out       = nullptr;
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (core == nullptr) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    for (size_t index = 0; index < count; ++index) {
        ct_kv_ref input{};
        read(context, index, &input);
        if ((input.key == nullptr && input.key_len != 0) || (input.value == nullptr && input.value_len != 0) ||
            input.kind > 1) {
            return static_cast<int32_t>(Code::kInvalidArgument);
        }
    }
    const auto retained = detail::AllocationBound(detail::shared_allocation_bound<ApplyCall>())
                              .add(detail::shared_allocation_bound<ReplayInputOwner>())
                              .add(detail::shared_allocation_bound<ct_future_impl>())
                              .add(sizeof(ct_future_handle))
                              .add(owner_control_bytes)
                              .value();
    if (!retained) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    std::shared_ptr<TreeApplyOperation> operation;
    auto                                status = core->prepare_replay_adopt(
        slot, count,
        [read, context](size_t index) {
            ct_kv_ref input{};
            read(context, index, &input);
            return CoreWriteInput{.key   = Slice(input.key, input.key_len),
                                  .value = Slice(input.value, input.value_len),
                                  .flags = input.kind};
        },
        [&incoming, owner_control_bytes](
            const std::shared_ptr<detail::AllocationReservation> &budget) -> std::shared_ptr<const void> {
            auto owner     = detail::reserved_shared<ReplayInputOwner>(budget, incoming.drop, incoming.context);
            incoming.drop  = nullptr;
            owner->control = AllocationLease(budget, owner_control_bytes, AllocationKind::kMetadata);
            return owner;
        },
        *retained, &operation);
    if (!status.ok()) {
        if (status.code() == Code::kInvalidArgument) {
            CRB_LOG_WARN("apply admission rejected {} operations: {}", count, status.to_string());
        }
        return static_cast<int32_t>(status.code());
    }
    auto metadata = operation->allocation_budget();
    auto call     = detail::reserved_shared<ApplyCall>(metadata, std::move(operation), c_api_completion_signal(tree));
    call->self    = call;
    *out          = reinterpret_cast<ct_future *>(call->future.release());
    call->operation->start(slot, &ApplyCall::completed, call.get());
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<int32_t>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int32_t>(Code::kInternal);
}
