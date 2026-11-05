// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_account.h"
#include "btree/tree_core_access.h"
#include "btree/tree_core_batch.h"
#include "c_api_internal.h"
#include "c_api_owned_allocation.h"

namespace
{
using namespace crowdb::tree;
using Values      = std::vector<buffer, MetadataAllocator<buffer>>;
using OwnedReader = detail::InlineFunction<ct_owned_kv_ref(size_t)>;

Status stage_values(const OwnedReader &read, Values &values)
{
    for (size_t index = 0; index < values.size(); ++index) {
        const auto input = read(index);
        if ((input.key_len != 0 && input.key == nullptr) || input.kind > 1) {
            return Status::invalid_argument();
        }
        if (input.kind == 1) {
            if (input.owner != nullptr || input.offset != 0 || input.length != 0) {
                return Status::invalid_argument();
            }
            continue;
        }
        if (input.owner == nullptr) {
            return Status::invalid_argument();
        }
        auto status = input.owner->bridge->slice(input.offset, input.length, &values[index]);
        if (!status.ok()) {
            return status;
        }
    }
    return {};
}
ct_status apply_owned(ct_tree *tree, uint64_t slot, size_t count, const OwnedReader &read)
try {
    auto *facade = c_api_tree(tree);
    auto *core   = facade == nullptr ? nullptr : TreeCoreAccess::get(*facade);
    if (core == nullptr) {
        return static_cast<int>(Code::kUnavailable);
    }
    std::shared_ptr<detail::AllocationAccount> budget;
    auto                                       status =
        detail::AllocationAccount::create(core->binding()->account(), detail::BudgetClass::kAdmittedWriter, &budget);
    if (!status.ok()) {
        return static_cast<int>(status.code());
    }
    Values values(count, MetadataAllocator<buffer>(budget));
    status = stage_values(read, values);
    if (!status.ok()) {
        return static_cast<int>(status.code());
    }
    status = core->apply_external_inputs(slot, count, [&](size_t index) {
        const auto input = read(index);
        return CoreWriteInput{.key     = Slice(input.key, input.key_len),
                              .value   = values[index].slice(),
                              .movable = &values[index],
                              .flags   = input.kind,
                              .encoded = false};
    });
    return static_cast<int>(status.code());
}
catch (const std::bad_alloc &) {
    return static_cast<int>(crowdb::tree::Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int>(crowdb::tree::Code::kInternal);
}

} // namespace

extern "C" ct_status ct_apply_batch_owned(ct_tree *tree, uint64_t slot, const ct_owned_kv_ref *operations, size_t count)
{
    if (count != 0 && operations == nullptr) {
        return static_cast<int>(crowdb::tree::Code::kInvalidArgument);
    }
    return apply_owned(tree, slot, count, [operations](size_t index) { return operations[index]; });
}

extern "C" ct_status ct_apply_batch_owned_reader(ct_tree *tree, uint64_t slot, size_t count, ct_owned_read_fn read,
                                                 const void *context)
{
    if (count != 0 && read == nullptr) {
        return static_cast<int>(crowdb::tree::Code::kInvalidArgument);
    }
    return apply_owned(tree, slot, count, [read, context](size_t index) { return read(context, index); });
}
