// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_core_batch.h"
#include "tree_core_internal.h"

namespace crowdb::tree
{
Status TreeCore::apply(uint64_t slot, const Batch &batch)
try {
    State::Ingress admission(*state_);
    if (admission.owner == nullptr) {
        return Status::unavailable("Tree ingress closed");
    }
    auto status = apply_source_batch(
        state_->config, *state_->sources, *state_->frontier, *state_->lifetime, state_->loads.max_value_bytes, slot,
        state_->automatic_slot, batch.ops.size(),
        [&](size_t index) {
            const auto &operation = batch.ops[index];
            return CoreWriteInput{.key     = Slice(operation.key),
                                  .value   = operation.kind == OpKind::kDelete ? Slice{} : Slice(operation.value),
                                  .movable = nullptr,
                                  .flags   = static_cast<uint8_t>(operation.kind),
                                  .encoded = false};
        },
        state_->apply_progress.get(), state_->source_rotation.get());
    if (status.ok()) {
        state_->apply_progress->advance();
        state_->source_rotation->wait(state_->source_rotation->request());
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("Tree apply allocation failed");
}

Status TreeCore::apply_auto(const Batch &batch)
try {
    State::Ingress admission(*state_);
    if (admission.owner == nullptr) {
        return Status::unavailable("Tree ingress closed");
    }
    auto status = apply_source_batch(
        state_->config, *state_->sources, *state_->frontier, *state_->lifetime, state_->loads.max_value_bytes,
        std::nullopt, state_->automatic_slot, batch.ops.size(),
        [&](size_t index) {
            const auto &operation = batch.ops[index];
            return CoreWriteInput{.key     = Slice(operation.key),
                                  .value   = operation.kind == OpKind::kDelete ? Slice{} : Slice(operation.value),
                                  .movable = nullptr,
                                  .flags   = static_cast<uint8_t>(operation.kind),
                                  .encoded = false};
        },
        state_->apply_progress.get(), state_->source_rotation.get());
    if (status.ok()) {
        state_->apply_progress->advance();
        state_->source_rotation->wait(state_->source_rotation->request());
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("Tree apply allocation failed");
}

Status TreeCore::apply_encoded(uint64_t slot, std::vector<Crowdbtree::encoded_op> operations)
try {
    State::Ingress admission(*state_);
    if (admission.owner == nullptr) {
        return Status::unavailable("Tree ingress closed");
    }
    auto status = apply_source_batch(
        state_->config, *state_->sources, *state_->frontier, *state_->lifetime, state_->loads.max_value_bytes, slot,
        state_->automatic_slot, operations.size(),
        [&](size_t index) {
            auto &operation = operations[index];
            return CoreWriteInput{.key     = Slice(operation.key),
                                  .value   = operation.cell.slice(),
                                  .movable = &operation.cell,
                                  .flags   = 0,
                                  .encoded = true};
        },
        state_->apply_progress.get(), state_->source_rotation.get());
    if (status.ok()) {
        state_->apply_progress->advance();
        state_->source_rotation->wait(state_->source_rotation->request());
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("Tree apply allocation failed");
}

Status TreeCore::apply_external(uint64_t slot, std::vector<Crowdbtree::external_op> operations)
{
    return apply_external_inputs(slot, operations.size(), [&](size_t index) {
        auto &operation = operations[index];
        return CoreWriteInput{.key     = Slice(operation.key),
                              .value   = operation.value.slice(),
                              .movable = &operation.value,
                              .flags   = operation.flags,
                              .encoded = false};
    });
}

Status TreeCore::apply_external_inputs(uint64_t slot, size_t count, const CoreWriteReader &read)
try {
    State::Ingress admission(*state_);
    if (admission.owner == nullptr) {
        return Status::unavailable("Tree ingress closed");
    }
    auto status = apply_source_batch(state_->config, *state_->sources, *state_->frontier, *state_->lifetime,
                                     state_->loads.max_value_bytes, slot, state_->automatic_slot, count, read,
                                     state_->apply_progress.get(), state_->source_rotation.get());
    if (status.ok()) {
        state_->apply_progress->advance();
        state_->source_rotation->wait(state_->source_rotation->request());
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("Tree apply allocation failed");
}

Status TreeCore::complete_noop(uint64_t slot)
{
    return apply(slot, {});
}

void TreeCore::force_advance_slot(uint64_t slot) noexcept
{
    State::Ingress admission(*state_);
    if (admission.owner != nullptr) {
        state_->frontier->force_advance(slot);
        state_->apply_progress->advance();
    }
}
} // namespace crowdb::tree
