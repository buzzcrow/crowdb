// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_operation_internal.h"

namespace crowdb::tree
{
void FlushOperation::State::acquire_admission()
{
    auto request         = detail::reserved_shared<PublicationRequest>(metadata, metadata);
    request->transaction = {
        .record              = admission_record,
        .metadata            = metadata,
        .expected_commit_seq = expected_commit_seq,
        .flush_admission = FlushAdmissionPublication{.action = FlushAdmissionAction::Acquire, .owner = admission_owner},
        .flush_admission_owner = admission_owner
    };
    auto self        = shared_from_this();
    request->on_done = [self](PublicationResult result) noexcept {
        if (!result.status.ok()) {
            self->fail(std::move(result.status));
            return;
        }
        self->admitted = true;
        self->dispatch(CpuStage::kBegin);
    };
    if (!publisher.try_submit(request, options.publication)) {
        fail(Status::resource_exhausted("flush admission queue full or closed"));
    }
}

void FlushOperation::State::release_admission(Status status)
{
    // This descriptor and root credit were retained before admission. Source
    // preparation cannot consume the capacity needed to release the gate.
    if (!release_request) {
        release_request              = detail::reserved_shared<PublicationRequest>(release_metadata, release_metadata);
        release_request->transaction = {
            .record = detail::reserved_shared<InstallRecord>(
                release_metadata, InstallToken{.generation = admission_record->token.generation,
                                               .batch      = admission_owner,
                                               .range      = UINT64_MAX - 3,
                                               .sequence   = 1}
                 ),
            .metadata = release_metadata,
            .flush_admission =
                FlushAdmissionPublication{.action = FlushAdmissionAction::Release, .owner = admission_owner},
            .flush_admission_owner = admission_owner
        };
    }
    report.handoff_status = std::move(status);
    auto request          = std::move(release_request);
    auto self             = shared_from_this();
    request->on_done      = [self](PublicationResult result) noexcept {
        if (result.status.ok()) {
            self->admitted           = false;
            self->admission_released = true;
            self->fail_terminal(std::move(self->report.handoff_status));
        }
        else {
            self->fail_terminal(std::move(result.status));
        }
    };
    if (!publisher.try_submit(request, options.publication)) {
        release_request          = std::move(request);
        release_request->on_done = {};
        fail_terminal(Status::resource_exhausted("flush admission release queue full or closed"));
    }
}
} // namespace crowdb::tree
