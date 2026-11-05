// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_core_internal.h"
#include "btree/tree_core_read_internal.h"
#include "host_completion.h"

namespace crowdb::tree
{
namespace
{
struct CoreGet : std::enable_shared_from_this<CoreGet>
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        CoreGet *owner;

        void set_value() const noexcept
        {
            owner->finish(owner->read->result().status);
        }

        void set_error(Status status) const noexcept
        {
            owner->finish(std::move(status));
        }

        void set_stopped() const noexcept
        {
            owner->finish(Status::unavailable("read stopped"));
        }
    };

    using Operation = detail::CallbackSender::Operation<Receiver>;
    core_read_detail::ReadPreparation    prepared;
    std::shared_ptr<PublishedGet>        read;
    std::function<void(Status, GetView)> complete;
    std::shared_ptr<Operation>           operation;
    std::shared_ptr<CoreGet>             owner;

    void finish(Status status) noexcept
    {
        auto    self = std::move(owner);
        GetView result;
        if (status.ok() && read->result().found) {
            const auto &cell = read->result().cell;
            result           = GetView::from_owner(cell.slot, cell.value, cell.owner);
        }
        detail::seal_backend_budget(prepared.backend);
        prepared.io->seal();
        prepared.credit->seal();
        try {
            complete(std::move(status), std::move(result));
        }
        catch (...) {
            // Completion is terminal; a throwing consumer cannot be retried.
            return;
        }
    }

    static void start(void *context) noexcept
    {
        auto *self = static_cast<CoreGet *>(context);
        auto  keep = self->owner;
        stdexec::start(*self->operation);
    }
};
} // namespace

void TreeCore::get_async(Slice key, std::function<void(Status, GetView)> complete)
{
    get_impl(key, std::move(complete), true);
}

void TreeCore::get_impl(Slice key, std::function<void(Status, GetView)> complete, bool dispatch)
{
    if (!complete) {
        return;
    }
    std::shared_ptr<CoreGet> operation;
    Status                   status;
    bool                     outside_range = false;
    try {
        State::Ingress                    ingress(*state_);
        core_read_detail::ReadPreparation prepared;
        outside_range = ingress.owner != nullptr && !state_->config.key_range.contains(key);
        if (ingress.owner == nullptr) {
            status = Status::unavailable("Tree closed");
        }
        else if (!outside_range) {
            status = core_read_detail::prepare(*this, false,
                                               detail::shared_allocation_bound<CoreGet>() +
                                                   detail::shared_allocation_bound<CoreGet::Operation>(),
                                               0, &prepared);
        }
        if (status.ok() && !outside_range) {
            operation           = detail::reserved_shared<CoreGet>(prepared.credit);
            operation->prepared = std::move(prepared);
            auto &ready         = operation->prepared;
            status              = PublishedGet::create(ready.view, ready.loader, key, ready.limits, &operation->read);
        }
        if (status.ok() && !outside_range) {
            operation->operation =
                detail::reserved_construct<CoreGet::Operation>(operation->prepared.credit, [&](auto *storage) {
                    ::new (static_cast<void *>(storage)) CoreGet::Operation(
                        stdexec::connect(operation->read->sender(), CoreGet::Receiver{operation.get()}));
                });
        }
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted("read allocation failed");
    }
    if (!status.ok() || outside_range) {
        complete(std::move(status), {});
        return;
    }
    operation->complete = std::move(complete);
    operation->owner    = operation;
    // The synchronous host boundary already has a caller thread. Starting its
    // bounded read inline avoids a worker round trip for resident data; cold
    // loads retain the same asynchronous loader and operation tickets.
    if (!dispatch) {
        CoreGet::start(operation.get());
        return;
    }
    status = operation->prepared.binding->cpu()->try_submit(operation->prepared.binding->lane(), &CoreGet::start,
                                                            operation.get());
    if (!status.ok()) {
        operation->finish(std::move(status));
    }
}

Status TreeCore::get(Slice key, GetView *out)
{
    if (out == nullptr) {
        return Status::invalid_argument("point read needs output");
    }
    detail::HostCompletion done;
    Status                 status;
    get_impl(
        key,
        [&](Status outcome, GetView result) {
            status = std::move(outcome);
            *out   = std::move(result);
            done.signal();
        },
        false);
    done.wait();
    return status;
}
} // namespace crowdb::tree
