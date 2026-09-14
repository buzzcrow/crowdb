// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/status.h"

#include <atomic>
#include <stdexec/execution.hpp>
#include <utility>

namespace crowdb::tree::detail
{

enum class CallbackSignal : std::uint8_t { kValue, kError, kStopped };

using CallbackComplete = void (*)(void *, CallbackSignal, Status);
using CallbackSubmit   = void (*)(void *, CallbackComplete, void *);

// Adapts callback transports to the standard sender contract. The operation
// state owns the receiver and is supplied directly as the callback context,
// so the adapter adds no promise state, std::function, or heap allocation.
// Transport adapters must invoke the completion exactly once, but the atomic
// terminal guard also contains a faulty duplicate callback.
class CallbackSender
{
  public:
    using sender_concept = stdexec::sender_tag;
    using completion_signatures =
        stdexec::completion_signatures<stdexec::set_value_t(), stdexec::set_error_t(Status), stdexec::set_stopped_t()>;

    CallbackSender(void *source, CallbackSubmit submit) : source_(source), submit_(submit)
    {
    }

    template <class Receiver> struct Operation
    {
        Operation(Receiver receiver, void *source, CallbackSubmit submit)
            : receiver_(std::move(receiver)),
              source_(source),
              submit_(submit)
        {
        }

        Operation(const Operation &)            = delete;
        Operation &operator=(const Operation &) = delete;
        Operation(Operation &&)                 = delete;
        Operation &operator=(Operation &&)      = delete;

        void start() & noexcept
        {
            submit_(source_, &Operation::complete, this);
        }

      private:
        static void complete(void *context, CallbackSignal signal, Status status) noexcept
        {
            auto *self = static_cast<Operation *>(context);
            if (self->terminal_.test_and_set(std::memory_order_acq_rel)) {
                return;
            }
            switch (signal) {
            case CallbackSignal::kValue:
                stdexec::set_value(std::move(self->receiver_));
                break;
            case CallbackSignal::kError:
                stdexec::set_error(std::move(self->receiver_), std::move(status));
                break;
            case CallbackSignal::kStopped:
                stdexec::set_stopped(std::move(self->receiver_));
                break;
            }
        }

        Receiver         receiver_;
        void            *source_;
        CallbackSubmit   submit_;
        std::atomic_flag terminal_ = ATOMIC_FLAG_INIT;
    };

    template <class Receiver>
    [[nodiscard]] [[nodiscard]] [[nodiscard]] auto connect(Receiver receiver) const -> Operation<Receiver>
    {
        return Operation<Receiver>{std::move(receiver), source_, submit_};
    }

  private:
    void          *source_;
    CallbackSubmit submit_;
};

static_assert(stdexec::sender<CallbackSender>);

} // namespace crowdb::tree::detail
