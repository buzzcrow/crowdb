// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "flush_batch.h"

namespace crowdb::tree
{
using FlushReportComplete = void (*)(void *, detail::CallbackSignal, Status, FlushResult);
using FlushReportSubmit   = void (*)(void *, FlushReportComplete, void *);

// The completed attempt owns its report independently of subsequent retries.
// Connecting adds no heap allocation; the caller owns the connected state.
class FlushReportSender
{
  public:
    using sender_concept = stdexec::sender_tag;
    using completion_signatures =
        stdexec::completion_signatures<stdexec::set_value_t(FlushResult), stdexec::set_error_t(Status),
                                       stdexec::set_stopped_t()>;

    FlushReportSender(void *source, FlushReportSubmit submit) : source_(source), submit_(submit)
    {
    }

    template <class Receiver> struct Operation
    {
        Operation(Receiver receiver, void *source, FlushReportSubmit submit)
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
        static void complete(void *context, detail::CallbackSignal signal, Status status, FlushResult report) noexcept
        {
            auto *self = static_cast<Operation *>(context);
            if (self->terminal_.test_and_set(std::memory_order_acq_rel)) {
                return;
            }
            switch (signal) {
            case detail::CallbackSignal::kValue:
                stdexec::set_value(std::move(self->receiver_), std::move(report));
                break;
            case detail::CallbackSignal::kError:
                stdexec::set_error(std::move(self->receiver_), std::move(status));
                break;
            case detail::CallbackSignal::kStopped:
                stdexec::set_stopped(std::move(self->receiver_));
                break;
            }
        }

        Receiver          receiver_;
        void             *source_;
        FlushReportSubmit submit_;
        std::atomic_flag  terminal_ = ATOMIC_FLAG_INIT;
    };

    template <class Receiver> [[nodiscard]] auto connect(Receiver receiver) const -> Operation<Receiver>
    {
        return {std::move(receiver), source_, submit_};
    }

  private:
    void             *source_;
    FlushReportSubmit submit_;
};

static_assert(stdexec::sender<FlushReportSender>);
} // namespace crowdb::tree
