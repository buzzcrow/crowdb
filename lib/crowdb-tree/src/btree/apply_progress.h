// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"

namespace crowdb::tree
{
// Bounded wakeups for accepted apply operations waiting for a source, flush
// attempt or frontier transition. A wakeup carries no mutable operation report:
// the receiver rechecks its own predicate on the shared CPU runtime.
class ApplyProgress
{
    struct State;

  public:
    ~ApplyProgress();
    ApplyProgress(const ApplyProgress &)            = delete;
    ApplyProgress &operator=(const ApplyProgress &) = delete;

    class Request
    {
      public:
        Request(const ApplyProgress &progress, uint64_t observed_sequence);
        ~Request();
        Request(const Request &)            = delete;
        Request &operator=(const Request &) = delete;

        // The accepted operation retains this request and its connected state
        // through terminal. Cancellation is an event, not request destruction.
        [[nodiscard]] detail::CallbackSender sender() noexcept;
        void                                 request_stop() noexcept;

      private:
        friend struct State;
        friend class ApplyProgress;
        static void              submit(void *source, detail::CallbackComplete complete, void *context);
        std::shared_ptr<State>   state_;
        uint64_t                 observed_;
        std::atomic<bool>        pending_{false};
        std::atomic<bool>        stopped_{false};
        std::atomic_flag         submitted_ = ATOMIC_FLAG_INIT;
        detail::CallbackComplete complete_  = nullptr;
        void                    *context_   = nullptr;
    };

    static Status create(size_t capacity, const std::shared_ptr<detail::AllocationReservation> &metadata,
                         std::shared_ptr<ApplyProgress> *out);
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t capacity) noexcept;
    [[nodiscard]] uint64_t                     sequence() const noexcept;
    // Host-only wait; registers first and rechecks the observed sequence.
    [[nodiscard]] Status wait(uint64_t observed_sequence) const;
    void                 advance() noexcept;
    void                 close() noexcept;

  private:
    explicit ApplyProgress(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
