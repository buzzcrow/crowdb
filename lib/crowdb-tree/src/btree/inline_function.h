// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <array>
#include <cstddef>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

namespace crowdb::tree::detail
{
template <class Signature, size_t Capacity = 64> class InlineFunction;

// Publication continuations live inside their already budgeted request. There
// is no heap fallback; oversized or throwing-move closures fail to compile.
template <class R, class... Args, size_t Capacity> class InlineFunction<R(Args...), Capacity>
{
    struct Operations
    {
        R (*invoke)(void *, Args &&...);
        void (*copy)(void *, const void *);
        void (*move)(void *, void *) noexcept;
        void (*destroy)(void *) noexcept;
    };

    template <class F>
    static constexpr bool accepts =
        sizeof(std::decay_t<F>) <= Capacity && alignof(std::decay_t<F>) <= alignof(std::max_align_t) &&
        std::is_copy_constructible_v<std::decay_t<F>> && std::is_nothrow_move_constructible_v<std::decay_t<F>> &&
        std::is_invocable_r_v<R, std::decay_t<F> &, Args...>;

  public:
    InlineFunction() = default;

    template <class F>
        requires(!std::is_same_v<std::decay_t<F>, InlineFunction>) && accepts<F>
    InlineFunction(F &&function)
    {
        using Callable = std::decay_t<F>;
        if constexpr (std::is_pointer_v<Callable> || std::is_member_pointer_v<Callable>) {
            if (function == nullptr) {
                return;
            }
        }
        static const Operations operations{
            .invoke = [](void *value, Args &&...args) -> R {
                return std::invoke(*std::launder(reinterpret_cast<Callable *>(value)), std::forward<Args>(args)...);
            },
            .copy =
                [](void *destination, const void *source) {
                    std::construct_at(reinterpret_cast<Callable *>(destination),
                                      *std::launder(reinterpret_cast<const Callable *>(source)));
                },
            .move =
                [](void *destination, void *source) noexcept {
                    auto *value = std::launder(reinterpret_cast<Callable *>(source));
                    std::construct_at(reinterpret_cast<Callable *>(destination), std::move(*value));
                    std::destroy_at(value);
                },
            .destroy =
                [](void *value) noexcept { std::destroy_at(std::launder(reinterpret_cast<Callable *>(value))); }};
        std::construct_at(reinterpret_cast<Callable *>(storage_.data()), std::forward<F>(function));
        operations_ = &operations;
    }

    InlineFunction(const InlineFunction &other)
    {
        if (other.operations_ != nullptr) {
            other.operations_->copy(storage_.data(), other.storage_.data());
            operations_ = other.operations_;
        }
    }

    InlineFunction(InlineFunction &&other) noexcept
    {
        if (other.operations_ != nullptr) {
            other.operations_->move(storage_.data(), other.storage_.data());
            operations_ = std::exchange(other.operations_, nullptr);
        }
    }

    InlineFunction &operator=(const InlineFunction &other)
    {
        if (this != &other) {
            InlineFunction copy(other);
            *this = std::move(copy);
        }
        return *this;
    }

    InlineFunction &operator=(InlineFunction &&other) noexcept
    {
        if (this != &other) {
            reset();
            if (other.operations_ != nullptr) {
                other.operations_->move(storage_.data(), other.storage_.data());
                operations_ = std::exchange(other.operations_, nullptr);
            }
        }
        return *this;
    }

    ~InlineFunction()
    {
        reset();
    }

    void reset() noexcept
    {
        if (operations_ != nullptr) {
            operations_->destroy(storage_.data());
            operations_ = nullptr;
        }
    }

    [[nodiscard]] explicit operator bool() const noexcept
    {
        return operations_ != nullptr;
    }

    R operator()(Args... args) const
    {
        if (operations_ == nullptr) {
            throw std::bad_function_call();
        }
        return operations_->invoke(storage_.data(), std::forward<Args>(args)...);
    }

  private:
    alignas(std::max_align_t) mutable std::array<std::byte, Capacity> storage_;
    const Operations *operations_ = nullptr;
};
} // namespace crowdb::tree::detail
