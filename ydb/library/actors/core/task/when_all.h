#pragma once

#include "task.h"

#include <coroutine>
#include <tuple>
#include <type_traits>
#include <utility>

namespace NActors::NTask {

    namespace NDetail {

        inline task<void> JoinCountdown(size_t& remaining) {
            for (;;) {
                if (--remaining == 0) {
                    co_return;
                }
                co_await std::suspend_always{};
            }
        }

        template<class... TTasks>
        class TWhenAll {
        public:
            explicit TWhenAll(TTasks&&... tasks)
                : Tasks(std::move(tasks)...)
            {}

            TWhenAll(const TWhenAll&) = delete;
            TWhenAll& operator=(const TWhenAll&) = delete;

            TWhenAll(TWhenAll&&) = default;
            TWhenAll& operator=(TWhenAll&&) = default;

            class TAwaiter {
            public:
                explicit TAwaiter(std::tuple<TTasks...>&& tasks)
                    : Tasks(std::move(tasks))
                {}

                bool await_ready() const noexcept {
                    return CountRemaining() == 0;
                }

                std::coroutine_handle<> await_suspend(std::coroutine_handle<> parent) {
                    Remaining = CountRemaining();
                    if (Remaining == 0) {
                        return parent;
                    }

                    Joiner = JoinCountdown(Remaining);
                    auto joinHandle = Joiner.GetHandle();
                    joinHandle.promise().SetContinuation(parent);

                    StartAll(joinHandle);

                    return std::noop_coroutine();
                }

                void await_resume() noexcept {
                    // nothing
                }

            private:
                size_t CountRemaining() const noexcept {
                    return CountRemainingImpl(std::make_index_sequence<sizeof...(TTasks)>{});
                }

                template<size_t... Is>
                size_t CountRemainingImpl(std::index_sequence<Is...>) const noexcept {
                    size_t remaining = 0;
                    ((remaining += (IsReady(std::get<Is>(Tasks)) ? 0 : 1)), ...);
                    return remaining;
                }

                static bool IsReady(const task<void>& t) noexcept {
                    if (!t) {
                        return true;
                    }
                    auto h = t.GetHandle();
                    return !h || h.done();
                }

                void StartAll(std::coroutine_handle<> joinHandle) {
                    StartAllImpl(joinHandle, std::make_index_sequence<sizeof...(TTasks)>{});
                }

                template<size_t... Is>
                void StartAllImpl(std::coroutine_handle<> joinHandle, std::index_sequence<Is...>) {
                    (StartOne(std::get<Is>(Tasks), joinHandle), ...);
                }

                static void StartOne(task<void>& t, std::coroutine_handle<> joinHandle) {
                    if (!t) {
                        return;
                    }
                    auto h = t.GetHandle();
                    if (!h || h.done()) {
                        return;
                    }
                    h.promise().SetContinuation(joinHandle);
                    h.resume();
                }

            private:
                std::tuple<TTasks...> Tasks;
                task<void> Joiner;
                size_t Remaining = 0;
            };

            TAwaiter operator co_await() && noexcept {
                return TAwaiter{ std::move(Tasks) };
            }

            TAwaiter operator co_await() & = delete;

        private:
            std::tuple<TTasks...> Tasks;
        };

    } // namespace NDetail

    template<class... TTasks>
    inline auto WhenAll(TTasks&&... tasks)
        requires (
            sizeof...(TTasks) > 0 &&
            (std::is_same_v<std::remove_cvref_t<TTasks>, task<void>> && ...))
    {
        return NDetail::TWhenAll<std::remove_cvref_t<TTasks>...>(std::forward<TTasks>(tasks)...);
    }

    inline auto WhenAll()
    {
        struct TReady {
            struct TAwaiter {
                static constexpr bool await_ready() noexcept { return true; }
                static constexpr void await_suspend(std::coroutine_handle<>) noexcept {}
                static constexpr void await_resume() noexcept {}
            };
            constexpr TAwaiter operator co_await() const noexcept { return {}; }
        };
        return TReady{};
    }

} // namespace NActors::NTask
