#pragma once

#include <coroutine>
#include <exception>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

namespace NActors::NTask {

    namespace NDetail {

        template<class T>
        class TTaskResult {
        public:
            void SetValue()
                requires (std::is_void_v<T>)
            {
                Value_.template emplace<1>();
            }

            template<class U>
            void SetValue(U&& value)
                requires (!std::is_void_v<T> && std::is_convertible_v<U&&, T>)
            {
                Value_.template emplace<1>(std::forward<U>(value));
            }

            void SetException(std::exception_ptr e) noexcept {
                Value_.template emplace<2>(std::move(e));
            }

            bool HasResult() const noexcept {
                return Value_.index() != 0;
            }

            T ExtractValue() {
                switch (Value_.index()) {
                    case 1: {
                        if constexpr (std::is_void_v<T>) {
                            return;
                        } else {
                            return std::get<1>(std::move(Value_));
                        }
                    }
                    case 2: {
                        std::rethrow_exception(std::get<2>(std::move(Value_)));
                    }
                }
                throw std::logic_error("task result has neither value nor exception");
            }

        private:
            struct TVoid {};
            using TValue = std::conditional_t<std::is_void_v<T>, TVoid, T>;
            std::variant<std::monostate, TValue, std::exception_ptr> Value_;
        };

        template<class T>
        class TTaskPromiseResult : public TTaskResult<T> {
        public:
            template<class U>
            void return_value(U&& value)
                requires (std::is_convertible_v<U&&, T>)
            {
                this->SetValue(std::forward<U>(value));
            }

            void unhandled_exception() noexcept {
                this->SetException(std::current_exception());
            }
        };

        template<>
        class TTaskPromiseResult<void> : public TTaskResult<void> {
        public:
            void return_void() noexcept {
                this->SetValue();
            }

            void unhandled_exception() noexcept {
                this->SetException(std::current_exception());
            }
        };

        template<class T>
        class TTaskPromise;

        class TPromiseBase {
        public:
            void SetContinuation(std::coroutine_handle<> c) noexcept {
                Continuation_ = c;
            }

        protected:
            struct TFinalAwaiter {
                static constexpr bool await_ready() noexcept { return false; }
                static constexpr void await_resume() noexcept {}

                template<class TPromise>
                static std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> self) noexcept {
                    auto c = self.promise().Continuation_;
                    return c ? c : std::noop_coroutine();
                }
            };

            std::coroutine_handle<> Continuation_;
        };

        template<class T>
        class TTaskPromise
            : public TPromiseBase
            , public TTaskPromiseResult<T>
        {
        public:
            using TResult = TTaskPromiseResult<T>;

            auto get_return_object() noexcept;

            static constexpr std::suspend_always initial_suspend() noexcept { return {}; }
            static constexpr TFinalAwaiter final_suspend() noexcept { return {}; }
        };

    } // namespace NDetail

    template<class T>
    class [[nodiscard]] task {
        using TPromise = NDetail::TTaskPromise<T>;

    public:
        using result_type = T;
        using promise_type = TPromise;
        using THandle = std::coroutine_handle<promise_type>;

        task() noexcept = default;

        explicit task(THandle handle) noexcept
            : Handle_(handle)
        {}

        task(const task&) = delete;
        task& operator=(const task&) = delete;

        task(task&& rhs) noexcept
            : Handle_(std::exchange(rhs.Handle_, nullptr))
        {}

        task& operator=(task&& rhs) noexcept {
            if (this != &rhs) [[likely]] {
                Destroy();
                Handle_ = std::exchange(rhs.Handle_, nullptr);
            }
            return *this;
        }

        ~task() {
            Destroy();
        }

        explicit operator bool() const noexcept {
            return bool(Handle_);
        }

        bool done() const noexcept {
            return !Handle_ || Handle_.done();
        }

        void resume() {
            Handle_.resume();
        }

        void Destroy() noexcept {
            if (Handle_) {
                Handle_.destroy();
                Handle_ = nullptr;
            }
        }

        THandle GetHandle() const noexcept {
            return Handle_;
        }

        THandle ReleaseHandle() noexcept {
            return std::exchange(Handle_, nullptr);
        }

        T ExtractValue() {
            return Handle_.promise().ExtractValue();
        }

        class TAwaiter {
        public:
            explicit TAwaiter(THandle handle) noexcept
                : Handle(handle)
            {}

            TAwaiter(const TAwaiter&) = delete;
            TAwaiter& operator=(const TAwaiter&) = delete;

            ~TAwaiter() {
                if (Handle) {
                    Handle.destroy();
                }
            }

            bool await_ready() const noexcept {
                return !Handle || Handle.done();
            }

            std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
                Handle.promise().SetContinuation(caller);
                return Handle;
            }

            T await_resume() {
                return Handle.promise().ExtractValue();
            }

        private:
            THandle Handle;
        };

        TAwaiter operator co_await() && noexcept {
            return TAwaiter{ std::exchange(Handle_, nullptr) };
        }

        TAwaiter operator co_await() & = delete;

    private:
        THandle Handle_ = nullptr;
    };

    namespace NDetail {

        template<class T>
        inline auto TTaskPromise<T>::get_return_object() noexcept {
            return task<T>(std::coroutine_handle<TTaskPromise<T>>::from_promise(*this));
        }

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
