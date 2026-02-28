#pragma once

#include "async.h"

#include <ydb/library/actors/task/task.h>
#include <ydb/library/actors/task/task_system.h>

#include <util/system/yassert.h>

#include <coroutine>
#include <exception>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

namespace NActors {

    namespace NDetail {

        template<class T>
        class TTaskExecuteResult {
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
        NTask::task<void> RunAndStoreTaskResult(
                NTask::task<T>&& sourceTask,
                std::shared_ptr<TTaskExecuteResult<T>> result)
        {
            try {
                if constexpr (std::is_void_v<T>) {
                    co_await std::move(sourceTask);
                    result->SetValue();
                } else {
                    result->SetValue(co_await std::move(sourceTask));
                }
            } catch (...) {
                result->SetException(std::current_exception());
            }
        }

        template<class T>
        class TExecuteTaskAwaiter {
        public:
            explicit TExecuteTaskAwaiter(NTask::task<T>&& task, NTask::TTaskSystem* taskSystem)
                : SourceTask_(std::move(task))
                , TaskSystem_(taskSystem)
                , Result_(std::make_shared<TTaskExecuteResult<T>>())
            {
            }

            TExecuteTaskAwaiter(const TExecuteTaskAwaiter&) = delete;
            TExecuteTaskAwaiter& operator=(const TExecuteTaskAwaiter&) = delete;
            TExecuteTaskAwaiter(TExecuteTaskAwaiter&&) = default;
            TExecuteTaskAwaiter& operator=(TExecuteTaskAwaiter&&) = default;

            static constexpr bool await_ready() noexcept {
                return false;
            }

            std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) {
                Y_ABORT_UNLESS(SourceTask_, "Execute called with empty task");

                RunnerTask_ = RunAndStoreTaskResult(std::move(SourceTask_), Result_);
                auto runnerHandle = RunnerTask_.GetHandle();
                Y_ABORT_UNLESS(runnerHandle);

                runnerHandle.promise().SetContinuation(continuation);

                if (TaskSystem_) {
                    TaskSystem_->Enqueue(std::coroutine_handle<>(RunnerTask_.ReleaseHandle()));
                    return std::noop_coroutine();
                }

                return std::coroutine_handle<>(runnerHandle);
            }

            T await_resume() {
                return Result_->ExtractValue();
            }

        private:
            NTask::task<T> SourceTask_;
            NTask::task<void> RunnerTask_;
            NTask::TTaskSystem* TaskSystem_ = nullptr;
            std::shared_ptr<TTaskExecuteResult<T>> Result_;
        };

    } // namespace NDetail

    template<class T>
    async<T> Execute(NTask::task<T>&& task) {
        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        NTask::TTaskSystem* taskSystem = actorSystem ? actorSystem->GetSubSystem<NTask::TTaskSystem>() : nullptr;
        if constexpr (std::is_void_v<T>) {
            co_await NDetail::TExecuteTaskAwaiter<T>(std::move(task), taskSystem);
            co_return;
        } else {
            co_return co_await NDetail::TExecuteTaskAwaiter<T>(std::move(task), taskSystem);
        }
    }

} // namespace NActors
