#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/subsystem.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/system/yassert.h>
#include <coroutine>

#include "task.h"

namespace NActors {

class IActor;

} // namespace NActors

namespace NActors::NTask {

    namespace NDetail {

        class ITaskEventAwaiter {
        public:
            virtual bool Handle(TAutoPtr<IEventHandle>& ev) = 0;

        protected:
            ~ITaskEventAwaiter() = default;
        };

        template<class TEvent>
        class TTaskEventAwaiter;

    } // namespace NDetail

    class TTaskExecutorActor;
    class TTaskSystem;

    IActor* CreateTaskExecutorActor(TTaskSystem& system, ui32 executorId);

    class TTaskSystem final : public ISubSystem {
    public:
        static constexpr ui32 DefaultExecutors = 1;
        enum EEv {
            EvRunTask = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvEnd
        };
        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "TTaskSystem private events overflow");

        struct TEvRunTask : public TEventLocal<TEvRunTask, EvRunTask> {
            explicit TEvRunTask(std::coroutine_handle<> handle) noexcept
                : Handle(handle)
            {
            }

            std::coroutine_handle<> Handle;
        };

        TTaskSystem() = default;

        TTaskSystem(const TTaskSystem&) = delete;
        TTaskSystem& operator=(const TTaskSystem&) = delete;

        void Initialize(TActorSystem* actorSystem, ui32 executorCount, ui32 poolId = 0) {
            Y_ASSERT(actorSystem);
            Y_ASSERT(executorCount > 0);
            Y_ASSERT(!IsInitialized());

            ActorSystem_ = actorSystem;
            Executors_.clear();
            Executors_.reserve(executorCount);

            for (ui32 executorId = 0; executorId < executorCount; ++executorId) {
                Executors_.emplace_back(actorSystem->Register(CreateTaskExecutorActor(*this, executorId), TMailboxType::HTSwap, poolId));
            }
        }

        bool IsInitialized() const {
            return !Executors_.empty();
        }

        static ui64 GetUniqueCookieForMessage() {
            TTaskSystem* system = CurrentTaskSystem();
            Y_ABORT_UNLESS(system, "TTaskSystem::GetUniqueCookieForMessage must be called from task executor actor");
            return system->AllocateCookie(CurrentExecutorActorId());
        }

        void Enqueue(std::coroutine_handle<> handle) {
            Y_ASSERT(handle);
            Y_ABORT_UNLESS(IsInitialized(), "TTaskSystem must be initialized before Enqueue");
            TriggerRandomExecutor(handle);
        }

        template<class T>
        void Enqueue(task<T>&& t) {
            auto handle = std::coroutine_handle<>(t.ReleaseHandle());
            if (handle) {
                Enqueue(handle);
            }
        }

    private:
        friend class TTaskExecutorActor;
        template<class TEvent>
        friend class NDetail::TTaskEventAwaiter;

        void RunTask(std::coroutine_handle<> handle, const TActorId& executorActorId) {
            Y_ABORT_UNLESS(IsExecutorActor(executorActorId), "TTaskSystem tasks must run only in task executor actors");

            const TRunContextGuard contextGuard(this, executorActorId);
            handle.resume();

            if (handle.done()) {
                handle.destroy();
            }
        }

        bool HandleExecutorEvent(const TActorId& executorActorId, TAutoPtr<IEventHandle>& ev) {
            TExecutor* executor = FindExecutor(executorActorId);
            auto it = executor->EventAwaiters.find(ev->Cookie);
            if (it == executor->EventAwaiters.end()) {
                return false;
            }

            for (auto* awaiter : it->second) {
                if (awaiter->Handle(ev)) {
                    return true;
                }
            }

            return false;
        }

        struct TExecutor {
            TActorId ActorId;
            THashMap<ui64, TVector<NDetail::ITaskEventAwaiter*>> EventAwaiters;
            ui64 NextMessageCookie = 1;

            explicit TExecutor(TActorId actorId)
                : ActorId(actorId)
            {
            }
        };

        struct TRunContext {
            TTaskSystem* System;
            TActorId ExecutorActorId;

            TRunContext()
                : System(nullptr)
                , ExecutorActorId()
            {
            }

            TRunContext(TTaskSystem* system, const TActorId& executorActorId)
                : System(system)
                , ExecutorActorId(executorActorId)
            {
            }
        };

        class TRunContextGuard {
        public:
            TRunContextGuard(TTaskSystem* system, const TActorId& executorActorId)
                : PrevContext_(CurrentRunContext_)
            {
                CurrentRunContext_ = TRunContext(system, executorActorId);
            }

            TRunContextGuard(const TRunContextGuard&) = delete;
            TRunContextGuard& operator=(const TRunContextGuard&) = delete;

            ~TRunContextGuard() {
                CurrentRunContext_ = PrevContext_;
            }

        private:
            TRunContext PrevContext_;
        };

        static inline thread_local TRunContext CurrentRunContext_;

        static TTaskSystem* CurrentTaskSystem() noexcept {
            return CurrentRunContext_.System;
        }

        static TActorId CurrentExecutorActorId() noexcept {
            return CurrentRunContext_.ExecutorActorId;
        }

        TExecutor* FindExecutor(const TActorId& actorId) {
            for (auto& executor : Executors_) {
                if (executor.ActorId == actorId) {
                    return &executor;
                }
            }
            Y_ABORT("Task executor actor is not registered in this TTaskSystem");
        }

        void RegisterEventAwaiter(const TActorId& executorActorId, ui64 cookie, NDetail::ITaskEventAwaiter* awaiter) {
            Y_ABORT_UNLESS(cookie != 0, "cookie must be non-zero");
            Y_ABORT_UNLESS(awaiter, "awaiter must not be null");
            FindExecutor(executorActorId)->EventAwaiters[cookie].push_back(awaiter);
        }

        void UnregisterEventAwaiter(const TActorId& executorActorId, ui64 cookie, NDetail::ITaskEventAwaiter* awaiter) {
            TExecutor* executor = FindExecutor(executorActorId);
            auto it = executor->EventAwaiters.find(cookie);
            if (it == executor->EventAwaiters.end()) {
                return;
            }

            auto& awaiters = it->second;
            for (size_t i = 0; i < awaiters.size(); ++i) {
                if (awaiters[i] == awaiter) {
                    awaiters.erase(awaiters.begin() + i);
                    break;
                }
            }

            if (awaiters.empty()) {
                executor->EventAwaiters.erase(it);
            }
        }

        ui64 AllocateCookie(const TActorId& executorActorId) {
            auto& nextCookie = FindExecutor(executorActorId)->NextMessageCookie;
            ui64 cookie = nextCookie++;
            if (cookie == 0) {
                cookie = nextCookie++;
            }
            return cookie;
        }

        ui32 ChooseExecutor() const {
            Y_ASSERT(!Executors_.empty());
            return RandomNumber<ui32>(Executors_.size());
        }

        void TriggerExecutor(ui32 idx, std::coroutine_handle<> handle) {
            Y_ASSERT(idx < Executors_.size());
            ActorSystem_->Send(Executors_[idx].ActorId, new TEvRunTask(handle));
        }

        void TriggerRandomExecutor(std::coroutine_handle<> handle) {
            TriggerExecutor(ChooseExecutor(), handle);
        }

        bool IsExecutorActor(const TActorId& actorId) const {
            for (const auto& executor : Executors_) {
                if (executor.ActorId == actorId) {
                    return true;
                }
            }
            return false;
        }

    private:
        TActorSystem* ActorSystem_ = nullptr;
        TVector<TExecutor> Executors_;
    };

    namespace NDetail {

        template<class TEvent>
        class [[nodiscard]] TTaskEventAwaiter final : public ITaskEventAwaiter {
        public:
            explicit TTaskEventAwaiter(ui64 cookie)
                : Cookie(cookie)
            {
            }

            TTaskEventAwaiter(const TTaskEventAwaiter&) = delete;
            TTaskEventAwaiter& operator=(const TTaskEventAwaiter&) = delete;

            ~TTaskEventAwaiter() {
                Detach();
            }

            bool await_ready() const noexcept {
                return false;
            }

            void await_suspend(std::coroutine_handle<> continuation) {
                System = TTaskSystem::CurrentTaskSystem();
                Y_ABORT_UNLESS(System, "NTask::WaitEvent must be called from TTaskSystem task executor actor");
                ExecutorActorId = TTaskSystem::CurrentExecutorActorId();
                Y_ABORT_UNLESS(ExecutorActorId, "executor actor id is not available");

                Continuation = continuation;
                System->RegisterEventAwaiter(ExecutorActorId, Cookie, this);
            }

            typename TEvent::TPtr await_resume() noexcept {
                return std::move(Result);
            }

            bool Handle(TAutoPtr<IEventHandle>& ev) override {
                Y_ABORT_UNLESS(System, "Unexpected Handle call after awaiter detach");
                if (Matches(ev)) {
                    Result = std::move(reinterpret_cast<typename TEvent::TPtr&>(ev));
                    Detach();
                    Continuation.resume();
                    return true;
                }
                return false;
            }

        private:
            bool Matches(TAutoPtr<IEventHandle>& ev) const {
                if constexpr (std::is_same_v<TEvent, IEventHandle>) {
                    return true;
                } else {
                    return ev->GetTypeRewrite() == TEvent::EventType;
                }
            }

            void Detach() {
                if (System) {
                    System->UnregisterEventAwaiter(ExecutorActorId, Cookie, this);
                    System = nullptr;
                    ExecutorActorId = TActorId();
                }
            }

        private:
            const ui64 Cookie;
            typename TEvent::TPtr Result;
            TTaskSystem* System = nullptr;
            TActorId ExecutorActorId;
            std::coroutine_handle<> Continuation;
        };

    } // namespace NDetail

    template<class TEvent>
    inline auto WaitEvent(ui64 cookie) {
        return NDetail::TTaskEventAwaiter<TEvent>(cookie);
    }

} // namespace NActors::NTask
