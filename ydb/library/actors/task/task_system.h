#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/subsystem.h>

#include <util/generic/hash.h>
#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/system/mutex.h>
#include <util/system/yassert.h>
#include <coroutine>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "task.h"

namespace NActors {

class IActor;

} // namespace NActors

namespace NActors::NTask {

    namespace NDetail {

        class ITaskEventAwaiter {
        public:
            virtual bool TryConsumeQueued(std::coroutine_handle<>& outContinuation, bool& outDestroyOnDone) = 0;

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
            explicit TEvRunTask(std::coroutine_handle<> handle, bool destroyOnDone = true) noexcept
                : Handle(handle)
                , DestroyOnDone(destroyOnDone)
            {
            }

            std::coroutine_handle<> Handle;
            bool DestroyOnDone = true;
        };

        TTaskSystem() = default;

        TTaskSystem(const TTaskSystem&) = delete;
        TTaskSystem& operator=(const TTaskSystem&) = delete;

        void Initialize(TActorSystem* actorSystem, ui32 executorCount, ui32 poolId = 0) {
            Y_ASSERT(actorSystem);
            Y_ASSERT(executorCount > 0);
            Y_ASSERT(!IsInitialized());

            InstallTaskHooks();
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

        void Enqueue(std::coroutine_handle<> handle) {
            Y_ASSERT(handle);
            Y_ABORT_UNLESS(IsInitialized(), "TTaskSystem must be initialized before Enqueue");
            TriggerRandomExecutor(handle, true);
        }

        template<class T>
        void Enqueue(task<T>&& t) {
            auto handle = std::coroutine_handle<>(t.ReleaseHandle());
            if (handle) {
                Enqueue(handle);
            }
        }

        class TEventQueue {
        public:
            TEventQueue() = default;

            TEventQueue(TTaskSystem* system, TActorId executorActorId, ui64 cookie)
                : System_(system)
                , ExecutorActorId_(executorActorId)
                , Cookie_(cookie)
            {
            }

            TEventQueue(const TEventQueue&) = delete;
            TEventQueue& operator=(const TEventQueue&) = delete;

            TEventQueue(TEventQueue&& rhs) noexcept
                : System_(std::exchange(rhs.System_, nullptr))
                , ExecutorActorId_(std::exchange(rhs.ExecutorActorId_, TActorId()))
                , Cookie_(std::exchange(rhs.Cookie_, 0))
            {
            }

            TEventQueue& operator=(TEventQueue&& rhs) noexcept {
                if (this != &rhs) {
                    Release();
                    System_ = std::exchange(rhs.System_, nullptr);
                    ExecutorActorId_ = std::exchange(rhs.ExecutorActorId_, TActorId());
                    Cookie_ = std::exchange(rhs.Cookie_, 0);
                }
                return *this;
            }

            ~TEventQueue() {
                Release();
            }

            ui64 Cookie() const {
                Y_ABORT_UNLESS(System_, "event queue is not initialized");
                return Cookie_;
            }

            TTaskSystem* System() const {
                Y_ABORT_UNLESS(System_, "event queue is not initialized");
                return System_;
            }

            const TActorId& ExecutorActorId() const {
                Y_ABORT_UNLESS(System_, "event queue is not initialized");
                return ExecutorActorId_;
            }

        private:
            void Release() {
                if (System_) {
                    System_->UnregisterEventQueue(ExecutorActorId_, Cookie_);
                    System_ = nullptr;
                    ExecutorActorId_ = TActorId();
                    Cookie_ = 0;
                }
            }

        private:
            TTaskSystem* System_ = nullptr;
            TActorId ExecutorActorId_;
            ui64 Cookie_ = 0;
        };

        static TEventQueue CreateEventQueue() {
            TTaskSystem* system = CurrentTaskSystem();
            Y_ABORT_UNLESS(system, "TTaskSystem::CreateEventQueue must be called from task executor actor");
            const TActorId executorActorId = CurrentExecutorActorId();
            Y_ABORT_UNLESS(executorActorId, "executor actor id is not available");
            const ui64 cookie = system->AllocateCookie(executorActorId);
            system->RegisterEventQueue(executorActorId, cookie);
            return TEventQueue(system, executorActorId, cookie);
        }

    private:
        friend class TTaskExecutorActor;
        template<class TEvent>
        friend class NDetail::TTaskEventAwaiter;

        struct TTaskActorContextState {
            ~TTaskActorContextState() {
                Reset();
            }

            void Refresh(const TActorContext& context) {
                Reset();
                std::construct_at(GetStorage(), context.Mailbox, context.ExecutorThread, context.EventStart, context.SelfID);
                Constructed = true;
            }

            TActorContext* Get() {
                Y_ABORT_UNLESS(Constructed, "task actor context is not initialized");
                return std::launder(reinterpret_cast<TActorContext*>(Storage));
            }

        private:
            void Reset() {
                if (Constructed) {
                    std::destroy_at(Get());
                    Constructed = false;
                }
            }

            TActorContext* GetStorage() {
                return reinterpret_cast<TActorContext*>(Storage);
            }

        private:
            alignas(TActorContext) unsigned char Storage[sizeof(TActorContext)];
            bool Constructed = false;
        };

        struct TTaskActorContextEntry {
            TTaskSystem* System = nullptr;
            std::shared_ptr<TTaskActorContextState> State;
        };

        void RunTask(std::coroutine_handle<> handle, const TActorId& executorActorId, bool destroyOnDone) {
            Y_ABORT_UNLESS(IsExecutorActor(executorActorId), "TTaskSystem tasks must run only in task executor actors");

            auto actorContextState = GetOrCreateTaskActorContext(handle);
            const auto& actorContext = TActivationContext::AsActorContext();
            actorContextState->Refresh(actorContext);

            const TRunContextGuard contextGuard(this, executorActorId, destroyOnDone, actorContextState);
            const TTlsActorContextGuard tlsActorContextGuard(*actorContextState->Get());
            handle.resume();

            if (destroyOnDone && handle.done()) {
                handle.destroy();
            }
        }

        bool HandleExecutorEvent(const TActorId& executorActorId, TAutoPtr<IEventHandle>& ev) {
            TExecutor* executor = FindExecutor(executorActorId);
            const ui64 cookie = ev->Cookie;
            if (StoreEventInQueue(executor, cookie, ev)) {
                std::coroutine_handle<> continuation;
                bool destroyOnDone = false;
                if (TryConsumeQueuedEvent(executor, cookie, continuation, destroyOnDone)) {
                    Y_ABORT_UNLESS(continuation, "WaitEvent continuation is not set");
                    RunTask(continuation, executorActorId, destroyOnDone);
                }
                return true;
            }
            return false;
        }

        struct alignas(64) TExecutor {
            TActorId ActorId;
            alignas(64) THashMap<ui64, TVector<NDetail::ITaskEventAwaiter*>> EventAwaiters;
            THashMap<ui64, TDeque<TAutoPtr<IEventHandle>>> EventQueues;
            ui64 NextMessageCookie = 1;

            explicit TExecutor(TActorId actorId)
                : ActorId(actorId)
            {
            }
        };

        struct TRunContext {
            TTaskSystem* System;
            TActorId ExecutorActorId;
            bool DestroyOnDone;
            std::shared_ptr<TTaskActorContextState> ActorContextState;

            TRunContext()
                : System(nullptr)
                , ExecutorActorId()
                , DestroyOnDone(false)
            {
            }

            TRunContext(TTaskSystem* system, const TActorId& executorActorId, bool destroyOnDone,
                    std::shared_ptr<TTaskActorContextState> actorContextState)
                : System(system)
                , ExecutorActorId(executorActorId)
                , DestroyOnDone(destroyOnDone)
                , ActorContextState(std::move(actorContextState))
            {
            }
        };

        class TRunContextGuard {
        public:
            TRunContextGuard(TTaskSystem* system, const TActorId& executorActorId, bool destroyOnDone,
                    std::shared_ptr<TTaskActorContextState> actorContextState)
                : PrevContext_(CurrentRunContext_)
            {
                CurrentRunContext_ = TRunContext(system, executorActorId, destroyOnDone, std::move(actorContextState));
            }

            TRunContextGuard(const TRunContextGuard&) = delete;
            TRunContextGuard& operator=(const TRunContextGuard&) = delete;

            ~TRunContextGuard() {
                CurrentRunContext_ = PrevContext_;
            }

        private:
            TRunContext PrevContext_;
        };

        class TTlsActorContextGuard {
        public:
            explicit TTlsActorContextGuard(TActorContext& actorContext)
                : PrevContext_(TlsActivationContext)
            {
                TlsActivationContext = &actorContext;
            }

            TTlsActorContextGuard(const TTlsActorContextGuard&) = delete;
            TTlsActorContextGuard& operator=(const TTlsActorContextGuard&) = delete;

            ~TTlsActorContextGuard() {
                TlsActivationContext = PrevContext_;
            }

        private:
            TActivationContext* PrevContext_ = nullptr;
        };

        static inline thread_local TRunContext CurrentRunContext_;

        static TTaskSystem* CurrentTaskSystem() noexcept {
            return CurrentRunContext_.System;
        }

        static TActorId CurrentExecutorActorId() noexcept {
            return CurrentRunContext_.ExecutorActorId;
        }

        static bool CurrentDestroyOnDone() noexcept {
            return CurrentRunContext_.DestroyOnDone;
        }

        static std::shared_ptr<TTaskActorContextState> CurrentTaskActorContextState() noexcept {
            return CurrentRunContext_.ActorContextState;
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
            auto* executor = FindExecutor(executorActorId);
            Y_ABORT_UNLESS(executor->EventQueues.find(cookie) != executor->EventQueues.end(),
                "event queue for cookie is not registered");
            executor->EventAwaiters[cookie].push_back(awaiter);
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

        void RegisterEventQueue(const TActorId& executorActorId, ui64 cookie) {
            Y_ABORT_UNLESS(cookie != 0, "cookie must be non-zero");
            auto* executor = FindExecutor(executorActorId);
            auto [it, inserted] = executor->EventQueues.try_emplace(cookie);
            Y_ABORT_UNLESS(inserted, "event queue for cookie already exists");
            Y_UNUSED(it);
        }

        void UnregisterEventQueue(const TActorId& executorActorId, ui64 cookie) {
            if (cookie == 0) {
                return;
            }
            auto* executor = FindExecutor(executorActorId);
            Y_ABORT_UNLESS(executor->EventAwaiters.find(cookie) == executor->EventAwaiters.end(),
                "event queue is being destroyed while awaiters are still registered");
            executor->EventQueues.erase(cookie);
        }

        template<class TEvent>
        bool TryTakeQueuedEvent(const TActorId& executorActorId, ui64 cookie, typename TEvent::TPtr& out) {
            TAutoPtr<IEventHandle> ev = TakeQueuedEvent(executorActorId, cookie, EventTypeOf<TEvent>());
            if (!ev) {
                return false;
            }
            out = std::move(reinterpret_cast<typename TEvent::TPtr&>(ev));
            return true;
        }

        template<class TEvent>
        static constexpr ui32 EventTypeOf() {
            if constexpr (std::is_same_v<TEvent, IEventHandle>) {
                return 0;
            } else {
                return TEvent::EventType;
            }
        }

        TAutoPtr<IEventHandle> TakeQueuedEvent(const TActorId& executorActorId, ui64 cookie, ui32 type) {
            auto* executor = FindExecutor(executorActorId);
            auto it = executor->EventQueues.find(cookie);
            if (it == executor->EventQueues.end()) {
                return {};
            }

            auto& queue = it->second;
            if (queue.empty()) {
                return {};
            }

            if (type == 0) {
                TAutoPtr<IEventHandle> ev = std::move(queue.front());
                queue.pop_front();
                return ev;
            }

            for (auto qIt = queue.begin(); qIt != queue.end(); ++qIt) {
                if ((*qIt)->GetTypeRewrite() == type) {
                    TAutoPtr<IEventHandle> ev = std::move(*qIt);
                    queue.erase(qIt);
                    return ev;
                }
            }

            return {};
        }

        bool StoreEventInQueue(TExecutor* executor, ui64 cookie, TAutoPtr<IEventHandle>& ev) {
            auto it = executor->EventQueues.find(cookie);
            if (it == executor->EventQueues.end()) {
                return false;
            }
            it->second.push_back(ev.Release());
            return true;
        }

        bool TryConsumeQueuedEvent(TExecutor* executor, ui64 cookie,
                std::coroutine_handle<>& outContinuation, bool& outDestroyOnDone) {
            auto it = executor->EventAwaiters.find(cookie);
            if (it == executor->EventAwaiters.end()) {
                return false;
            }
            const auto awaiters = it->second;
            for (auto* awaiter : awaiters) {
                if (awaiter->TryConsumeQueued(outContinuation, outDestroyOnDone)) {
                    return true;
                }
            }
            return false;
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

        void TriggerExecutor(ui32 idx, std::coroutine_handle<> handle, bool destroyOnDone) {
            Y_ASSERT(idx < Executors_.size());
            ActorSystem_->Send(Executors_[idx].ActorId, new TEvRunTask(handle, destroyOnDone));
        }

        void TriggerRandomExecutor(std::coroutine_handle<> handle, bool destroyOnDone) {
            TriggerExecutor(ChooseExecutor(), handle, destroyOnDone);
        }

        bool IsExecutorActor(const TActorId& actorId) const {
            for (const auto& executor : Executors_) {
                if (executor.ActorId == actorId) {
                    return true;
                }
            }
            return false;
        }

        std::shared_ptr<TTaskActorContextState> GetOrCreateTaskActorContext(std::coroutine_handle<> handle) {
            Y_ABORT_UNLESS(handle, "task handle must not be empty");

            TGuard<TMutex> guard(TaskActorContextsLock_);
            auto& entry = TaskActorContexts_[handle.address()];
            if (entry.System) {
                Y_ABORT_UNLESS(entry.System == this, "task actor context is bound to another task system");
            } else {
                entry.System = this;
            }
            if (!entry.State) {
                entry.State = std::make_shared<TTaskActorContextState>();
            }
            return entry.State;
        }

        void BindTaskActorContext(std::coroutine_handle<> handle, std::shared_ptr<TTaskActorContextState> actorContextState) {
            Y_ABORT_UNLESS(handle, "task handle must not be empty");
            Y_ABORT_UNLESS(actorContextState, "task actor context state must not be empty");

            TGuard<TMutex> guard(TaskActorContextsLock_);
            auto& entry = TaskActorContexts_[handle.address()];
            if (entry.System) {
                Y_ABORT_UNLESS(entry.System == this, "task actor context is bound to another task system");
            } else {
                entry.System = this;
            }
            entry.State = std::move(actorContextState);
        }

        static void ReleaseTaskActorContext(std::coroutine_handle<> handle) noexcept {
            if (!handle) {
                return;
            }

            TGuard<TMutex> guard(TaskActorContextsLock_);
            TaskActorContexts_.erase(handle.address());
        }

        static void OnTaskAwaitSuspend(std::coroutine_handle<> handle) noexcept {
            TTaskSystem* system = CurrentTaskSystem();
            if (!system || !handle) {
                return;
            }

            auto actorContextState = CurrentTaskActorContextState();
            if (!actorContextState) {
                return;
            }

            system->BindTaskActorContext(handle, std::move(actorContextState));
        }

        static void OnTaskFinalSuspend(std::coroutine_handle<> handle) noexcept {
            ReleaseTaskActorContext(handle);
        }

        static void OnTaskDestroy(std::coroutine_handle<> handle) noexcept {
            ReleaseTaskActorContext(handle);
        }

        static void InstallTaskHooks() noexcept {
            NDetail::TaskAwaitSuspendHook = &TTaskSystem::OnTaskAwaitSuspend;
            NDetail::TaskFinalSuspendHook = &TTaskSystem::OnTaskFinalSuspend;
            NDetail::TaskDestroyHook = &TTaskSystem::OnTaskDestroy;
        }

    private:
        TActorSystem* ActorSystem_ = nullptr;
        TVector<TExecutor> Executors_;
        static inline TMutex TaskActorContextsLock_;
        static inline THashMap<void*, TTaskActorContextEntry> TaskActorContexts_;
    };

    namespace NDetail {

        template<class TEvent>
        class [[nodiscard]] TTaskEventAwaiter final : public ITaskEventAwaiter {
        public:
            explicit TTaskEventAwaiter(TTaskSystem::TEventQueue& queue)
                : Cookie(queue.Cookie())
                , System(queue.System())
                , ExecutorActorId(queue.ExecutorActorId())
                , DestroyOnDone(TTaskSystem::CurrentDestroyOnDone())
            {
            }

            TTaskEventAwaiter(const TTaskEventAwaiter&) = delete;
            TTaskEventAwaiter& operator=(const TTaskEventAwaiter&) = delete;

            ~TTaskEventAwaiter() {
                Detach();
            }

            bool await_ready() {
                ValidateContext();
                return TryTakeQueuedEvent();
            }

            bool await_suspend(std::coroutine_handle<> continuation) {
                ValidateContext();
                Continuation = continuation;
                System->RegisterEventAwaiter(ExecutorActorId, Cookie, this);
                Registered = true;
                if (TryTakeQueuedEvent()) {
                    Detach();
                    return false;
                }
                return true;
            }

            typename TEvent::TPtr await_resume() noexcept {
                return std::move(Result);
            }

            bool TryConsumeQueued(std::coroutine_handle<>& outContinuation, bool& outDestroyOnDone) override {
                Y_ABORT_UNLESS(System, "Unexpected TryConsumeQueued call after awaiter detach");
                if (TryTakeQueuedEvent()) {
                    outContinuation = Continuation;
                    outDestroyOnDone = DestroyOnDone;
                    Detach();
                    return true;
                }
                return false;
            }

        private:
            void ValidateContext() const {
                Y_ABORT_UNLESS(System, "event queue is not initialized");
                Y_ABORT_UNLESS(TTaskSystem::CurrentTaskSystem() == System &&
                    TTaskSystem::CurrentExecutorActorId() == ExecutorActorId,
                    "NTask::WaitEvent must be called from owner task executor actor");
            }

            bool TryTakeQueuedEvent() {
                return System->template TryTakeQueuedEvent<TEvent>(ExecutorActorId, Cookie, Result);
            }

            void Detach() {
                if (System) {
                    if (Registered) {
                        System->UnregisterEventAwaiter(ExecutorActorId, Cookie, this);
                        Registered = false;
                    }
                    System = nullptr;
                    ExecutorActorId = TActorId();
                }
            }

        private:
            const ui64 Cookie;
            typename TEvent::TPtr Result;
            TTaskSystem* System = nullptr;
            TActorId ExecutorActorId;
            bool DestroyOnDone = false;
            bool Registered = false;
            std::coroutine_handle<> Continuation;
        };

    } // namespace NDetail

    template<class TEvent>
    inline auto WaitEvent(TTaskSystem::TEventQueue& queue) {
        return NDetail::TTaskEventAwaiter<TEvent>(queue);
    }

} // namespace NActors::NTask
