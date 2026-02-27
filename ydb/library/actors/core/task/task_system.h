#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>

#include <algorithm>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/system/yassert.h>
#include <util/thread/lfqueue.h>

#include <atomic>
#include <coroutine>
#include <optional>

#include "task.h"

namespace NActors {

class IActor;

} // namespace NActors

namespace NActors::NTask {

    class TTaskSystem;

    IActor* CreateTaskExecutorActor(TTaskSystem& system, ui32 executorId);

    class TTaskSystem final {
    public:
        static constexpr ui32 DefaultExecutors = 1;
        static constexpr ui32 WakeupProbeCount = 4;
        static constexpr TDuration DefaultTimeQuota = TDuration::MicroSeconds(500);

        TTaskSystem() = default;

        TTaskSystem(const TTaskSystem&) = delete;
        TTaskSystem& operator=(const TTaskSystem&) = delete;

        void Initialize(TActorSystem* actorSystem, ui32 executorCount, TDuration timeQuota = DefaultTimeQuota) {
            Y_ASSERT(actorSystem);
            Y_ASSERT(executorCount > 0);
            Y_ASSERT(!IsInitialized());

            ActorSystem_ = actorSystem;
            TimeQuota_ = timeQuota;
            Executors_.clear();
            Executors_.reserve(executorCount);

            for (ui32 executorId = 0; executorId < executorCount; ++executorId) {
                Executors_.emplace_back(actorSystem->Register(CreateTaskExecutorActor(*this, executorId)), false);
            }
        }

        bool IsInitialized() const {
            return !Executors_.empty();
        }

        TDuration GetTimeQuota() const {
            return TimeQuota_;
        }

        void Enqueue(std::coroutine_handle<> handle) {
            Y_ASSERT(handle);
            Queue_.Enqueue(handle);

            if (!IsInitialized()) {
                return;
            }

            TriggerRandomExecutor();
        }

        template<class T>
        void Enqueue(task<T>&& t) {
            auto handle = std::coroutine_handle<>(t.ReleaseHandle());
            if (handle) {
                Enqueue(handle);
            }
        }

        std::optional<std::coroutine_handle<>> TryDequeue() {
            std::coroutine_handle<> handle;
            if (Queue_.Dequeue(&handle)) {
                return handle;
            }
            return std::nullopt;
        }

        bool TryRunOne() {
            auto handle = TryDequeue();
            if (!handle) {
                return false;
            }

            RunTask(*handle);
            return true;
        }

        std::optional<std::coroutine_handle<>> TakeTask(ui32 executorId) {
            auto handle = TryDequeue();
            if (handle) {
                return handle;
            }

            Y_ASSERT(executorId < Executors_.size());
            auto& executor = Executors_[executorId];

            executor.Active.store(false, std::memory_order_release);

            handle = TryDequeue();
            if (!handle) {
                return std::nullopt;
            }

            if (!executor.Active.exchange(true, std::memory_order_acq_rel)) {
                // transitioned back to active locally
            }

            return handle;
        }

        void RunTask(std::coroutine_handle<> handle) const {
            handle.resume();
            if (handle.done()) {
                handle.destroy();
            }
        }

    private:
        struct TExecutor {
            TActorId ActorId;
            std::atomic_bool Active = false;

            TExecutor() = default;

            TExecutor(TActorId actorId, bool active)
                : ActorId(actorId)
                , Active(active)
            {
            }

            TExecutor(const TExecutor&) = delete;
            TExecutor& operator=(const TExecutor&) = delete;

            TExecutor(TExecutor&& other) noexcept
                : ActorId(std::move(other.ActorId))
                , Active(other.Active.load(std::memory_order_acquire))
            {
            }

            TExecutor& operator=(TExecutor&& other) noexcept {
                if (this != &other) {
                    ActorId = std::move(other.ActorId);
                    Active.store(other.Active.load(std::memory_order_acquire), std::memory_order_release);
                }
                return *this;
            }
        };

        ui32 ChooseExecutor() const {
            Y_ASSERT(!Executors_.empty());
            return RandomNumber<ui32>(Executors_.size());
        }

        bool TriggerExecutor(ui32 idx) {
            auto& executor = Executors_[idx];
            if (!executor.Active.exchange(true, std::memory_order_acq_rel)) {
                ActorSystem_->Send(executor.ActorId, new TEvents::TEvWakeup());
                return true;
            }
            return false;
        }

        void TriggerRandomExecutor() {
            const ui32 start = ChooseExecutor();
            const ui32 attempts = std::min<ui32>(WakeupProbeCount, Executors_.size());
            for (ui32 i = 0; i < attempts; ++i) {
                const ui32 idx = (start + i) % Executors_.size();
                if (TriggerExecutor(idx)) {
                    return;
                }
            }
        }

    private:
        TActorSystem* ActorSystem_ = nullptr;
        TDuration TimeQuota_ = DefaultTimeQuota;
        TVector<TExecutor> Executors_;
        TLockFreeQueue<std::coroutine_handle<>, TDefaultLFCounter> Queue_;
    };

} // namespace NActors::NTask
