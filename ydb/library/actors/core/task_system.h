#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>

#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/system/yassert.h>
#include <coroutine>

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

        void Enqueue(std::coroutine_handle<> handle) {
            Y_ASSERT(handle);
            if (!IsInitialized()) {
                RunTask(handle);
                return;
            }

            TriggerRandomExecutor(handle);
        }

        template<class T>
        void Enqueue(task<T>&& t) {
            auto handle = std::coroutine_handle<>(t.ReleaseHandle());
            if (handle) {
                Enqueue(handle);
            }
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

            explicit TExecutor(TActorId actorId)
                : ActorId(actorId)
            {
            }
        };

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

    private:
        TActorSystem* ActorSystem_ = nullptr;
        TVector<TExecutor> Executors_;
    };

} // namespace NActors::NTask
