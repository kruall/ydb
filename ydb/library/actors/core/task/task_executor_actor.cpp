#include "task_system.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NActors::NTask {

    class TTaskExecutorActor final : public TActor<TTaskExecutorActor> {
    public:
        explicit TTaskExecutorActor(TTaskSystem& system, ui32 executorId)
            : TActor(&TTaskExecutorActor::StateWork)
            , System_(&system)
            , ExecutorId_(executorId)
        {
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvents::TEvWakeup, HandleWakeup);
                HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            }
        }

    private:
        void HandleWakeup(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
            const TMonotonic deadline = ctx.Monotonic() + System_->GetTimeQuota();
            while (ctx.Monotonic() < deadline) {
                auto handle = System_->TakeTask(ExecutorId_);
                if (!handle) {
                    break;
                }

                System_->RunTask(*handle);
            }

            if (ctx.Monotonic() >= deadline) {
                ctx.Send(SelfId(), new TEvents::TEvWakeup());
            }
        }

        void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
            Die(ctx);
        }

    private:
        TTaskSystem* System_ = nullptr;
        ui32 ExecutorId_ = 0;
    };

    IActor* CreateTaskExecutorActor(TTaskSystem& system, ui32 executorId) {
        return new TTaskExecutorActor(system, executorId);
    }

} // namespace NActors::NTask
