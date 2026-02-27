#pragma once

#include "task_system.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NActors::NTask {

    class TTaskExecutorActor final : public TActorBootstrapped<TTaskExecutorActor> {
        using TBase = TActorBootstrapped<TTaskExecutorActor>;

    public:
        explicit TTaskExecutorActor(TTaskSystem& system, ui32 maxBatch = 1024)
            : System_(&system)
            , MaxBatch_(maxBatch)
        {
        }

        void Bootstrap(const TActorContext& /*ctx*/) {
            Become(&TTaskExecutorActor::StateWork);
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvents::TEvWakeup, HandleWakeup);
                HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            }
        }

    private:
        void HandleWakeup(TEvents::TEvWakeup::TPtr& /*ev*/, const TActorContext& ctx) {
            ui32 ran = 0;
            for (; ran < MaxBatch_; ++ran) {
                if (!System_->TryRunOne()) {
                    break;
                }
            }

            if (ran == MaxBatch_) {
                ctx.Send(SelfId(), new TEvents::TEvWakeup());
            }
        }

        void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr& /*ev*/, const TActorContext& ctx) {
            Die(ctx);
        }

    private:
        TTaskSystem* System_ = nullptr;
        ui32 MaxBatch_ = 0;
    };

    inline IActor* CreateTaskExecutorActor(TTaskSystem& system, ui32 maxBatch = 1024) {
        return new TTaskExecutorActor(system, maxBatch);
    }

} // namespace NActors::NTask

