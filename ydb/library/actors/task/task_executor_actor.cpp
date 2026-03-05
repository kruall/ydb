#include "task_system.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NActors::NTask {

    class TTaskExecutorActor final : public TActor<TTaskExecutorActor> {
    public:
        explicit TTaskExecutorActor(TTaskSystem& system)
            : TActor(&TTaskExecutorActor::StateWork)
            , System_(&system)
        {
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TTaskSystem::TEvRunTask, HandleRunTask);
                HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
                default:
                    HandleEvent(ev);
            }
        }

    private:
        void HandleRunTask(TTaskSystem::TEvRunTask::TPtr& ev, const TActorContext& ctx) {
            auto handle = ev->Get()->Handle;
            System_->RunTask(handle, ctx.SelfID, ev->Get()->DestroyOnDone);
        }

        void HandlePoisonPill(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
            Die(ctx);
        }

        void HandleEvent(TAutoPtr<IEventHandle>& ev) {
            Y_UNUSED(System_->HandleExecutorEvent(ActorContext().SelfID, ev));
        }

    private:
        TTaskSystem* System_ = nullptr;
    };

    IActor* CreateTaskExecutorActor(TTaskSystem& system, ui32) {
        return new TTaskExecutorActor(system);
    }

} // namespace NActors::NTask
