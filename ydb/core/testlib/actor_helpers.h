#pragma once
#include "defs.h"

namespace NActors {
    class TActorSystem;
    struct TMailboxHeader;
    class TExecutorThread;
    struct TActorContext;
    struct TActivationContext;
}

namespace NKikimr {

struct TActorSystemStub {
    THolder<NActors::TActorSystem> System;
    THolder<NActors::TMailboxHeader> Mailbox;
    THolder<NActors::TExecutorThread> ExecutorThread;
    NActors::TActorId SelfID;
    THolder<NActors::TActorContext> Ctx;
    NActors::TActivationContext* PrevCtx;

    TActorSystemStub();
    ~TActorSystemStub();
};

}
