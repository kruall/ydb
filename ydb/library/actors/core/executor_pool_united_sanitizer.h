#pragma once

#include "executor_pool_united.h"
#include "executor_pool_united_pseudo.h"

#include <atomic>
#include <ctime>
#include <ydb/library/actors/util/thread.h>


namespace NActors {

class TUnitedExecutorPoolSanitizer : public ISimpleThread {
public:
    TUnitedExecutorPoolSanitizer(TUnitedExecutorPool *pool)
        : Pool(pool)
    {}

    void CheckSemaphore() const {
        bool found = false;
        for (ui32 i = 0; i != Pool->PoolLeaseManager.PoolInfos.size(); ++i) {
            auto *pool = Pool->Pools[i];
            if (pool == nullptr) {
                continue;
            }
            ui64 semaphore = AtomicGet(Pool->Pools[i]->Semaphore);
            ui64 leases = Pool->Pools[i]->Leases.load(std::memory_order_acquire);
            ui64 localThreads = Pool->Pools[i]->LocalThreads.load(std::memory_order_acquire);
            ui64 localNotifications = Pool->Pools[i]->LocalNotifications.load(std::memory_order_acquire);

            ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "Iteration_", Iteration, " Pseudo_", i, " TUnitedExecutorPoolSanitizer::CheckSemaphore: semaphore == ", semaphore, " leases == ", leases, " localThreads == ", localThreads, " defaultThreads == ", Pool->Pools[i]->Config.DefaultThreadsCount, " localNotifications == ", localNotifications);
            if (semaphore > 0 && leases == Pool->Pools[i]->Config.Leases && localThreads == 0 && localNotifications == 0) {
                ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "Iteration_", Iteration, " Pseudo_", i, " TUnitedExecutorPoolSanitizer::CheckSemaphore: error state found: semaphore > 0 && leases == Pool->Pools[i]->Config.Leases && localThreads == 0 && localNotifications == 0");
                found = true;
            }
        }
        if (!found) {
            ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "Iteration_", Iteration, " TUnitedExecutorPoolSanitizer::CheckSemaphore: no error states found");
        }
    }

    void* ThreadProc() override {
        while (!StopFlag.load(std::memory_order_acquire)) {
            CheckSemaphore();
            NanoSleep(1000000000);
            Iteration++;
        }
        return nullptr;
    }

    void Stop() {
        StopFlag.store(true, std::memory_order_release);
    }

private:
    TUnitedExecutorPool *Pool;
    std::atomic_bool StopFlag = false;
    ui64 Iteration = 0;
};

} // namespace NActors
