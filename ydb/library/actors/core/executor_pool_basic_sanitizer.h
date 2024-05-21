#pragma once

#include "executor_pool_basic.h"

#include <atomic>
#include <ctime>
#include <ydb/library/actors/util/thread.h>


namespace NActors {

class TBasicExecutorPoolSanitizer : public ISimpleThread {
public:
    TBasicExecutorPoolSanitizer(TBasicExecutorPool *pool)
        : Pool(pool)
    {}

    void CheckSemaphore() const {
        auto x = AtomicGet(Pool->Semaphore);
        auto semaphore = TBasicExecutorPool::TSemaphore::GetSemaphore(x);
        Y_ABORT_UNLESS(semaphore.OldSemaphore <= 0 || semaphore.CurrentThreadCount != semaphore.CurrentSleepThreadCount || Pool->StopFlag.load());
    }

    void CheckThreads() {
        ui64 stopFlagBit = 1ll << 63;
        Pool->ThreadsInUncertainState.fetch_add(stopFlagBit, std::memory_order_acq_rel);

        while(Pool->ThreadsInUncertainState.load(std::memory_order_acquire) ^ stopFlagBit) {
            SpinLockPause();
        }

        Pool->ThreadsInUncertainState.fetch_sub(stopFlagBit, std::memory_order_acq_rel);
    }

    void* ThreadProc() override {
        ui64 idx = 0;
        while (!StopFlag.load(std::memory_order_acquire)) {
            idx++;
            CheckSemaphore();
            if (idx % 128 == 0) {
                CheckThreads();
            }
            NanoSleep(1000);
        }
        return nullptr;
    }

    void Stop() {
        StopFlag.store(true, std::memory_order_release);
    }

private:
    TBasicExecutorPool *Pool;
    std::atomic_bool StopFlag = false;
};

} // namespace NActors
