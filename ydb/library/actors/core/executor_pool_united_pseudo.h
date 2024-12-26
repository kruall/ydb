#pragma once
#include "executor_pool_jail.h"
#include "executor_pool_base.h"
#include "executor_pool_united.h"
#include "actor.h"
#include "config.h"
#include "executor_thread_ctx.h"
#include "probes.h"
#include "mailbox.h"
#include "debug.h"
#include "thread_context.h"
#include <atomic>
#include <memory>
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/datetime.h>

#ifdef _linux_
#include <pthread.h>
#endif

namespace NActors {

    struct TPsuedoBasicExecutorPoolConfig {
        i16 PoolId;
        i16 DefaultThreadsCount;
        i16 MaxThreadsCount;
        i16 UnitedThreadsCount;
        TString PoolName;
        bool UseRingQueue;
        ui64 Leases;
    };

    class TPsuedoBasicExecutorPool : TExecutorPoolBase {
        friend class TUnitedExecutorPool;
        friend class TUnitedExecutorPoolSanitizer;
        TUnitedExecutorPool *Pool;
        TPsuedoBasicExecutorPoolConfig Config;
        TArrayHolder<NThreading::TPadded<TExecutorThreadStats>> ThreadStats;
        alignas(64) NThreading::TPadded<std::atomic<ui64>> LocalNotifications;
        alignas(64) NThreading::TPadded<std::atomic<ui64>> Leases;
        alignas(64) NThreading::TPadded<std::atomic<ui64>> LocalThreads;
    public:
        TPsuedoBasicExecutorPool(const TPsuedoBasicExecutorPoolConfig& config, TUnitedExecutorPool *pool)
            : TExecutorPoolBase(config.PoolId, config.MaxThreadsCount, nullptr, config.UseRingQueue)
            , Pool(pool)
            , Config(config)
            , ThreadStats(new NThreading::TPadded<TExecutorThreadStats>[config.UnitedThreadsCount])
            , LocalNotifications(0)
            , Leases(config.Leases)
            , LocalThreads(config.DefaultThreadsCount)
        {
        }

        TString GetName() const override {
            return Config.PoolName;
        }

        void Initialize(TWorkerContext& wctx) override {
            wctx.UnitedExecutor = Pool;
            wctx.Stats = &ThreadStats[wctx.WorkerId];
            wctx.MailboxTable = MailboxTable.Get();
            wctx.Executor = this;
            TlsThreadContext->Pool = this;
            wctx.Stats = &ThreadStats[wctx.WorkerId];
        }

        TMailbox* GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) override {
            TWorkerId workerId = wctx.WorkerId;
            NHPTimer::STime hpnow = GetCycleCountFast();
            TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard(hpnow);

            Pool->Threads[workerId].UnsetWork();
            TAtomic semaphore = AtomicGet(Semaphore);
            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " Pseudo_", PoolId, " TPsuedoBasicExecutorPool::GetReadyActivation: revolvingCounter == ", revolvingCounter, " semaphore == ", semaphore);
            while (!StopFlag.load(std::memory_order_acquire)) {
                if (semaphore == 0) {
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " Pseudo_", PoolId, " TPsuedoBasicExecutorPool::GetReadyActivation: semaphore == 0");
                    return nullptr;
                } else {
                    TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION_FROM_QUEUE, false> activityGuard;
                    if (const ui32 activation = std::visit([&revolvingCounter](auto &x) {return x.Pop(++revolvingCounter);}, Activations)) {
                        Pool->Threads[workerId].SetWork();
                        ui64 semaphore = AtomicDecrement(Semaphore);
                        ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " Pseudo_", PoolId, " TPsuedoBasicExecutorPool::GetReadyActivation: activation == ", activation, " semaphore == ", semaphore);
                        return MailboxTable->Get(activation);
                    }
                }

                SpinLockPause();
                semaphore = AtomicGet(Semaphore);
            }
            ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " Pseudo_", PoolId, " TPsuedoBasicExecutorPool::GetReadyActivation: stop");
            return nullptr;
        }

        void Notify() {
            ui64 notifications = LocalNotifications.load(std::memory_order_acquire);
            if (notifications == 0) {
                notifications = LocalNotifications.fetch_add(1, std::memory_order_release);
                TWorkerId workerId = Max<TWorkerId>();
                if (TlsThreadContext) {
                    workerId = TlsThreadContext->WorkerId;
                }
                ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " Pseudo_", PoolId, " TPsuedoBasicExecutorPool::Notify: notifications == ", notifications, " -> ", notifications + 1);
            }
        }

        void ScheduleActivationEx(TMailbox* mailbox, ui64 revolvingCounter) override {
            TWorkerId workerId = Max<TWorkerId>();
            if (TlsThreadContext) {
                workerId = TlsThreadContext->WorkerId;
            }
            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " Pseudo_", PoolId, " TPsuedoBasicExecutorPool::ScheduleActivationEx; mailbox->Hint == ", mailbox->Hint, " revolvingCounter == ", revolvingCounter);
            std::visit([mailbox, revolvingCounter](auto &x) {
                x.Push(mailbox->Hint, revolvingCounter);
            }, Activations);
            ui64 semaphore = AtomicIncrement(Semaphore);
            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " Pseudo_", PoolId, " TPsuedoBasicExecutorPool::ScheduleActivationEx: semaphore == ", semaphore);
            Notify();
            Pool->TryToWakeUp(PoolId);
        }

        ////////////////////////////////////////////////////////////////

        void GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const override {
            ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "Pseudo_", PoolId, " TPsuedoBasicExecutorPool::GetCurrentStats: poolStats.WrongWakenedThreadCount == 0");
            poolStats.WrongWakenedThreadCount = 0;
            poolStats.CurrentThreadCount = GetThreadCount();
            poolStats.DefaultThreadCount = Config.DefaultThreadsCount;
            poolStats.MaxThreadCount = Config.MaxThreadsCount;
            // TODO potential max thread count
            poolStats.PotentialMaxThreadCount = Config.MaxThreadsCount;
            poolStats.SpinningTimeUs = 0; // Ts2Us(SpinningTimeUs);
            poolStats.SpinThresholdUs = 0; // Ts2Us(SpinThresholdCycles);
            statsCopy.resize(Config.UnitedThreadsCount + 1);
            // Save counters from the pool object
            statsCopy[0] = TExecutorThreadStats();
            statsCopy[0].Aggregate(Stats);
    #if defined(ACTORSLIB_COLLECT_EXEC_STATS)
            RecalculateStuckActors(statsCopy[0]);
    #endif
            // Per-thread stats
            for (i16 i = 0; i < Config.UnitedThreadsCount; ++i) {
                statsCopy[i + 1].Aggregate(ThreadStats[i]);
            }
        }

        ////////////////////////////////////////////////////////////////

        void Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) override {
            ActorSystem = actorSystem;
            *scheduleReaders = nullptr;
            *scheduleSz = 0;
        }

        void Start() override {
            ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, " Pseudo_", PoolId, " TPsuedoBasicExecutorPool::Start");
        }

        void PrepareStop() override {
            StopFlag.store(true, std::memory_order_release);
            ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "Pseudo_", PoolId, " TPsuedoBasicExecutorPool::PrepareStop");
        }

        void Shutdown() override {
            ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, " Pseudo_", PoolId, " TPsuedoBasicExecutorPool::Shutdown");
        }

        void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override {
            Pool->Schedule(deadline, ev, cookie, workerId);
        }

        void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override {
            Pool->Schedule(deadline, ev, cookie, workerId);
        }

        void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override {
            Pool->Schedule(delta, ev, cookie, workerId);
        }

    };

}
