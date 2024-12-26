#include "executor_pool_jail.h"
#include "executor_pool_united.h"
#include "executor_pool_united_pseudo.h"
#include "executor_pool_united_sanitizer.h"
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

    TPoolLeaseManager::TPoolLeaseManager(const TVector<TPoolInfo> &poolInfos)
        : PoolInfos(poolInfos)
    {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TPoolLeaseManager::TPoolLeaseManager: poolInfos.size() == ", poolInfos.size());
        TotalLeases = 0;
        for (const auto& poolInfo : poolInfos) {
            TotalLeases += poolInfo.MaxThreadCount - poolInfo.DefaultThreadCount;
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TPoolLeaseManager::TPoolLeaseManager: TotalLeases == ", TotalLeases);
        PoolLeaseRanges.resize(poolInfos.size());
        ui64 totalThreads = 0;
        for (const auto& poolInfo : poolInfos) {
            PoolLeaseRanges[poolInfo.PoolId].Leases = poolInfo.MaxThreadCount - poolInfo.DefaultThreadCount;
            PoolLeaseRanges[poolInfo.PoolId].ThreadBegin = totalThreads;
            PoolLeaseRanges[poolInfo.PoolId].ThreadEnd = totalThreads + poolInfo.DefaultThreadCount;
            ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TPoolLeaseManager::TPoolLeaseManager: PoolLeaseRanges[", poolInfo.PoolId, "]: Leases [", PoolLeaseRanges[poolInfo.PoolId].Leases, "] Threads [", PoolLeaseRanges[poolInfo.PoolId].ThreadBegin, ";", PoolLeaseRanges[poolInfo.PoolId].ThreadEnd, ")");
            totalThreads += poolInfo.DefaultThreadCount;
            if (poolInfo.DefaultThreadCount > 0) {
                PriorityOrder.push_back(poolInfo.PoolId);
            }
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TPoolLeaseManager::TPoolLeaseManager: totalThreads == ", totalThreads);
        Sort(PoolInfos.begin(), PoolInfos.end(), [](const TPoolInfo& a, const TPoolInfo& b) {
            return a.PoolId < b.PoolId;
        });
    }

    LWTRACE_USING(ACTORLIB_PROVIDER);

    TUnitedExecutorPool::TUnitedExecutorPool(
        const TUnitedExecutorPoolConfig& cfg,
        const TVector<TPoolInfo> &poolInfos
    )   : TExecutorPoolBase(Max<ui32>(), SumThreads(poolInfos), new TAffinity(cfg.Affinity), cfg.UseRingQueue)
        , PoolLeaseManager(poolInfos)
        , DefaultSpinThresholdCycles(cfg.SpinThreshold * NHPTimer::GetCyclesPerSecond() * 0.000001) // convert microseconds to cycles
        , PoolName("United")
        , TimePerMailbox(cfg.TimePerMailbox)
        , EventsPerMailbox(cfg.EventsPerMailbox)
        , SoftProcessingDurationTs(cfg.SoftProcessingDurationTs)
        , Threads(new NThreading::TPadded<TUnitedExecutorThreadCtx>[PoolThreads])
        , SpinThresholdCycles(DefaultSpinThresholdCycles)
        , ThreadsState(PoolThreads)
    {
        Y_UNUSED(Barrier);
        ui64 passedThreads = 0;
        for (i16 i = 0; i < static_cast<i16>(PoolLeaseManager.PoolLeaseRanges.size()); ++i) {
            for (i16 j = PoolLeaseManager.PoolLeaseRanges[i].ThreadBegin; j < PoolLeaseManager.PoolLeaseRanges[i].ThreadEnd; ++j) {
                Threads[j].OwnerPoolId = i;
                Threads[j].CurrentPoolId = i;
                Threads[j].SoftDeadlineForPool = 0;
                Threads[j].SoftProcessingDurationTs = Us2Ts(100'000);
                passedThreads++;
            }
        }
        Y_ABORT_UNLESS(passedThreads == static_cast<ui64>(PoolThreads), "Passed threads %" PRIu64 " != PoolThreads %" PRIu64, passedThreads, static_cast<ui64>(PoolThreads));

        Pools.resize(PoolLeaseManager.PoolLeaseRanges.size());
    }

    TUnitedExecutorPool::~TUnitedExecutorPool() {
        Threads.Destroy();
    }

    i16 TUnitedExecutorPool::SumThreads(const TVector<TPoolInfo> &poolInfos) {
        i16 threadCount = 0;
        for (const auto &poolInfo : poolInfos) {
            threadCount += poolInfo.DefaultThreadCount;
        }
        return threadCount;
    }

    i16 TUnitedExecutorPool::FindPoolForWorker(TUnitedExecutorThreadCtx& thread, ui64 revolvingCounter) {
        TWorkerId workerId = Max<TWorkerId>();
        if (TlsThreadContext) {
            workerId = TlsThreadContext->WorkerId;
        }
        for (i16 i : PoolLeaseManager.PriorityOrder) {
            if (Pools[i] == nullptr) {
                ACTORLIB_DEBUG(EDebugLevel::Trace, "Worker_", workerId, " TUnitedExecutorPool::FindPoolForWorker: pool[", i, "] is nullptr; OwnerPoolId == ", thread.OwnerPoolId);
                continue;
            }
            if (PoolLeaseManager.PoolLeaseRanges[i].Leases == 0) {
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::FindPoolForWorker: don't have leases; OwnerPoolId == ", thread.OwnerPoolId);
                continue;
            }
            ui64 semaphore = AtomicGet(Pools[i]->Semaphore);
            if (semaphore == 0) {
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::FindPoolForWorker: pool[", i, "]::Semaphore == 0; OwnerPoolId == ", thread.OwnerPoolId);
                continue;
            }

            if (thread.OwnerPoolId == i) {
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::FindPoolForWorker: ownerPoolId == poolId; ownerPoolId == ", thread.OwnerPoolId, " poolId == ", i);
                return i;
            }

            ui64 leases = Pools[i]->Leases.load(std::memory_order_acquire);

            while (true) {
                if (leases == 0) {
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::FindPoolForWorker: pool[", i, "]::Leases == 0; OwnerPoolId == ", thread.OwnerPoolId);
                    break;
                }
                if (Pools[i]->Leases.compare_exchange_strong(leases, leases - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    ACTORLIB_DEBUG(EDebugLevel::Lease, "Worker_", workerId, " TUnitedExecutorPool::FindPoolForWorker: get lease; ownerPoolId == ", thread.OwnerPoolId, " poolId == ", i, " semaphore == ", semaphore, " leases == ", leases, " -> ", leases - 1);
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::FindPoolForWorker: found pool; ownerPoolId == ", thread.OwnerPoolId, " poolId == ", i, " semaphore == ", semaphore);
                    return i;
                }
            }
        }
        return -1;
    }

    void TUnitedExecutorPool::SwitchToPool(TWorkerContext& wctx, i16 poolId, NHPTimer::STime hpNow) {
        wctx.UpdateThreadTime();
        if (Threads[wctx.WorkerId].CurrentPoolId != poolId && Threads[wctx.WorkerId].CurrentPoolId != Threads[wctx.WorkerId].OwnerPoolId) {
            ui64 leases = Pools[Threads[wctx.WorkerId].CurrentPoolId]->Leases.fetch_add(1, std::memory_order_acq_rel);
            ACTORLIB_DEBUG(EDebugLevel::Lease, "Worker_", wctx.WorkerId, " TUnitedExecutorPool::SwitchToPool: return lease; ownerPoolId == ", Threads[wctx.WorkerId].OwnerPoolId, " currentPoolId == ", Threads[wctx.WorkerId].CurrentPoolId, " poolId = ", poolId, " leases == ", leases, " -> ", leases + 1);
        }
        Threads[wctx.WorkerId].CurrentPoolId = poolId;
        wctx.MailboxTable = Pools[poolId]->MailboxTable.Get();
        wctx.Executor = Pools[poolId];
        TlsThreadContext->Pool = Pools[poolId];
        Threads[wctx.WorkerId].SoftDeadlineForPool = Max<NHPTimer::STime>();
        wctx.Stats = &Pools[poolId]->ThreadStats[wctx.WorkerId];
    }

    TMailbox* TUnitedExecutorPool::GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) {
        Y_UNUSED(SoftProcessingDurationTs);
        NHPTimer::STime hpnow = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard(hpnow);

        TWorkerId workerId = wctx.WorkerId;
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);
        auto &thread = Threads[workerId];
        thread.UnsetWork();
        TMailbox *mailbox = nullptr;
        while (!StopFlag.load(std::memory_order_acquire)) {
            if (hpnow < thread.SoftDeadlineForPool || thread.CurrentPoolId == thread.OwnerPoolId) {
                ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: continue same pool; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                if (thread.SoftDeadlineForPool == Max<NHPTimer::STime>()) {
                    thread.SoftDeadlineForPool = GetCycleCountFast() + thread.SoftProcessingDurationTs;
                }
                mailbox = Pools[thread.CurrentPoolId]->GetReadyActivation(wctx, revolvingCounter);
                if (mailbox) {
                    TlsThreadContext->ProcessedActivationsByCurrentPool++;
                    ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: found mailbox; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                    return mailbox;
                } else if (!wctx.IsNeededToWaitNextActivation) {
                    TlsThreadContext->ProcessedActivationsByCurrentPool++;
                    ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: no mailbox and no need to wait; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                    return nullptr;
                } else {
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: no mailbox and need to find new pool; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " processedActivationsByCurrentPool == ", TlsThreadContext->ProcessedActivationsByCurrentPool);
                    TlsThreadContext->ProcessedActivationsByCurrentPool = 0;
                    if (thread.CurrentPoolId != thread.OwnerPoolId) {
                        SwitchToPool(wctx, thread.OwnerPoolId, hpnow);
                        continue;
                    }
                }
            } else if (!wctx.IsNeededToWaitNextActivation) {
                TlsThreadContext->ProcessedActivationsByCurrentPool++;
                ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: no mailbox and no need to wait; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                return nullptr;
            } else {
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: comeback to owner pool; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " processedActivationsByCurrentPool == ", TlsThreadContext->ProcessedActivationsByCurrentPool, " hpnow == ", hpnow, " softDeadlineForPool == ", thread.SoftDeadlineForPool);
                TlsThreadContext->ProcessedActivationsByCurrentPool = 0;
                SwitchToPool(wctx, thread.OwnerPoolId, hpnow);
                // after soft deadline we check owner pool again
                continue;
            }
            bool goToSleep = true;
            for (i16 attempt = 0; attempt < 1; ++attempt) {
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: attempt == ", attempt, " ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                if (i16 poolId = FindPoolForWorker(thread, revolvingCounter); poolId != -1) {
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: found pool; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                    SwitchToPool(wctx, poolId, hpnow);
                    goToSleep = false;
                    break;
                }
            }
            if (goToSleep) {
                bool allowedToSleep = false;
                ui64 threadsStateRaw = ThreadsState.load(std::memory_order_acquire);
                TThreadsState threadsState = TThreadsState::GetThreadsState(threadsStateRaw);
                while (true) {
                    if (threadsState.Notifications == 0) {
                        threadsState.WorkingThreadCount--;
                        if (ThreadsState.compare_exchange_strong(threadsStateRaw, threadsState.ConvertToUI64(), std::memory_order_acq_rel, std::memory_order_acquire)) {
                            allowedToSleep = true;
                            ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: checked global state; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " notifications == ", threadsState.Notifications, " workingThreadCount: ", threadsState.WorkingThreadCount + 1, " -> ", threadsState.WorkingThreadCount);
                            break;
                        }
                    } else {
                        threadsState.Notifications--;
                        if (ThreadsState.compare_exchange_strong(threadsStateRaw, threadsState.ConvertToUI64(), std::memory_order_acq_rel, std::memory_order_acquire)) {
                            ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: checked global state; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " workingThreadCount == ", threadsState.WorkingThreadCount, " notifications: ", threadsState.Notifications + 1, " -> ", threadsState.Notifications);
                            break;
                        }
                    }
                    threadsState = TThreadsState::GetThreadsState(threadsStateRaw);
                }
                if (allowedToSleep) {
                    ui64 localNotifications = Pools[thread.OwnerPoolId]->LocalNotifications.load(std::memory_order_acquire);
                    while (true) {
                        if (localNotifications == 0) {
                            ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: checked local state; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " localNotifications == 0");
                            break;
                        }
                        if (Pools[thread.OwnerPoolId]->LocalNotifications.compare_exchange_strong(localNotifications, localNotifications - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                            allowedToSleep = false;
                            ThreadsState.fetch_add(1, std::memory_order_acq_rel);
                            ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: checked local state; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " localNotifications: ", localNotifications, " -> ", localNotifications - 1);
                            break;
                        }
                    }
                }
                if (allowedToSleep) {
                    SwitchToPool(wctx, thread.OwnerPoolId, hpnow);
                    Pools[thread.OwnerPoolId]->LocalThreads.fetch_sub(1, std::memory_order_acq_rel);
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: going to sleep; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                    if (thread.Wait(SpinThresholdCycles.load(std::memory_order_relaxed), &StopFlag, &Pools[thread.OwnerPoolId]->LocalNotifications, &ThreadsState)) {
                        ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::GetReadyActivation: interrupted; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                        return nullptr; // interrupted
                    }
                    Pools[thread.OwnerPoolId]->LocalThreads.fetch_add(1, std::memory_order_acq_rel);
                    ThreadsState.fetch_add(1, std::memory_order_acq_rel);
                }
            }
            hpnow = GetCycleCountFast();
        }

        return nullptr;
    }

    void TUnitedExecutorPool::ScheduleActivationEx(TMailbox* mailbox, ui64 revolvingCounter) {
        // unreachable call
        Y_ABORT("ScheduleActivationEx is not allowed for united pool");
    }

    void TUnitedExecutorPool::GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const {
        poolStats.CurrentThreadCount = GetThreadCount();
        poolStats.DefaultThreadCount = GetDefaultThreadCount();
        poolStats.MaxThreadCount = GetMaxThreadCount();
        poolStats.SpinningTimeUs = Ts2Us(SpinningTimeUs);
        poolStats.SpinThresholdUs = Ts2Us(SpinThresholdCycles);
        statsCopy.resize(PoolThreads + 1);
        // Save counters from the pool object
        statsCopy[0] = TExecutorThreadStats();
        statsCopy[0].Aggregate(Stats);
#if defined(ACTORSLIB_COLLECT_EXEC_STATS)
        RecalculateStuckActors(statsCopy[0]);
#endif
        // Per-thread stats
        for (i16 i = 0; i < PoolThreads; ++i) {
            Threads[i].Thread->GetCurrentStats(statsCopy[i + 1]);
        }
    }

    void TUnitedExecutorPool::GetExecutorPoolState(TExecutorPoolState &poolState) const {
        poolState.CurrentLimit = GetThreadCount();
        poolState.MaxLimit = GetMaxThreadCount();
        poolState.MinLimit = GetDefaultThreadCount();
    }

    void TUnitedExecutorPool::Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) {
        TAffinityGuard affinityGuard(Affinity());

        ActorSystem = actorSystem;

        ScheduleReaders.Reset(new NSchedulerQueue::TReader[PoolThreads]);
        ScheduleWriters.Reset(new NSchedulerQueue::TWriter[PoolThreads]);


        for (i16 i = 0; i != PoolThreads; ++i) {
            Y_ABORT_UNLESS(Pools[Threads[i].OwnerPoolId] != nullptr, "Pool is nullptr i %" PRIu16, i);
            Y_ABORT_UNLESS(Threads[i].OwnerPoolId < static_cast<i16>(Pools.size()), "OwnerPoolId is out of range i %" PRIu16 " OwnerPoolId == %" PRIu16, i, Threads[i].OwnerPoolId);
            Y_ABORT_UNLESS(Threads[i].OwnerPoolId >= 0, "OwnerPoolId is out of range i %" PRIu16 " OwnerPoolId == %" PRIu16, i, Threads[i].OwnerPoolId);
            Threads[i].Thread.reset(    
                new TExecutorThread(
                    i,
                    0, // CpuId is not used in BASIC pool
                    actorSystem,
                    Pools[Threads[i].OwnerPoolId],
                    MailboxTable.Get(),
                    PoolName,
                    TimePerMailbox,
                    EventsPerMailbox));
            ScheduleWriters[i].Init(ScheduleReaders[i]);
        }

        *scheduleReaders = ScheduleReaders.Get();
        *scheduleSz = PoolThreads;

        Sanitizer = std::make_unique<TUnitedExecutorPoolSanitizer>(this);
    }

    void TUnitedExecutorPool::Start() {
        TAffinityGuard affinityGuard(Affinity());

        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TUnitedExecutorPool::Start");

        for (i16 i = 0; i != PoolThreads; ++i) {
            Y_ABORT_UNLESS(Threads[i].Thread != nullptr, "Thread is nullptr i %" PRIu16, i);
            Threads[i].Thread->Start();
        }
        Sanitizer->Start();
    }

    void TUnitedExecutorPool::PrepareStop() {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TUnitedExecutorPool::PrepareStop");
        StopFlag.store(true, std::memory_order_release);
        for (i16 i = 0; i != PoolThreads; ++i) {
            Threads[i].Thread->StopFlag.store(true, std::memory_order_release);
            Threads[i].Interrupt();
        }
        Sanitizer->Stop();
    }

    void TUnitedExecutorPool::Shutdown() {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TUnitedExecutorPool::Shutdown");
        for (i16 i = 0; i != PoolThreads; ++i) {
            Threads[i].Thread->Join();
        }
        Sanitizer->Join();
    }

    void TUnitedExecutorPool::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);

        Schedule(deadline - ActorSystem->Timestamp(), ev, cookie, workerId);
    }

    void TUnitedExecutorPool::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);

        const auto current = ActorSystem->Monotonic();
        if (deadline < current)
            deadline = current;

        if (workerId >= 0) {
            ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        } else {
            ScheduleWriters[PoolThreads + 2 + workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        }
    }

    void TUnitedExecutorPool::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);

        const auto deadline = ActorSystem->Monotonic() + delta;
        if (workerId >= 0) {
            ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        } else {
            ScheduleWriters[PoolThreads + 2 + workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        }
    }

    float TUnitedExecutorPool::GetThreadCount() const {
        return PoolThreads;
    }

    float TUnitedExecutorPool::GetDefaultThreadCount() const {
        return PoolThreads;
    }

    float TUnitedExecutorPool::GetMinThreadCount() const {
        return PoolThreads;
    }

    float TUnitedExecutorPool::GetMaxThreadCount() const {
        return PoolThreads;
    }

    ui32 TUnitedExecutorPool::GetThreads() const {
        return PoolThreads;
    }

    void TUnitedExecutorPool::Initialize(TWorkerContext& /*wctx*/) {
    }

    bool TUnitedExecutorPool::TryToWakeUpLocal(i16 ownerPoolId) {
        ui64 localThreads = Pools[ownerPoolId]->LocalThreads.load(std::memory_order_acquire);
        if (localThreads == static_cast<ui64>(PoolLeaseManager.PoolInfos[ownerPoolId].DefaultThreadCount)) {
            return false;
        }

        for (i16 threadId = PoolLeaseManager.PoolLeaseRanges[ownerPoolId].ThreadBegin; threadId < PoolLeaseManager.PoolLeaseRanges[ownerPoolId].ThreadEnd; ++threadId) {
            auto &thread = Threads[threadId];
            if (thread.WakeUp()) {
                TWorkerId workerId = Max<TWorkerId>();
                if (TlsThreadContext) {
                    workerId = TlsThreadContext->WorkerId;
                }
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::TryToWakeUp: wakeup from own pool; ownerPoolId == ", ownerPoolId, " threadId == ", threadId);
                break;
            }
        }
        return true;
    }

    void TUnitedExecutorPool::TryToWakeUp(i16 ownerPoolId) {
        TWorkerId workerId = Max<TWorkerId>();
        if (TlsThreadContext) {
            workerId = TlsThreadContext->WorkerId;
        }
        if (TryToWakeUpLocal(ownerPoolId)) {
            return;
        }
        if (Pools[ownerPoolId]->Config.Leases == 0) {
            return;
        }
        ui64 leases = Pools[ownerPoolId]->Leases.load(std::memory_order_acquire);
        if (leases == 0) {
            return;
        }

        ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TUnitedExecutorPool::TryToWakeUp: ownerPoolId == ", ownerPoolId);
        ui64 threadsStateRaw = ThreadsState.load(std::memory_order_acquire);
        TThreadsState threadsState = TThreadsState::GetThreadsState(threadsStateRaw);
        bool allowedToWakeUp = false;
        bool increaseNotifications = false;
        while (true) {
            if (threadsState.Notifications >= static_cast<ui64>(PoolThreads) && threadsState.WorkingThreadCount == static_cast<ui64>(PoolThreads)) {
                break;
            }
            threadsState.Notifications++;
            increaseNotifications = true;
            allowedToWakeUp = threadsState.WorkingThreadCount < static_cast<ui64>(PoolThreads);
            if (ThreadsState.compare_exchange_strong(threadsStateRaw, threadsState.ConvertToUI64(), std::memory_order_acq_rel, std::memory_order_acquire)) {
                break;
            }
            threadsState = TThreadsState::GetThreadsState(threadsStateRaw);
            increaseNotifications = false;
        }
        if (increaseNotifications) {
            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TUnitedExecutorPool::TryToWakeUp: increase notifications; ", threadsState.Notifications - 1, " -> ", threadsState.Notifications);
        }
        if (!allowedToWakeUp) {
            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TUnitedExecutorPool::TryToWakeUp: no free threads");
            return;
        }

        for (i16 i = static_cast<i16>(PoolLeaseManager.PriorityOrder.size()) - 1; i >= 0; --i) {
            i16 poolId = PoolLeaseManager.PriorityOrder[i];
            for (i16 threadId = PoolLeaseManager.PoolLeaseRanges[poolId].ThreadBegin; threadId < PoolLeaseManager.PoolLeaseRanges[poolId].ThreadEnd; ++threadId) {
                auto &thread = Threads[threadId];
                if (thread.WakeUp()) {
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::TryToWakeUp: wakeup from other pool; ownerPoolId == ", ownerPoolId, " poolId == ", poolId, " threadId == ", threadId);
                    return;
                }
            }
        }
        ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TUnitedExecutorPool::TryToWakeUp: no free threads");
    }

    std::unique_ptr<IExecutorPool> TUnitedExecutorPool::MakePseudoPool(i16 poolId) {
        Y_ABORT_UNLESS(poolId < static_cast<i16>(Pools.size()));
        Y_ABORT_UNLESS(!Pools[poolId]);
        TPsuedoBasicExecutorPoolConfig config = TPsuedoBasicExecutorPoolConfig{
            .PoolId = poolId,
            .DefaultThreadsCount = PoolLeaseManager.PoolInfos[poolId].DefaultThreadCount,
            .MaxThreadsCount = PoolLeaseManager.PoolInfos[poolId].MaxThreadCount,
            .UnitedThreadsCount = PoolThreads,
            .PoolName = PoolLeaseManager.PoolInfos[poolId].PoolName,
            .UseRingQueue = UseRingQueue,
            .Leases = static_cast<ui64>(PoolLeaseManager.PoolLeaseRanges[poolId].Leases)
        };
        auto pool = std::make_unique<TPsuedoBasicExecutorPool>(config, this);
        Pools[poolId] = pool.get();
        return std::unique_ptr<IExecutorPool>(pool.release());
    }

    bool TUnitedExecutorThreadCtx::Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag, std::atomic<ui64> *localNotifications, std::atomic<ui64> *threadsState) {
        NHPTimer::STime start = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_SPIN> activityGuard(start);
        while (true) {
            for (ui32 j = 0;j < 12; ++j) {
                NHPTimer::STime hpnow = GetCycleCountFast();
                if (hpnow >= i64(start + spinThresholdCycles)) {
                    return false;
                }
                for (ui32 i = 0; i < 12; ++i) {
                    EThreadState state = GetState<EThreadState>();
                    if (state == EThreadState::Spin) {
                        TUnitedExecutorPool::TThreadsState threadsStateValue = TUnitedExecutorPool::TThreadsState::GetThreadsState(threadsState->load(std::memory_order_acquire));
                        ui64 localNotificationsValue = localNotifications->load(std::memory_order_acquire);
                        if (threadsStateValue.Notifications == 0 && localNotificationsValue == 0) {
                            SpinLockPause();
                        } else {
                            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", TlsThreadContext->WorkerId, " TUnitedExecutorPool::Spin: wake up from notifications; ownerPoolId == ", OwnerPoolId, " notifications == ", threadsStateValue.Notifications, " localNotifications == ", localNotificationsValue);
                            ExchangeState(EThreadState::None);
                            return true;
                        }
                    } else {
                        return true;
                    }
                }
            }
            if (stopFlag->load(std::memory_order_relaxed)) {
                return true;
            }
        }
        return false;
    }


    bool TUnitedExecutorThreadCtx::Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag, std::atomic<ui64> *localNotifications, std::atomic<ui64> *threadsState) {
        EThreadState state = ExchangeState<EThreadState>(EThreadState::Spin);
        Y_ABORT_UNLESS(state == EThreadState::None, "WaitingFlag# %d", int(state));
        if (spinThresholdCycles > 0) {
            // spin configured period
            if (Spin(spinThresholdCycles, stopFlag, localNotifications, threadsState)) {
                return false;
            }
        }
        TUnitedExecutorPool::TThreadsState threadsStateValue = TUnitedExecutorPool::TThreadsState::GetThreadsState(threadsState->load(std::memory_order_acquire));
        ui64 localNotificationsValue = localNotifications->load(std::memory_order_acquire);
        if (threadsStateValue.Notifications != 0 || localNotificationsValue != 0)
        {
            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", TlsThreadContext->WorkerId, " TUnitedExecutorPool::Wait: wake up from notifications; ownerPoolId == ", OwnerPoolId, " notifications == ", threadsStateValue.Notifications, " localNotifications == ", localNotificationsValue);
            ExchangeState(EThreadState::None);
            return false;
        } else {
            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", TlsThreadContext->WorkerId, " TUnitedExecutorPool::Wait: going to sleep after checking notifications; ownerPoolId == ", OwnerPoolId);
        }
        return Sleep(stopFlag);
    }

}
