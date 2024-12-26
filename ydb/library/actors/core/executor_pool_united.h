#pragma once

#include "actorsystem.h"
#include "config.h"
#include "executor_thread.h"
#include "executor_thread_ctx.h"
#include "executor_pool_basic_feature_flags.h"
#include "executor_pool_basic.h"
#include "executor_pool_base.h"
#include "scheduler_queue.h"
#include <memory>
#include <ydb/library/actors/actor_type/indexes.h>
#include <ydb/library/actors/util/unordered_cache.h>
#include <ydb/library/actors/util/threadparkpad.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <library/cpp/threading/chunk_queue/queue.h>

#include <util/system/mutex.h>

#include <queue>

namespace NActors {

    class TExecutorPoolJail;
    class TUnitedExecutorPoolSanitizer;

    class TPsuedoBasicExecutorPool;

    struct TPoolInfo {
        i16 PoolId = 0;
        i16 DefaultThreadCount = 0;
        i16 MaxThreadCount = 0;
        TString PoolName;
    };

    struct TPoolLeaseRange {
        i16 Leases;
        i16 ThreadBegin;
        i16 ThreadEnd;
    };

    struct TPoolLease {
        i16 PoolId;
        std::atomic<bool> IsAcquired;
    };

    struct TPoolLeaseManager {
        TStackVec<TPoolInfo, 8> PoolInfos;
        TStackVec<TPoolLeaseRange, 8> PoolLeaseRanges;
        TStackVec<i16, 8> PriorityOrder;
        ui64 TotalLeases;
    
        TPoolLeaseManager(const TVector<TPoolInfo> &poolInfos);
    };

    class TUnitedExecutorPool: public TExecutorPoolBase {
        friend class TPsuedoBasicExecutorPool;
        friend class TUnitedExecutorPoolSanitizer;

        TPoolLeaseManager PoolLeaseManager;
        TStackVec<TPsuedoBasicExecutorPool*> Pools;
        std::unique_ptr<TUnitedExecutorPoolSanitizer> Sanitizer;


        TArrayHolder<NSchedulerQueue::TReader> ScheduleReaders;
        TArrayHolder<NSchedulerQueue::TWriter> ScheduleWriters;

        const ui64 DefaultSpinThresholdCycles;
        const TString PoolName;
        const TDuration TimePerMailbox;
        const ui32 EventsPerMailbox;
        const ui64 SoftProcessingDurationTs;

        char Barrier[64];

        TArrayHolder<NThreading::TPadded<TUnitedExecutorThreadCtx>> Threads;
        static_assert(sizeof(std::decay_t<decltype(Threads[0])>) == 2 * PLATFORM_CACHE_LINE);

        alignas(64) NThreading::TPadded<std::atomic<ui64>> SpinThresholdCycles;
        alignas(64) NThreading::TPadded<std::atomic<ui64>> SpinningTimeUs;
        alignas(64) NThreading::TPadded<std::atomic<ui64>> ThreadsState;

        const ui32 ActorSystemIndex = NActors::TActorTypeOperator::GetActorSystemIndex();
    public:
        struct TThreadsState {
            ui64 WorkingThreadCount = 0;
            ui64 Notifications = 0;

            inline ui64 ConvertToUI64() {
                ui64 value = WorkingThreadCount;
                return value
                    | ((ui64)Notifications << 32);
            }

            static inline TThreadsState GetThreadsState(i64 value) {
                TThreadsState state;
                state.WorkingThreadCount = value & 0x7fffffff;
                state.Notifications = (value >> 32) & 0x7fffffff;
                return state;
            }
        };

        static constexpr TDuration DEFAULT_TIME_PER_MAILBOX = TBasicExecutorPoolConfig::DEFAULT_TIME_PER_MAILBOX;
        static constexpr ui32 DEFAULT_EVENTS_PER_MAILBOX = TBasicExecutorPoolConfig::DEFAULT_EVENTS_PER_MAILBOX;

        explicit TUnitedExecutorPool(const TUnitedExecutorPoolConfig& cfg, const TVector<TPoolInfo> &poolInfos);
        ~TUnitedExecutorPool();

        i16 SumThreads(const TVector<TPoolInfo> &poolInfos);
        void Initialize(TWorkerContext& wctx) override;
        i16 FindPoolForWorker(TUnitedExecutorThreadCtx& thread, ui64 revolvingReadCounter);
        TMailbox* GetReadyActivation(TWorkerContext& wctx, ui64 revolvingReadCounter) override;

        void SwitchToPool(TWorkerContext& wctx, i16 poolId, NHPTimer::STime hpNow);

        void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;

        void ScheduleActivationEx(TMailbox* mailbox, ui64 revolvingWriteCounter) override;

        void Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) override;
        void Start() override;
        void PrepareStop() override;
        void Shutdown() override;

        void GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const override;
        void GetExecutorPoolState(TExecutorPoolState &poolState) const override;
        TString GetName() const override {
            return PoolName;
        }

        ui32 GetThreads() const override;
        float GetThreadCount() const override;
        float GetDefaultThreadCount() const override;
        float GetMinThreadCount() const override;
        float GetMaxThreadCount() const override;

        void TryToWakeUp(i16 poolId);
        bool TryToWakeUpLocal(i16 poolId);

        std::unique_ptr<IExecutorPool> MakePseudoPool(i16 poolId);
    };
}
