#include "executor_pool_shared.h"

#include "actorsystem.h"
#include "config.h"
#include "debug.h"
#include "executor_pool_basic.h"
#include "executor_thread.h"
#include "executor_thread_ctx.h"

#include <atomic>
#include <ydb/library/actors/util/affinity.h>


namespace NActors {

class TSharedExecutorPool: public ISharedExecutorPool {
public:
    TSharedExecutorPool(const TSharedExecutorPoolConfig &config, i16 poolCount, std::vector<i16> poolsWithThreads);

    virtual ~TSharedExecutorPool() override;
    // IThreadPool
    void Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) override;
    void Start() override;
    void PrepareStop() override;
    void Shutdown() override;
    bool Cleanup() override;

    TSharedExecutorThreadCtx *GetSharedThread(i16 poolId) override;
    void GetSharedStats(i16 pool, std::vector<TExecutorThreadStats>& statsCopy) override;
    void GetSharedStatsForHarmonizer(i16 pool, std::vector<TExecutorThreadStats>& statsCopy) override;
    TCpuConsumption GetThreadCpuConsumption(i16 poolId, i16 threadIdx) override;
    std::vector<TCpuConsumption> GetThreadsCpuConsumption(i16 poolId) override;

    i16 ReturnOwnHalfThread(i16 pool) override;
    i16 ReturnBorrowedHalfThread(i16 pool) override;
    void GiveHalfThread(i16 from, i16 to) override;

    i16 GetSharedThreadCount() const override;

    TSharedPoolState GetState() const override;

    void Init(const std::vector<IExecutorPool*>& pools, bool withThreads) override;

private:
    TSharedPoolState State;

    TActorSystem* ActorSystem;

    std::vector<IExecutorPool*> Pools;

    i16 PoolCount;
    i16 SharedThreadCount;
    std::unique_ptr<TSharedExecutorThreadCtx[]> Threads;

    std::unique_ptr<NSchedulerQueue::TReader[]> ScheduleReaders;
    std::unique_ptr<NSchedulerQueue::TWriter[]> ScheduleWriters;

    TDuration TimePerMailbox;
    ui32 EventsPerMailbox;
    ui64 SoftProcessingDurationTs;
}; // class TSharedExecutorPool

TSharedExecutorPool::TSharedExecutorPool(const TSharedExecutorPoolConfig &config, i16 poolCount, std::vector<i16> poolsWithThreads)
    : State(poolCount, poolsWithThreads.size())
    , Pools(poolCount)
    , PoolCount(poolCount)
    , SharedThreadCount(poolsWithThreads.size())
    , Threads(new TSharedExecutorThreadCtx[SharedThreadCount])
    , TimePerMailbox(config.TimePerMailbox)
    , EventsPerMailbox(config.EventsPerMailbox)
    , SoftProcessingDurationTs(config.SoftProcessingDurationTs)
{
    ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::ctor: start");
    for (ui32 poolIdx = 0, threadIdx = 0; poolIdx < poolsWithThreads.size(); ++poolIdx, ++threadIdx) {
        Y_ABORT_UNLESS(poolsWithThreads[poolIdx] < poolCount);
        State.ThreadByPool[poolsWithThreads[poolIdx]] = threadIdx;
        State.PoolByThread[threadIdx] = poolsWithThreads[poolIdx];
    }
    ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::ctor: end");
}

TSharedExecutorPool::~TSharedExecutorPool() {
    ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::dtor start");
    Threads.reset();
    ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::dtor end");
}

void TSharedExecutorPool::Init(const std::vector<IExecutorPool*>& pools, bool withThreads) {
    ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::Init: start");
    std::vector<IExecutorPool*> poolByThread(SharedThreadCount);
    for (IExecutorPool* pool : pools) {
        Pools[pool->PoolId] = pool;
        i16 threadIdx = State.ThreadByPool[pool->PoolId];
        if (threadIdx >= 0) {
            poolByThread[threadIdx] = pool;
        }
    }

    for (i16 i = 0; i != SharedThreadCount; ++i) {
        // !TODO
        Threads[i].ExecutorPools[0].store(dynamic_cast<TBasicExecutorPool*>(poolByThread[i]), std::memory_order_release);
        if (withThreads) {
            Threads[i].Thread.reset(
                new TSharedExecutorThread(
                    -1,
                    ActorSystem,
                    &Threads[i],
                    PoolCount,
                    "SharedThread",
                    SoftProcessingDurationTs,
                    TimePerMailbox,
                    EventsPerMailbox
                )
            );
        }
    }
}

void TSharedExecutorPool::Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) {
    ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::Prepare: start");
    ScheduleReaders.reset(new NSchedulerQueue::TReader[SharedThreadCount]);
    ScheduleWriters.reset(new NSchedulerQueue::TWriter[SharedThreadCount]);

    ActorSystem = actorSystem;
    std::vector<IExecutorPool*> poolsBasic = actorSystem->GetBasicExecutorPools();
    Init(poolsBasic, true);

    for (i16 i = 0; i != SharedThreadCount; ++i) {
        ScheduleWriters[i].Init(ScheduleReaders[i]);
    }

    *scheduleReaders = ScheduleReaders.get();
    *scheduleSz = SharedThreadCount;
}

void TSharedExecutorPool::Start() {
    ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::Start: start");
    //ThreadUtilization = 0;
    //AtomicAdd(MaxUtilizationCounter, -(i64)GetCycleCountFast());

    for (i16 i = 0; i != SharedThreadCount; ++i) {
        Threads[i].Thread->Start();
    }
}

void TSharedExecutorPool::PrepareStop() {
    ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::PrepareStop: start");
    for (i16 i = 0; i != SharedThreadCount; ++i) {
        Threads[i].Thread->StopFlag = true;
        Threads[i].Interrupt();
    }
    ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::PrepareStop: end");
}

void TSharedExecutorPool::Shutdown() {
    ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::Shutdown: start");
    for (i16 i = 0; i != SharedThreadCount; ++i) {
        Threads[i].Thread->Join();
    }
    ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::Shutdown: end");
}

bool TSharedExecutorPool::Cleanup() {
    ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::Cleanup");
    return true;
}

TSharedExecutorThreadCtx* TSharedExecutorPool::GetSharedThread(i16 pool) {
    i16 threadIdx = State.ThreadByPool[pool];
    if (threadIdx < 0 || threadIdx >= PoolCount) {
        return nullptr;
    }
    return &Threads[threadIdx];
}

i16 TSharedExecutorPool::ReturnOwnHalfThread(i16 pool) {
    ACTORLIB_DEBUG(EDebugLevel::Executor, "TSharedExecutorPool::ReturnOwnHalfThread: start");
    i16 threadIdx = State.ThreadByPool[pool];
    IExecutorPool* borrowingPool = Threads[threadIdx].ExecutorPools[1].exchange(nullptr, std::memory_order_acq_rel);
    Y_ABORT_UNLESS(borrowingPool);
    i16 borrowedPool = State.PoolByBorrowedThread[threadIdx];
    State.BorrowedThreadByPool[borrowedPool] = -1;
    State.PoolByBorrowedThread[threadIdx] = -1;
    // TODO(kruall): Check on race
    borrowingPool->ReleaseSharedThread();
    return borrowedPool;
}

i16 TSharedExecutorPool::ReturnBorrowedHalfThread(i16 pool) {
    ACTORLIB_DEBUG(EDebugLevel::Executor, "TSharedExecutorPool::ReturnBorrowedHalfThread: start");
    i16 threadIdx = State.BorrowedThreadByPool[pool];
    IExecutorPool* borrowingPool = Threads[threadIdx].ExecutorPools[1].exchange(nullptr, std::memory_order_acq_rel);
    Y_ABORT_UNLESS(borrowingPool);
    State.BorrowedThreadByPool[State.PoolByBorrowedThread[threadIdx]] = -1;
    State.PoolByBorrowedThread[threadIdx] = -1;
    // TODO(kruall): Check on race
    borrowingPool->ReleaseSharedThread();
    return State.PoolByThread[threadIdx];
}

void TSharedExecutorPool::GiveHalfThread(i16 from, i16 to) {
    ACTORLIB_DEBUG(EDebugLevel::Executor, "TSharedExecutorPool::GiveHalfThread: start");
    if (from == to) {
        return;
    }
    i16 borrowedThreadIdx = State.BorrowedThreadByPool[from];
    if (borrowedThreadIdx != -1) {
        i16 originalPool = State.PoolByThread[borrowedThreadIdx];
        if (originalPool == to) {
            ReturnOwnHalfThread(to);
        } else {
            ReturnOwnHalfThread(originalPool);
        }
        from = originalPool;
    }
    i16 threadIdx = State.ThreadByPool[from];
    IExecutorPool* borrowingPool = Pools[to];
    Threads[threadIdx].ExecutorPools[1].store(borrowingPool, std::memory_order_release);
    State.BorrowedThreadByPool[to] = threadIdx;
    State.PoolByBorrowedThread[threadIdx] = to;
    // TODO(kruall): Check on race
    borrowingPool->AddSharedThread(&Threads[threadIdx]);
}

void TSharedExecutorPool::GetSharedStats(i16 poolId, std::vector<TExecutorThreadStats>& statsCopy) {
    statsCopy.resize(SharedThreadCount);
    for (i16 i = 0; i < SharedThreadCount; ++i) {
        Threads[i].Thread->GetSharedStats(poolId, statsCopy[i]);
    }
}

void TSharedExecutorPool::GetSharedStatsForHarmonizer(i16 poolId, std::vector<TExecutorThreadStats>& statsCopy) {
    statsCopy.resize(SharedThreadCount);
    for (i16 i = 0; i < SharedThreadCount; ++i) {
        Threads[i].Thread->GetSharedStatsForHarmonizer(poolId, statsCopy[i]);
    }
}

TCpuConsumption TSharedExecutorPool::GetThreadCpuConsumption(i16 poolId, i16 threadIdx) {
    if (threadIdx >= SharedThreadCount) {
        return {0.0, 0.0};
    }
    TExecutorThreadStats stats;
    Threads[threadIdx].Thread->GetSharedStatsForHarmonizer(poolId, stats);
    return {Ts2Us(stats.SafeElapsedTicks), static_cast<double>(stats.CpuUs), stats.NotEnoughCpuExecutions};
}

std::vector<TCpuConsumption> TSharedExecutorPool::GetThreadsCpuConsumption(i16 poolId) {
    std::vector<TCpuConsumption> result;
    for (i16 threadIdx = 0; threadIdx < SharedThreadCount; ++threadIdx) {
        result.push_back(GetThreadCpuConsumption(poolId, threadIdx));
    }
    return result;
}

i16 TSharedExecutorPool::GetSharedThreadCount() const {
    return SharedThreadCount;
}

TSharedPoolState TSharedExecutorPool::GetState() const {
    return State;
}

ISharedExecutorPool *CreateSharedExecutorPool(const TSharedExecutorPoolConfig &config, i16 poolCount, std::vector<i16> poolsWithThreads) {
    return new TSharedExecutorPool(config, poolCount, poolsWithThreads);
}

TString TSharedPoolState::ToString() const {
    TStringBuilder builder;
    builder << '{';
    builder << "ThreadByPool: [";
    for (ui32 i = 0; i < ThreadByPool.size(); ++i) {
        builder << ThreadByPool[i] << (i == ThreadByPool.size() - 1 ? "" : ", ");
    }
    builder << "], ";
    builder << "PoolByThread: [";
    for (ui32 i = 0; i < PoolByThread.size(); ++i) {
        builder << PoolByThread[i] << (i == PoolByThread.size() - 1 ? "" : ", ");
    }
    builder << "], ";
    builder << "BorrowedThreadByPool: [";
    for (ui32 i = 0; i < BorrowedThreadByPool.size(); ++i) {
        builder << BorrowedThreadByPool[i] << (i == BorrowedThreadByPool.size() - 1 ? "" : ", ");
    }
    builder << "], ";
    builder << "PoolByBorrowedThread: [";
    for (ui32 i = 0; i < PoolByBorrowedThread.size(); ++i) {
        builder << PoolByBorrowedThread[i] << (i == PoolByBorrowedThread.size() - 1 ? "" : ", ");
    }
    builder << ']';
    return builder << '}';
}

}
