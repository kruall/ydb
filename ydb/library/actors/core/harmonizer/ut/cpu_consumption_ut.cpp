#include "cpu_consumption.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace {

class TMockExecutorPool : public IExecutorPool {
public:
    TMockExecutorPool(ui32 poolId, TString name, float threadCount, i16 fullThreadCount)
        : IExecutorPool(poolId)
        , Name(std::move(name))
        , ThreadCount(threadCount)
        , FullThreadCount(fullThreadCount)
    {
    }

    TString Name;
    float ThreadCount = 0.0f;
    i16 FullThreadCount = 0;

    void Prepare(TActorSystem*, NSchedulerQueue::TReader**, ui32*) override {}
    void Start() override {}
    void PrepareStop() override {}
    void Shutdown() override {}
    bool Cleanup() override { return true; }

    TMailbox* GetReadyActivation(ui64) override { return nullptr; }
    TMailbox* ResolveMailbox(ui32) override { return nullptr; }

    void Schedule(TInstant, TAutoPtr<IEventHandle>, ISchedulerCookie*, TWorkerId) override {}
    void Schedule(TMonotonic, TAutoPtr<IEventHandle>, ISchedulerCookie*, TWorkerId) override {}
    void Schedule(TDuration, TAutoPtr<IEventHandle>, ISchedulerCookie*, TWorkerId) override {}

    bool Send(TAutoPtr<IEventHandle>&) override { return true; }
    bool SpecificSend(TAutoPtr<IEventHandle>&) override { return true; }
    void ScheduleActivation(TMailbox*) override {}
    void SpecificScheduleActivation(TMailbox*) override {}
    void ScheduleActivationEx(TMailbox*, ui64) override {}
    TActorId Register(IActor*, TMailboxType::EType, ui64, const TActorId&) override { return TActorId(); }
    TActorId Register(IActor*, TMailboxCache&, ui64, const TActorId&) override { return TActorId(); }
    TActorId Register(IActor*, TMailbox*, const TActorId&) override { return TActorId(); }
    TActorId RegisterAlias(TMailbox*, IActor*) override { return TActorId(); }
    void UnregisterAlias(TMailbox*, const TActorId&) override {}

    TAffinity* Affinity() const override { return nullptr; }
    TString GetName() const override { return Name; }
    float GetThreadCount() const override { return ThreadCount; }
    i16 GetFullThreadCount() const override { return FullThreadCount; }
    i16 GetDefaultFullThreadCount() const override { return FullThreadCount; }
    i16 GetMinFullThreadCount() const override { return FullThreadCount; }
    i16 GetMaxFullThreadCount() const override { return FullThreadCount; }
    float GetDefaultThreadCount() const override { return ThreadCount; }
    float GetMinThreadCount() const override { return ThreadCount; }
    float GetMaxThreadCount() const override { return ThreadCount; }
    ui64 TimePerMailboxTs() const override { return 1; }
    ui32 EventsPerMailbox() const override { return 1; }
};

void RegisterHistory(TValueHistory<8>& history, ui64 ts, double value) {
    history.Register(1, 0.0);
    history.Register(ts, value * 8.0);
}

std::unique_ptr<TPoolInfo> MakePoolInfo(
    TMockExecutorPool* pool,
    EHarmonizerPoolKind kind,
    i16 maxFullThreadCount,
    i16 sharedThreadCount)
{
    auto poolInfo = std::make_unique<TPoolInfo>();
    poolInfo->Pool = pool;
    poolInfo->HarmonizerPoolKind = kind;
    poolInfo->MaxFullThreadCount = maxFullThreadCount;
    poolInfo->ThreadInfo.resize(maxFullThreadCount);
    poolInfo->SharedInfo.resize(sharedThreadCount);
    return poolInfo;
}

TSharedInfo MakeSharedInfo(i16 poolCount, float freeCpu) {
    TSharedInfo sharedInfo;
    sharedInfo.PoolCount = poolCount;
    sharedInfo.CpuConsumption.resize(poolCount);
    sharedInfo.FreeCpu = freeCpu;
    return sharedInfo;
}

} // namespace

Y_UNIT_TEST_SUITE(CpuConsumptionTests) {

    Y_UNIT_TEST(RegularPoolCanBecomeNeedyBeforeExactSaturation) {
        const ui64 ts = Us2Ts(8.0 * 1'000'000);

        TMockExecutorPool pool(0, "Regular", 4.0f, 4);
        auto poolInfo = MakePoolInfo(&pool, EHarmonizerPoolKind::Regular, 4, 0);
        for (auto& thread : poolInfo->ThreadInfo) {
            RegisterHistory(thread.ElapsedCpu, ts, 0.9);
            RegisterHistory(thread.UsedCpu, ts, 0.9);
        }
        poolInfo->NewNotEnoughCpuExecutions = 1;

        std::vector<std::unique_ptr<TPoolInfo>> pools;
        pools.push_back(std::move(poolInfo));

        THarmonizerCpuConsumption cpuConsumption;
        cpuConsumption.Init(pools.size());
        cpuConsumption.Pull(pools, MakeSharedInfo(pools.size(), 0.0f));

        UNIT_ASSERT_VALUES_EQUAL(cpuConsumption.IsNeedyByPool.size(), 1);
        UNIT_ASSERT(cpuConsumption.IsNeedyByPool[0]);
        UNIT_ASSERT_VALUES_EQUAL(cpuConsumption.NeedyPools.size(), 1);
    }

    Y_UNIT_TEST(ForeignSharedOnlyPoolIsNotNeedyWhileSharedCpuIsAvailable) {
        const ui64 ts = Us2Ts(8.0 * 1'000'000);

        TMockExecutorPool pool(0, "Borrowed", 1.0f, 0);
        auto poolInfo = MakePoolInfo(&pool, EHarmonizerPoolKind::ForeignSharedOnly, 0, 1);
        RegisterHistory(poolInfo->SharedInfo[0].ElapsedCpu, ts, 0.9);
        RegisterHistory(poolInfo->SharedInfo[0].UsedCpu, ts, 0.9);
        poolInfo->NewNotEnoughCpuExecutions = 1;

        std::vector<std::unique_ptr<TPoolInfo>> pools;
        pools.push_back(std::move(poolInfo));

        THarmonizerCpuConsumption cpuConsumption;
        cpuConsumption.Init(pools.size());
        cpuConsumption.Pull(pools, MakeSharedInfo(pools.size(), 0.5f));

        UNIT_ASSERT_VALUES_EQUAL(cpuConsumption.IsNeedyByPool.size(), 1);
        UNIT_ASSERT(!cpuConsumption.IsNeedyByPool[0]);
        UNIT_ASSERT(cpuConsumption.NeedyPools.empty());
        UNIT_ASSERT(cpuConsumption.HoggishPools.empty());
    }

    Y_UNIT_TEST(ForeignSharedOnlyPoolBecomesNeedyWhenSharedCpuIsExhausted) {
        const ui64 ts = Us2Ts(8.0 * 1'000'000);

        TMockExecutorPool pool(0, "Borrowed", 1.0f, 0);
        auto poolInfo = MakePoolInfo(&pool, EHarmonizerPoolKind::ForeignSharedOnly, 0, 1);
        RegisterHistory(poolInfo->SharedInfo[0].ElapsedCpu, ts, 0.9);
        RegisterHistory(poolInfo->SharedInfo[0].UsedCpu, ts, 0.9);
        poolInfo->NewNotEnoughCpuExecutions = 1;

        std::vector<std::unique_ptr<TPoolInfo>> pools;
        pools.push_back(std::move(poolInfo));

        THarmonizerCpuConsumption cpuConsumption;
        cpuConsumption.Init(pools.size());
        cpuConsumption.Pull(pools, MakeSharedInfo(pools.size(), 0.0f));

        UNIT_ASSERT_VALUES_EQUAL(cpuConsumption.IsNeedyByPool.size(), 1);
        UNIT_ASSERT(cpuConsumption.IsNeedyByPool[0]);
        UNIT_ASSERT_VALUES_EQUAL(cpuConsumption.NeedyPools.size(), 1);
        UNIT_ASSERT(cpuConsumption.HoggishPools.empty());
    }
}
