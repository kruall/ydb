#include <ydb/core/base/group_stat.h>
#include <ydb/core/mind/bscontroller/stat_processor.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NBsController;

namespace {

    class TStatProcessorTest {
        TTestActorSystem Runtime{1};
        TActorId ParentId;
        TActorId StatProcessorId;

    public:
        TStatProcessorTest() {
            Runtime.Start();
            ParentId = Runtime.AllocateEdgeActor(1);
            StatProcessorId = Runtime.Register(CreateStatProcessorActor(), ParentId, 0, std::nullopt, 1);
        }

        ~TStatProcessorTest() {
            Runtime.Stop();
        }

        void UpdateGroup(TGroupId groupId, std::initializer_list<TActorId> vdiskServiceIds) {
            auto ev = std::make_unique<TEvControllerNotifyGroupChange>();
            auto& ids = ev->Updated[groupId];
            ids.insert(vdiskServiceIds.begin(), vdiskServiceIds.end());
            Send(std::move(ev));
        }

        void DeleteGroup(TGroupId groupId) {
            auto ev = std::make_unique<TEvControllerNotifyGroupChange>();
            ev->Deleted.push_back(groupId);
            Send(std::move(ev));
        }

        void Report(TGroupId groupId, const TActorId& vdiskServiceId) {
            TGroupStat stat;
            stat.Update(TGroupStat::EKind::PUT_TABLET_LOG, TDuration::MilliSeconds(5), TInstant::Seconds(1));

            auto ev = std::make_unique<TEvBlobStorage::TEvControllerUpdateGroupStat>();
            auto *report = ev->Record.AddPerGroupReport();
            report->SetGroupId(groupId.GetRawId());
            ActorIdToProto(vdiskServiceId, report->MutableVDiskServiceId());
            stat.Serialize(report);
            Send(std::move(ev));
        }

        TGroupLatencyStats WaitForLatencyUpdate(TGroupId groupId) {
            auto ev = Runtime.WaitForEdgeActorEvent<TEvControllerCommitGroupLatencies>(ParentId, false);
            const auto it = ev->Get()->Updates.find(groupId);
            UNIT_ASSERT_C(it != ev->Get()->Updates.end(), "missing group latency update");
            return it->second;
        }

        void Advance(TDuration duration) {
            Runtime.Schedule(duration,
                new IEventHandle(ParentId, ParentId, new TEvents::TEvWakeup),
                nullptr, 1);
            Runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(ParentId, false);
        }

    private:
        template<typename TEvent>
        void Send(std::unique_ptr<TEvent>&& ev) {
            Runtime.Send(new IEventHandle(StatProcessorId, ParentId, ev.release()), 1);
        }
    };

    void AssertHasLatency(const TGroupLatencyStats& stats) {
        UNIT_ASSERT(stats.PutTabletLog);
    }

    void AssertHasNoLatency(const TGroupLatencyStats& stats) {
        UNIT_ASSERT(!stats.PutTabletLog);
        UNIT_ASSERT(!stats.PutUserData);
        UNIT_ASSERT(!stats.GetFast);
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(TStatProcessorTestSuite) {

    Y_UNIT_TEST(ExpiresStatsWithoutNewReports) {
        TStatProcessorTest test;
        const TGroupId groupId = TGroupId::FromValue(0x80000001);
        const TActorId vdiskServiceId = MakeBlobStorageVDiskID(1, 1, 1);

        test.UpdateGroup(groupId, {vdiskServiceId});
        test.Report(groupId, vdiskServiceId);
        AssertHasLatency(test.WaitForLatencyUpdate(groupId));
        AssertHasNoLatency(test.WaitForLatencyUpdate(groupId));
    }

    Y_UNIT_TEST(RejectsLateReportFromRemovedVDisk) {
        TStatProcessorTest test;
        const TGroupId groupId = TGroupId::FromValue(0x80000002);
        const TActorId vdiskServiceId = MakeBlobStorageVDiskID(1, 1, 2);

        test.UpdateGroup(groupId, {vdiskServiceId});
        test.Report(groupId, vdiskServiceId);
        AssertHasLatency(test.WaitForLatencyUpdate(groupId));

        test.UpdateGroup(groupId, {});
        test.Report(groupId, vdiskServiceId);
        AssertHasNoLatency(test.WaitForLatencyUpdate(groupId));
    }

    Y_UNIT_TEST(GroupDeletionPurgesCleanupSchedule) {
        TStatProcessorTest test;
        const TGroupId groupId = TGroupId::FromValue(0x80000003);
        const TActorId vdiskServiceId = MakeBlobStorageVDiskID(1, 1, 3);

        test.UpdateGroup(groupId, {vdiskServiceId});
        test.Report(groupId, vdiskServiceId);
        test.Advance(TDuration::Seconds(1));

        test.DeleteGroup(groupId);
        test.UpdateGroup(groupId, {vdiskServiceId});
        test.Report(groupId, vdiskServiceId);
        AssertHasLatency(test.WaitForLatencyUpdate(groupId));

        // Old report expired at t=30. Recreated group's report expires after t=31.
        // A stale schedule item would clear the new report before this t=35 marker.
        test.Advance(TDuration::Seconds(25));
        AssertHasNoLatency(test.WaitForLatencyUpdate(groupId));
    }
}
