#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(MultiGet) {

    struct TRangeIndexBenchmarkResult {
        TDuration PutTime;
        TDuration ReadTime;
        ui32 Blobs = 0;
        ui32 Reads = 0;
    };

    TRangeIndexBenchmarkResult RunRangeIndexBenchmark(bool enableFlatEvents) {
        TEnvironmentSetup env(false, TBlobStorageGroupType::Erasure4Plus2Block);
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool();
        if (enableFlatEvents) {
            env.SetIcbControl(0, "BlobStorage.EnableVDiskFlatEvents", true);
        }
        const ui32 groupId = env.GetGroups().front();
        const TActorId& edge = runtime->AllocateEdgeActor(1);

        constexpr ui64 tabletId = 1;
        constexpr ui32 blobsToSend = 2'000;
        constexpr ui32 reads = 50;
        const TString buffer = "A SMALL BLOB 16b";

        THPTimer putTimer;
        for (ui32 i = 1; i <= blobsToSend; ++i) {
            const TLogoBlobID id(tabletId, 1, i, 0, buffer.size(), 0);
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max()));
            });
        }
        for (ui32 i = 0; i < blobsToSend; ++i) {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }
        const TDuration putTime = TDuration::Seconds(putTimer.Passed());

        const TLogoBlobID from(tabletId, 0, 0, 0, 0, 0);
        const TLogoBlobID to(tabletId, Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel, TLogoBlobID::MaxBlobSize,
            TLogoBlobID::MaxCookie);

        THPTimer readTimer;
        for (ui32 i = 0; i < reads; ++i) {
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvRange(tabletId, from, to, false, TInstant::Max(),
                    true));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses.size(), blobsToSend);
        }
        const TDuration readTime = TDuration::Seconds(readTimer.Passed());

        return {
            .PutTime = putTime,
            .ReadTime = readTime,
            .Blobs = blobsToSend,
            .Reads = reads,
        };
    }

    void PrintRangeIndexBenchmarkResult(TStringBuf name, const TRangeIndexBenchmarkResult& result) {
        const ui64 responses = static_cast<ui64>(result.Blobs) * result.Reads;
        Cerr << "RangeIndexBenchmark " << name
            << " blobs# " << result.Blobs
            << " reads# " << result.Reads
            << " responses# " << responses
            << " putTime# " << result.PutTime
            << " readTime# " << result.ReadTime
            << " readUsPerResponse# " << (responses ? result.ReadTime.MicroSeconds() / responses : 0)
            << Endl;
    }

    void RunSequentialGet(bool enableFlatEvents) {
        TEnvironmentSetup env(false, TBlobStorageGroupType::Erasure4Plus2Block);
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool();
        if (enableFlatEvents) {
            env.SetIcbControl(0, "BlobStorage.EnableVDiskFlatEvents", true);
        }
        const ui32 groupId = env.GetGroups().front();

        const TActorId& edge = runtime->AllocateEdgeActor(1);
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        ui32 numInFlight = 0;

        constexpr ui32 blobsToSend = 10'000;

        for (ui32 i = 1; i <= blobsToSend; ++i) {
            const TString buffer = "A SMALL BLOB 16b";
            const TLogoBlobID id(1, 1, i, 0, buffer.size(), 0);
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max()));
            });
            ++numInFlight;
        }

        for (; numInFlight > 0; --numInFlight) {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        auto rusage = TRusage::Get();
        const ui64 rssOnBegin = rusage.MaxRss;
        Cerr << "rssOnBegin# " << rssOnBegin << Endl;

        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvRange(1, TLogoBlobID(1, 0, 0, 0, 0, 0),
                TLogoBlobID(1, Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel, TLogoBlobID::MaxBlobSize,
                TLogoBlobID::MaxCookie), false, TInstant::Max()));
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge, false);
            UNIT_ASSERT_EQUAL(res->Get()->Responses.size(), blobsToSend);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        rusage = TRusage::Get();
        const ui64 rssOnEnd = rusage.MaxRss;

        Cerr << rssOnBegin << " -> " << rssOnEnd << Endl;
    }

    Y_UNIT_TEST(SequentialGet) {
        RunSequentialGet(false);
    }

    Y_UNIT_TEST(SequentialGetFlatEvents) {
        RunSequentialGet(true);
    }

    Y_UNIT_TEST(RangeIndexProtoVsFlatBenchmark) {
        const auto proto = RunRangeIndexBenchmark(false);
        const auto flat = RunRangeIndexBenchmark(true);

        PrintRangeIndexBenchmarkResult("proto", proto);
        PrintRangeIndexBenchmarkResult("flat", flat);
        if (flat.ReadTime.MicroSeconds()) {
            Cerr << "RangeIndexBenchmark protoToFlatReadRatio# "
                << static_cast<double>(proto.ReadTime.MicroSeconds()) / flat.ReadTime.MicroSeconds()
                << Endl;
        }
    }

}
