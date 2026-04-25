#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(MultiGet) {

    struct TRangeIndexBenchmarkResult {
        TString Query;
        TDuration PutTime;
        TDuration Duration;
        ui32 Blobs = 0;
        ui32 ResponsesPerRead = 0;
        ui64 Ops = 0;
        ui64 Responses = 0;
    };

    struct TRangeIndexBenchmarkQuery {
        TString Name;
        TLogoBlobID From;
        TLogoBlobID To;
        ui32 ExpectedResponses = 0;
    };

    TVector<TRangeIndexBenchmarkResult> RunRangeIndexBenchmark(bool enableFlatEvents) {
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
        const TDuration benchmarkDuration = TDuration::Seconds(60);
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

        const TVector<TRangeIndexBenchmarkQuery> queries{
            {
                .Name = "full-forward",
                .From = TLogoBlobID(tabletId, 0, 0, 0, 0, 0),
                .To = TLogoBlobID(tabletId, Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                    TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie),
                .ExpectedResponses = blobsToSend,
            },
            {
                .Name = "full-backward",
                .From = TLogoBlobID(tabletId, Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                    TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie),
                .To = TLogoBlobID(tabletId, 0, 0, 0, 0, 0),
                .ExpectedResponses = blobsToSend,
            },
            {
                .Name = "small-forward",
                .From = TLogoBlobID(tabletId, 1, 100, 0, 0, 0),
                .To = TLogoBlobID(tabletId, 1, 199, 0, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie),
                .ExpectedResponses = 100,
            },
            {
                .Name = "point",
                .From = TLogoBlobID(tabletId, 1, 123, 0, buffer.size(), 0),
                .To = TLogoBlobID(tabletId, 1, 123, 0, buffer.size(), 0),
                .ExpectedResponses = 1,
            },
            {
                .Name = "empty",
                .From = TLogoBlobID(tabletId, 2, 0, 0, 0, 0),
                .To = TLogoBlobID(tabletId, 2, Max<ui32>(), TLogoBlobID::MaxChannel, TLogoBlobID::MaxBlobSize,
                    TLogoBlobID::MaxCookie),
                .ExpectedResponses = 0,
            },
        };

        TVector<TRangeIndexBenchmarkResult> results;
        const TDuration queryDuration = TDuration::MicroSeconds(benchmarkDuration.MicroSeconds() / queries.size());
        const double queryDurationSeconds = static_cast<double>(queryDuration.MicroSeconds()) / 1'000'000;
        for (const auto& query : queries) {
            THPTimer readTimer;
            ui64 ops = 0;
            while (readTimer.Passed() < queryDurationSeconds) {
                runtime->WrapInActorContext(edge, [&] {
                    SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvRange(tabletId, query.From, query.To, false,
                        TInstant::Max(), true));
                });
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge, false);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses.size(), query.ExpectedResponses);
                ++ops;
            }
            const TDuration duration = TDuration::Seconds(readTimer.Passed());
            results.push_back({
                .Query = query.Name,
                .PutTime = putTime,
                .Duration = duration,
                .Blobs = blobsToSend,
                .ResponsesPerRead = query.ExpectedResponses,
                .Ops = ops,
                .Responses = ops * query.ExpectedResponses,
            });
        }

        return results;
    }

    void PrintRangeIndexBenchmarkResult(TStringBuf name, const TRangeIndexBenchmarkResult& result) {
        Cerr << "RangeIndexBenchmark " << name
            << " query# " << result.Query
            << " blobs# " << result.Blobs
            << " responsesPerRead# " << result.ResponsesPerRead
            << " ops# " << result.Ops
            << " responses# " << result.Responses
            << " putTime# " << result.PutTime
            << " duration# " << result.Duration
            << " opsPerSecond# " << (result.Duration.MicroSeconds() ?
                result.Ops * 1'000'000 / result.Duration.MicroSeconds() : 0)
            << " responsesPerSecond# " << (result.Duration.MicroSeconds() ?
                result.Responses * 1'000'000 / result.Duration.MicroSeconds() : 0)
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

        UNIT_ASSERT_VALUES_EQUAL(proto.size(), flat.size());
        for (ui32 i = 0; i < proto.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(proto[i].Query, flat[i].Query);
            PrintRangeIndexBenchmarkResult("proto", proto[i]);
            PrintRangeIndexBenchmarkResult("flat", flat[i]);
            if (proto[i].Ops) {
                Cerr << "RangeIndexBenchmark query# " << proto[i].Query
                    << " flatToProtoOpsRatio# "
                    << static_cast<double>(flat[i].Ops) / proto[i].Ops
                    << Endl;
            }
        }
    }

}
