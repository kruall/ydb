#include "keyvalue_task_read_prepare.h"
#include "keyvalue_const.h"

#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/task/task_system.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKeyValue::NTask {

namespace {

using NActors::NTask::task;

TReadSharedSnapshotPtr MakeSnapshot(const TVector<std::pair<TString, TString>>& values) {
    auto snapshot = std::make_shared<TReadSharedSnapshot>();
    snapshot->IsActual.store(true, std::memory_order_release);
    snapshot->Epoch = 1;
    snapshot->TabletId = 42;
    snapshot->UserGeneration = 7;
    snapshot->ChannelGeneration = 11;
    snapshot->ChannelStep = 13;

    for (const auto& [key, value] : values) {
        TReadIndexRecordSnapshot record;
        TReadChainItemSnapshot item;
        item.Offset = 0;
        item.InlineData = TRope(value);
        record.Chain.push_back(std::move(item));
        snapshot->Index.emplace(key, std::move(record));
    }

    return snapshot;
}

TReadSharedSnapshotPtr MakeSnapshotWithNonInlineValue(const TString& key, ui32 blobSize) {
    auto snapshot = std::make_shared<TReadSharedSnapshot>();
    snapshot->IsActual.store(true, std::memory_order_release);
    snapshot->Epoch = 1;
    snapshot->TabletId = 42;
    snapshot->UserGeneration = 7;
    snapshot->ChannelGeneration = 11;
    snapshot->ChannelStep = 13;

    TReadIndexRecordSnapshot record;
    TReadChainItemSnapshot item;
    item.Offset = 0;
    item.LogoBlobId = TLogoBlobID(snapshot->TabletId, 1, 1, BLOB_CHANNEL, blobSize, 1);
    record.Chain.push_back(std::move(item));
    snapshot->Index.emplace(key, std::move(record));

    return snapshot;
}

TPrepareReadTaskCommonArgs MakeCommonArgs(const TReadSharedSnapshotPtr& snapshot, const TActorId& respondTo) {
    return TPrepareReadTaskCommonArgs{
        .Snapshot = snapshot,
        .KeyValueActorId = respondTo,
        .RespondTo = respondTo,
        .TraceId = {},
        .UsePayloadInResponse = false,
    };
}

task<void> RunReadTaskAndAssertSuccess(const TActorId& notifyTo, TPrepareReadTaskArgs args,
        TString expectedKey, TString expectedValue)
{
    auto result = co_await RunPrepareReadTask(std::move(args));
    UNIT_ASSERT(!result.NeedsFallback);
    UNIT_ASSERT(result.Intermediate);
    UNIT_ASSERT_VALUES_EQUAL(result.RequestType, TRequestType::ReadOnlyInline);
    UNIT_ASSERT(result.Intermediate->ReadCommand);

    auto* read = std::get_if<TIntermediate::TRead>(&*result.Intermediate->ReadCommand);
    UNIT_ASSERT(read);
    UNIT_ASSERT_VALUES_EQUAL(read->Key, expectedKey);
    UNIT_ASSERT_VALUES_EQUAL(read->CumulativeStatus(), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(read->BuildRope().ConvertToString(), expectedValue);

    NActors::TActivationContext::AsActorContext().Send(notifyTo, new TEvents::TEvWakeup());
    co_return;
}

task<void> RunReadRangeTaskAndAssertSuccess(const TActorId& notifyTo, TPrepareReadRangeTaskArgs args,
        TVector<std::pair<TString, TString>> expected)
{
    auto result = co_await RunPrepareReadRangeTask(std::move(args));
    UNIT_ASSERT(!result.NeedsFallback);
    UNIT_ASSERT(result.Intermediate);
    UNIT_ASSERT_VALUES_EQUAL(result.RequestType, TRequestType::ReadOnlyInline);
    UNIT_ASSERT(result.Intermediate->ReadCommand);

    auto* rangeRead = std::get_if<TIntermediate::TRangeRead>(&*result.Intermediate->ReadCommand);
    UNIT_ASSERT(rangeRead);
    UNIT_ASSERT_VALUES_EQUAL(rangeRead->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(rangeRead->Reads.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(rangeRead->Reads[i].Key, expected[i].first);
        UNIT_ASSERT_VALUES_EQUAL(rangeRead->Reads[i].CumulativeStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(rangeRead->Reads[i].BuildRope().ConvertToString(), expected[i].second);
    }

    NActors::TActivationContext::AsActorContext().Send(notifyTo, new TEvents::TEvWakeup());
    co_return;
}

task<void> RunReadTaskAndAssertFallback(const TActorId& notifyTo, TPrepareReadTaskArgs args) {
    auto result = co_await RunPrepareReadTask(std::move(args));
    UNIT_ASSERT(result.NeedsFallback);
    UNIT_ASSERT(!result.Intermediate);

    NActors::TActivationContext::AsActorContext().Send(notifyTo, new TEvents::TEvWakeup());
    co_return;
}

task<void> RunReadRangeTaskAndAssertFallback(const TActorId& notifyTo, TPrepareReadRangeTaskArgs args) {
    auto result = co_await RunPrepareReadRangeTask(std::move(args));
    UNIT_ASSERT(result.NeedsFallback);
    UNIT_ASSERT(!result.Intermediate);

    NActors::TActivationContext::AsActorContext().Send(notifyTo, new TEvents::TEvWakeup());
    co_return;
}

task<void> RunReadTaskAndAssertNonInlineSuccess(const TActorId& notifyTo, TPrepareReadTaskArgs args,
        TString expectedKey, ui32 expectedBlobSize)
{
    auto result = co_await RunPrepareReadTask(std::move(args));
    UNIT_ASSERT(!result.NeedsFallback);
    UNIT_ASSERT(result.Intermediate);
    UNIT_ASSERT_VALUES_EQUAL(result.RequestType, TRequestType::ReadOnly);
    UNIT_ASSERT(result.Intermediate->ReadCommand);

    auto* read = std::get_if<TIntermediate::TRead>(&*result.Intermediate->ReadCommand);
    UNIT_ASSERT(read);
    UNIT_ASSERT_VALUES_EQUAL(read->Key, expectedKey);
    UNIT_ASSERT_VALUES_EQUAL(read->CumulativeStatus(), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(read->ReadItems.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(read->ReadItems[0].BlobOffset, 0u);
    UNIT_ASSERT_VALUES_EQUAL(read->ReadItems[0].BlobSize, expectedBlobSize);
    UNIT_ASSERT_VALUES_EQUAL(read->ReadItems[0].ValueOffset, 0u);

    NActors::TActivationContext::AsActorContext().Send(notifyTo, new TEvents::TEvWakeup());
    co_return;
}

task<void> RunReadRangeTaskAndAssertMetaOnly(const TActorId& notifyTo, TPrepareReadRangeTaskArgs args,
        TVector<std::pair<TString, ui32>> expected)
{
    auto result = co_await RunPrepareReadRangeTask(std::move(args));
    UNIT_ASSERT(!result.NeedsFallback);
    UNIT_ASSERT(result.Intermediate);
    UNIT_ASSERT_VALUES_EQUAL(result.RequestType, TRequestType::ReadOnlyInline);
    UNIT_ASSERT(result.Intermediate->ReadCommand);

    auto* rangeRead = std::get_if<TIntermediate::TRangeRead>(&*result.Intermediate->ReadCommand);
    UNIT_ASSERT(rangeRead);
    UNIT_ASSERT_VALUES_EQUAL(rangeRead->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(rangeRead->IncludeData, false);
    UNIT_ASSERT_VALUES_EQUAL(rangeRead->Reads.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(rangeRead->Reads[i].Key, expected[i].first);
        UNIT_ASSERT_VALUES_EQUAL(rangeRead->Reads[i].ValueSize, expected[i].second);
        UNIT_ASSERT_VALUES_EQUAL(rangeRead->Reads[i].ReadItems.size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(rangeRead->Reads[i].BuildRope().size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(rangeRead->Reads[i].CumulativeStatus(), NKikimrProto::OK);
    }

    NActors::TActivationContext::AsActorContext().Send(notifyTo, new TEvents::TEvWakeup());
    co_return;
}

} // namespace

Y_UNIT_TEST_SUITE(KeyValueTaskReadPrepare) {

    Y_UNIT_TEST(ReadUsesSnapshotInTask) {
        TTestActorSystem runtime(1);
        runtime.Start();

        auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
        UNIT_ASSERT(actorSystem);

        NActors::NTask::TTaskSystem taskSystem;
        taskSystem.Initialize(actorSystem, 1);

        const TActorId edge = runtime.AllocateEdgeActor(1);
        const auto snapshot = MakeSnapshot({{"key-1", "value-1"}});

        NKikimrKeyValue::ReadRequest request;
        request.set_key("key-1");
        request.set_tablet_id(snapshot->TabletId);

        taskSystem.Enqueue(RunReadTaskAndAssertSuccess(edge, TPrepareReadTaskArgs{
            .Common = MakeCommonArgs(snapshot, edge),
            .Request = std::move(request),
        }, "key-1", "value-1"));

        auto ev = runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(edge);
        UNIT_ASSERT(ev);
    }

    Y_UNIT_TEST(ReadRangeUsesSnapshotInTask) {
        TTestActorSystem runtime(1);
        runtime.Start();

        auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
        UNIT_ASSERT(actorSystem);

        NActors::NTask::TTaskSystem taskSystem;
        taskSystem.Initialize(actorSystem, 1);

        const TActorId edge = runtime.AllocateEdgeActor(1);
        const auto snapshot = MakeSnapshot({{"a", "A"}, {"b", "B"}});

        NKikimrKeyValue::ReadRangeRequest request;
        request.set_tablet_id(snapshot->TabletId);
        request.set_include_data(true);
        request.mutable_range()->set_from_key_inclusive("a");
        request.mutable_range()->set_to_key_inclusive("b");

        taskSystem.Enqueue(RunReadRangeTaskAndAssertSuccess(edge, TPrepareReadRangeTaskArgs{
            .Common = MakeCommonArgs(snapshot, edge),
            .Request = std::move(request),
        }, {{"a", "A"}, {"b", "B"}}));

        auto ev = runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(edge);
        UNIT_ASSERT(ev);
    }

    Y_UNIT_TEST(ReadFallsBackWhenSnapshotIsStale) {
        TTestActorSystem runtime(1);
        runtime.Start();

        auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
        UNIT_ASSERT(actorSystem);

        NActors::NTask::TTaskSystem taskSystem;
        taskSystem.Initialize(actorSystem, 1);

        const TActorId edge = runtime.AllocateEdgeActor(1);
        const auto snapshot = MakeSnapshot({{"key-1", "value-1"}});
        snapshot->Invalidate();

        NKikimrKeyValue::ReadRequest request;
        request.set_key("key-1");
        request.set_tablet_id(snapshot->TabletId);

        taskSystem.Enqueue(RunReadTaskAndAssertFallback(edge, TPrepareReadTaskArgs{
            .Common = MakeCommonArgs(snapshot, edge),
            .Request = std::move(request),
        }));

        auto ev = runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(edge);
        UNIT_ASSERT(ev);
    }

    Y_UNIT_TEST(ReadRangeFallsBackWhenSnapshotIsStale) {
        TTestActorSystem runtime(1);
        runtime.Start();

        auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
        UNIT_ASSERT(actorSystem);

        NActors::NTask::TTaskSystem taskSystem;
        taskSystem.Initialize(actorSystem, 1);

        const TActorId edge = runtime.AllocateEdgeActor(1);
        const auto snapshot = MakeSnapshot({{"a", "A"}, {"b", "B"}});
        snapshot->Invalidate();

        NKikimrKeyValue::ReadRangeRequest request;
        request.set_tablet_id(snapshot->TabletId);
        request.set_include_data(true);
        request.mutable_range()->set_from_key_inclusive("a");
        request.mutable_range()->set_to_key_inclusive("b");

        taskSystem.Enqueue(RunReadRangeTaskAndAssertFallback(edge, TPrepareReadRangeTaskArgs{
            .Common = MakeCommonArgs(snapshot, edge),
            .Request = std::move(request),
        }));

        auto ev = runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(edge);
        UNIT_ASSERT(ev);
    }

    Y_UNIT_TEST(ReadFallsBackOnLockGenerationMismatch) {
        TTestActorSystem runtime(1);
        runtime.Start();

        auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
        UNIT_ASSERT(actorSystem);

        NActors::NTask::TTaskSystem taskSystem;
        taskSystem.Initialize(actorSystem, 1);

        const TActorId edge = runtime.AllocateEdgeActor(1);
        const auto snapshot = MakeSnapshot({{"key-1", "value-1"}});

        NKikimrKeyValue::ReadRequest request;
        request.set_key("key-1");
        request.set_tablet_id(snapshot->TabletId);
        request.set_lock_generation(snapshot->UserGeneration + 1);

        taskSystem.Enqueue(RunReadTaskAndAssertFallback(edge, TPrepareReadTaskArgs{
            .Common = MakeCommonArgs(snapshot, edge),
            .Request = std::move(request),
        }));

        auto ev = runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(edge);
        UNIT_ASSERT(ev);
    }

    Y_UNIT_TEST(ReadRangeFallsBackOnLockGenerationMismatch) {
        TTestActorSystem runtime(1);
        runtime.Start();

        auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
        UNIT_ASSERT(actorSystem);

        NActors::NTask::TTaskSystem taskSystem;
        taskSystem.Initialize(actorSystem, 1);

        const TActorId edge = runtime.AllocateEdgeActor(1);
        const auto snapshot = MakeSnapshot({{"a", "A"}, {"b", "B"}});

        NKikimrKeyValue::ReadRangeRequest request;
        request.set_tablet_id(snapshot->TabletId);
        request.set_include_data(true);
        request.set_lock_generation(snapshot->UserGeneration + 1);
        request.mutable_range()->set_from_key_inclusive("a");
        request.mutable_range()->set_to_key_inclusive("b");

        taskSystem.Enqueue(RunReadRangeTaskAndAssertFallback(edge, TPrepareReadRangeTaskArgs{
            .Common = MakeCommonArgs(snapshot, edge),
            .Request = std::move(request),
        }));

        auto ev = runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(edge);
        UNIT_ASSERT(ev);
    }

    Y_UNIT_TEST(ReadRangeWithoutDataUsesSnapshotInTask) {
        TTestActorSystem runtime(1);
        runtime.Start();

        auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
        UNIT_ASSERT(actorSystem);

        NActors::NTask::TTaskSystem taskSystem;
        taskSystem.Initialize(actorSystem, 1);

        const TActorId edge = runtime.AllocateEdgeActor(1);
        const auto snapshot = MakeSnapshot({{"a", "AAA"}, {"b", "BBBB"}});

        NKikimrKeyValue::ReadRangeRequest request;
        request.set_tablet_id(snapshot->TabletId);
        request.set_include_data(false);
        request.mutable_range()->set_from_key_inclusive("a");
        request.mutable_range()->set_to_key_inclusive("b");

        taskSystem.Enqueue(RunReadRangeTaskAndAssertMetaOnly(edge, TPrepareReadRangeTaskArgs{
            .Common = MakeCommonArgs(snapshot, edge),
            .Request = std::move(request),
        }, {{"a", 3u}, {"b", 4u}}));

        auto ev = runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(edge);
        UNIT_ASSERT(ev);
    }

    Y_UNIT_TEST(ReadUsesReadOnlyRequestTypeForNonInlineData) {
        TTestActorSystem runtime(1);
        runtime.Start();

        auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
        UNIT_ASSERT(actorSystem);

        NActors::NTask::TTaskSystem taskSystem;
        taskSystem.Initialize(actorSystem, 1);

        const TActorId edge = runtime.AllocateEdgeActor(1);
        const auto snapshot = MakeSnapshotWithNonInlineValue("key-blob", 16);

        NKikimrKeyValue::ReadRequest request;
        request.set_key("key-blob");
        request.set_tablet_id(snapshot->TabletId);

        taskSystem.Enqueue(RunReadTaskAndAssertNonInlineSuccess(edge, TPrepareReadTaskArgs{
            .Common = MakeCommonArgs(snapshot, edge),
            .Request = std::move(request),
        }, "key-blob", 16u));

        auto ev = runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(edge);
        UNIT_ASSERT(ev);
    }

} // Y_UNIT_TEST_SUITE(KeyValueTaskReadPrepare)

} // namespace NKikimr::NKeyValue::NTask
