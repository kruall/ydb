#include "keyvalue_task_read.h"
#include "keyvalue_const.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/task/task_system.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/algorithm.h>

namespace NKikimr::NKeyValue::NTask {

namespace {

using NActors::NTask::task;

struct TFakeProxyState {
    NKikimrProto::EReplyStatus Status = NKikimrProto::OK;
    ui32 GroupId = 0;
    THashMap<TLogoBlobID, TString> BlobData;
};

class TFakeProxyActor final : public NActors::TActor<TFakeProxyActor> {
public:
    explicit TFakeProxyActor(TFakeProxyState& state)
        : TActor(&TThis::StateWork)
        , State_(state)
    {}

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGet, HandleGet);
        }
    }

private:
    void HandleGet(TEvBlobStorage::TEvGet::TPtr& ev) {
        auto result = std::make_unique<TEvBlobStorage::TEvGetResult>(
            State_.Status,
            ev->Get()->QuerySize,
            State_.GroupId);

        for (ui32 queryIdx = 0; queryIdx < ev->Get()->QuerySize; ++queryIdx) {
            const auto& query = ev->Get()->Queries[queryIdx];
            auto& response = result->Responses[queryIdx];
            response.Id = query.Id;
            response.Shift = query.Shift;
            response.RequestedSize = query.Size;

            if (State_.Status != NKikimrProto::OK) {
                response.Status = State_.Status;
                continue;
            }

            const auto it = State_.BlobData.find(query.Id);
            if (it == State_.BlobData.end()) {
                response.Status = NKikimrProto::NODATA;
                continue;
            }

            const TString& data = it->second;
            const ui64 shift = Min<ui64>(query.Shift, data.size());
            const ui64 size = Min<ui64>(query.Size, data.size() - shift);
            response.Status = NKikimrProto::OK;
            response.Buffer = TRope(data.substr(shift, size));
        }

        Send(ev->Sender, result.release(), 0, ev->Cookie);
    }

private:
    TFakeProxyState& State_;
};

void StartRuntimeWithSnapshotSubsystem(TTestActorSystem& runtime) {
    auto* node = runtime.GetNode(1);
    UNIT_ASSERT(node);
    runtime.SetupNode(1, *node);
    node->ActorSystem->RegisterSubSystem(std::make_unique<TReadSharedSnapshotSubSystem>());
    runtime.StartNode(1);
}

std::shared_ptr<TReadSharedSnapshot> MakeSnapshot(ui64 tabletId, ui32 groupId) {
    auto snapshot = std::make_shared<TReadSharedSnapshot>();
    snapshot->TabletId = tabletId;
    snapshot->UserGeneration = 1;
    snapshot->ChannelGeneration = 1;
    snapshot->ChannelStep = 1;
    snapshot->TabletInfo = new TTabletStorageInfo(tabletId, TTabletTypes::KeyValue);
    snapshot->TabletInfo->Channels.emplace_back(0, TBlobStorageGroupType::ErasureNone);
    snapshot->TabletInfo->Channels.emplace_back(1, TBlobStorageGroupType::ErasureNone);
    snapshot->TabletInfo->Channels.emplace_back(BLOB_CHANNEL, TBlobStorageGroupType::ErasureNone);
    snapshot->TabletInfo->Channels[BLOB_CHANNEL].History.emplace_back(1, groupId);
    snapshot->IsActual.store(true, std::memory_order_release);
    return snapshot;
}

task<void> RunReadTaskAndSendTo(const NActors::TActorId& target, TReadTaskArgs args) {
    NKikimrKeyValue::ReadRequest fallback = args.Request;
    auto result = co_await RunReadTask(std::move(args));
    auto& ctx = NActors::TActivationContext::AsActorContext();
    if (result.NeedsFallback || !result.Response) {
        auto* ev = new TEvKeyValue::TEvRead();
        ev->Record = std::move(fallback);
        ctx.Send(target, ev);
    } else {
        ctx.Send(target, result.Response.release());
    }
    co_return;
}

TReadTaskArgs MakeReadTaskArgs(ui64 tabletId, TString key) {
    TReadTaskArgs args;
    args.TabletId = tabletId;
    args.Request.set_key(key);
    args.Request.set_cookie(17);
    args.Request.set_priority(NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
    return args;
}

} // namespace

Y_UNIT_TEST_SUITE(KeyValueTaskRead) {

Y_UNIT_TEST(UsesInlineSnapshotPath) {
    TTestActorSystem runtime(1);
    StartRuntimeWithSnapshotSubsystem(runtime);

    auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
    UNIT_ASSERT(actorSystem);
    auto* subSystem = actorSystem->GetSubSystem<TReadSharedSnapshotSubSystem>();
    UNIT_ASSERT(subSystem);

    constexpr ui64 tabletId = 1001;
    constexpr ui32 groupId = 7001;
    auto snapshot = MakeSnapshot(tabletId, groupId);
    TReadIndexRecordSnapshot record;
    record.CreationUnixTime = 10;
    TReadChainItemSnapshot inlineItem;
    inlineItem.LogoBlobId = TLogoBlobID();
    inlineItem.InlineData = TRope("inline-value");
    inlineItem.Offset = 0;
    record.Chain.push_back(std::move(inlineItem));
    snapshot->Index.emplace("key-inline", std::move(record));
    subSystem->Update(tabletId, snapshot);

    NActors::NTask::TTaskSystem taskSystem;
    taskSystem.Initialize(actorSystem, 1);

    const TActorId edge = runtime.AllocateEdgeActor(1);
    auto args = MakeReadTaskArgs(tabletId, "key-inline");
    args.RespondTo = edge;
    args.KeyValueActorId = edge;

    taskSystem.Enqueue(RunReadTaskAndSendTo(edge, std::move(args)));

    auto ev = runtime.WaitForEdgeActorEvent<TEvKeyValue::TEvReadResponse>(edge);
    UNIT_ASSERT(ev);
    UNIT_ASSERT_VALUES_EQUAL(
        static_cast<int>(ev->Get()->Record.status()),
        static_cast<int>(NKikimrKeyValue::Statuses::RSTATUS_OK));
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.value(), "inline-value");
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.cookie(), 17);
}

Y_UNIT_TEST(ReadsBlobDataBySnapshotPath) {
    TTestActorSystem runtime(1);
    StartRuntimeWithSnapshotSubsystem(runtime);

    auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
    UNIT_ASSERT(actorSystem);
    auto* subSystem = actorSystem->GetSubSystem<TReadSharedSnapshotSubSystem>();
    UNIT_ASSERT(subSystem);

    constexpr ui64 tabletId = 2002;
    constexpr ui32 groupId = 8002;
    auto snapshot = MakeSnapshot(tabletId, groupId);

    const TLogoBlobID id(tabletId, 1, 42, BLOB_CHANNEL, 5, 1);
    TReadIndexRecordSnapshot record;
    record.CreationUnixTime = 11;
    TReadChainItemSnapshot blobItem;
    blobItem.LogoBlobId = id;
    blobItem.Offset = 0;
    record.Chain.push_back(std::move(blobItem));
    snapshot->Index.emplace("key-blob", std::move(record));
    subSystem->Update(tabletId, snapshot);

    TFakeProxyState proxyState;
    proxyState.Status = NKikimrProto::OK;
    proxyState.GroupId = groupId;
    proxyState.BlobData[id] = "hello";

    const TActorId proxyActor = runtime.Register(new TFakeProxyActor(proxyState), 1);
    runtime.RegisterService(MakeBlobStorageProxyID(groupId), proxyActor);

    NActors::NTask::TTaskSystem taskSystem;
    taskSystem.Initialize(actorSystem, 1);

    const TActorId edge = runtime.AllocateEdgeActor(1);
    auto args = MakeReadTaskArgs(tabletId, "key-blob");
    args.RespondTo = edge;
    args.KeyValueActorId = edge;

    taskSystem.Enqueue(RunReadTaskAndSendTo(edge, std::move(args)));

    auto ev = runtime.WaitForEdgeActorEvent<TEvKeyValue::TEvReadResponse>(edge);
    UNIT_ASSERT(ev);
    UNIT_ASSERT_VALUES_EQUAL(
        static_cast<int>(ev->Get()->Record.status()),
        static_cast<int>(NKikimrKeyValue::Statuses::RSTATUS_OK));
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.value(), "hello");
}

Y_UNIT_TEST(FallsBackWhenSnapshotIsNotActual) {
    TTestActorSystem runtime(1);
    StartRuntimeWithSnapshotSubsystem(runtime);

    auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
    UNIT_ASSERT(actorSystem);
    auto* subSystem = actorSystem->GetSubSystem<TReadSharedSnapshotSubSystem>();
    UNIT_ASSERT(subSystem);

    constexpr ui64 tabletId = 3003;
    constexpr ui32 groupId = 9003;
    auto snapshot = MakeSnapshot(tabletId, groupId);
    snapshot->Invalidate();
    subSystem->Update(tabletId, snapshot);

    NActors::NTask::TTaskSystem taskSystem;
    taskSystem.Initialize(actorSystem, 1);

    const TActorId edge = runtime.AllocateEdgeActor(1);
    auto args = MakeReadTaskArgs(tabletId, "missing");
    args.RespondTo = edge;
    args.KeyValueActorId = edge;

    taskSystem.Enqueue(RunReadTaskAndSendTo(edge, std::move(args)));

    auto ev = runtime.WaitForEdgeActorEvent<TEvKeyValue::TEvRead>(edge);
    UNIT_ASSERT(ev);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.key(), "missing");
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.cookie(), 17);
}

Y_UNIT_TEST(FallsBackOnBlobReadError) {
    TTestActorSystem runtime(1);
    StartRuntimeWithSnapshotSubsystem(runtime);

    auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
    UNIT_ASSERT(actorSystem);
    auto* subSystem = actorSystem->GetSubSystem<TReadSharedSnapshotSubSystem>();
    UNIT_ASSERT(subSystem);

    constexpr ui64 tabletId = 4004;
    constexpr ui32 groupId = 9104;
    auto snapshot = MakeSnapshot(tabletId, groupId);

    const TLogoBlobID id(tabletId, 1, 50, BLOB_CHANNEL, 4, 1);
    TReadIndexRecordSnapshot record;
    record.CreationUnixTime = 12;
    TReadChainItemSnapshot blobItem;
    blobItem.LogoBlobId = id;
    blobItem.Offset = 0;
    record.Chain.push_back(std::move(blobItem));
    snapshot->Index.emplace("key-error", std::move(record));
    subSystem->Update(tabletId, snapshot);

    TFakeProxyState proxyState;
    proxyState.Status = NKikimrProto::ERROR;
    proxyState.GroupId = groupId;
    const TActorId proxyActor = runtime.Register(new TFakeProxyActor(proxyState), 1);
    runtime.RegisterService(MakeBlobStorageProxyID(groupId), proxyActor);

    NActors::NTask::TTaskSystem taskSystem;
    taskSystem.Initialize(actorSystem, 1);

    const TActorId edge = runtime.AllocateEdgeActor(1);
    auto args = MakeReadTaskArgs(tabletId, "key-error");
    args.RespondTo = edge;
    args.KeyValueActorId = edge;

    taskSystem.Enqueue(RunReadTaskAndSendTo(edge, std::move(args)));

    auto ev = runtime.WaitForEdgeActorEvent<TEvKeyValue::TEvRead>(edge);
    UNIT_ASSERT(ev);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.key(), "key-error");
}

} // Y_UNIT_TEST_SUITE(KeyValueTaskRead)

} // namespace NKikimr::NKeyValue::NTask
