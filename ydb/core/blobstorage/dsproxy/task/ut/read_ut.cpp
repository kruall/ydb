#include <ydb/core/blobstorage/dsproxy/task/read.h>

#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/task/task_system.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBlobStorage::NDSProxy::NTask {

    namespace {

        using NActors::NTask::task;

        struct TFakeProxyState {
            NKikimrProto::EReplyStatus Status = NKikimrProto::OK;
            ui32 GroupId = 1;
            ui32 Requests = 0;
            ui32 LastRestartCounter = 0;
            std::optional<ui32> LastForceGroupGeneration = std::nullopt;
            bool ReturnPayload = false;
            TString Payload = {};
        };

        class TFakeProxyActor final : public NActors::TActor<TFakeProxyActor> {
        public:
            explicit TFakeProxyActor(TFakeProxyState& state)
                : TActor(&TThis::StateWork)
                , State_(state)
            {
            }

            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvBlobStorage::TEvGet, HandleGet);
                }
            }

        private:
            void HandleGet(TEvBlobStorage::TEvGet::TPtr& ev) {
                ++State_.Requests;
                State_.LastRestartCounter = ev->Get()->RestartCounter;
                State_.LastForceGroupGeneration = ev->Get()->ForceGroupGeneration;

                if (State_.ReturnPayload) {
                    auto result = std::make_unique<TEvBlobStorage::TEvGetResult>(State_.Status, 1, State_.GroupId);
                    auto& response = result->Responses[0];
                    response.Status = State_.Status;
                    response.Id = ev->Get()->Queries[0].Id;
                    response.Shift = 0;
                    response.RequestedSize = State_.Payload.size();
                    response.Buffer = TRope(State_.Payload);
                    Send(ev->Sender, result.release(), 0, ev->Cookie);
                    return;
                }

                auto result = std::make_unique<TEvBlobStorage::TEvGetResult>(State_.Status, 0, State_.GroupId);
                Send(ev->Sender, result.release(), 0, ev->Cookie);
            }

        private:
            TFakeProxyState& State_;
        };

        struct TFakeQueueState {
            NKikimrProto::EReplyStatus Status = NKikimrProto::BLOCKED;
            ui32 Requests = 0;
            TString Payload = "queue-payload";
        };

        class TFakeQueueActor final : public NActors::TActor<TFakeQueueActor> {
        public:
            explicit TFakeQueueActor(TFakeQueueState& state)
                : TActor(&TThis::StateWork)
                , State_(state)
            {
            }

            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvBlobStorage::TEvVGet, HandleVGet);
                }
            }

        private:
            void HandleVGet(TEvBlobStorage::TEvVGet::TPtr& ev) {
                ++State_.Requests;
                const auto& request = ev->Get()->Record;
                UNIT_ASSERT(ev->Get()->Record.HasVDiskID());
                const TVDiskID vdiskId = VDiskIDFromVDiskID(ev->Get()->Record.GetVDiskID());
                auto result = std::make_unique<TEvBlobStorage::TEvVGetResult>(
                    State_.Status,
                    vdiskId,
                    TAppData::TimeProvider->Now(),
                    ui32(0),
                    nullptr,
                    nullptr,
                    nullptr,
                    nullptr,
                    TMaybe<ui64>(),
                    ui32(0),
                    ui64(0));

                if (State_.Status == NKikimrProto::OK) {
                    UNIT_ASSERT(request.ExtremeQueriesSize() > 0);
                    for (const auto& query : request.GetExtremeQueries()) {
                        UNIT_ASSERT(query.HasId());
                        const auto id = LogoBlobIDFromLogoBlobID(query.GetId());
                        const ui64 shift = query.HasShift() ? query.GetShift() : 0;
                        const ui32 size = query.HasSize() ? query.GetSize() : id.BlobSize();

                        ui64 queryCookie = 0;
                        const ui64* cookiePtr = nullptr;
                        if (query.HasCookie()) {
                            queryCookie = query.GetCookie();
                            cookiePtr = &queryCookie;
                        }

                        TString data = State_.Payload;
                        if (data.empty()) {
                            data = "q";
                        }
                        if (data.size() < size) {
                            data.append(size - data.size(), 'q');
                        } else if (data.size() > size) {
                            data.resize(size);
                        }

                        result->AddResult(NKikimrProto::OK, id, shift, TRope(std::move(data)), cookiePtr);
                    }
                }
                Send(ev->Sender, result.release(), 0, ev->Cookie);
            }

        private:
            TFakeQueueState& State_;
        };

        std::unique_ptr<TEvBlobStorage::TEvGet> MakeGetRequest() {
            return std::make_unique<TEvBlobStorage::TEvGet>(
                TLogoBlobID(1, 1, 1, 0, 16, 0),
                0,
                16,
                TInstant::Max(),
                NKikimrBlobStorage::FastRead);
        }

        std::unique_ptr<TEvBlobStorage::TEvGet> MakePartialGetRequest() {
            return std::make_unique<TEvBlobStorage::TEvGet>(
                TLogoBlobID(1, 1, 1, 0, 64, 0),
                8,
                16,
                TInstant::Max(),
                NKikimrBlobStorage::FastRead);
        }

        task<void> RunReadTaskAndSendTo(const NActors::TActorId& target, TReadTaskArgs args) {
            auto result = co_await RunReadTask(std::move(args));
            UNIT_ASSERT(result);
            NActors::TActivationContext::AsActorContext().Send(target, result.release());
            co_return;
        }

        void StartRuntimeWithSharedStateSubsystem(TTestActorSystem& runtime) {
            auto* node = runtime.GetNode(1);
            UNIT_ASSERT(node);
            runtime.SetupNode(1, *node);
            node->ActorSystem->RegisterSubSystem(std::make_unique<TBlobStorageGroupSharedStateSubSystem>());
            runtime.StartNode(1);
        }

        TBlobStorageGroupSharedStatePtr MakeReadySharedState(TTestActorSystem& runtime, const TActorId& queueActorId) {
            TVector<TActorId> vdisks = {runtime.AllocateEdgeActor(1)};
            auto groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(
                TBlobStorageGroupType::ErasureNone,
                ui32(1),
                ui32(1),
                ui32(1),
                &vdisks);
            auto groupQueues = MakeIntrusive<TGroupQueues>(groupInfo->GetTopology());

            for (auto& failDomain : groupQueues->FailDomains) {
                for (auto& vdisk : failDomain.VDisks) {
                    auto& queues = vdisk.Queues;
                    queues.PutTabletLog.ActorId = queueActorId;
                    queues.PutAsyncBlob.ActorId = queueActorId;
                    queues.PutUserData.ActorId = queueActorId;
                    queues.GetAsyncRead.ActorId = queueActorId;
                    queues.GetFastRead.ActorId = queueActorId;
                    queues.GetDiscover.ActorId = queueActorId;
                    queues.GetLowRead.ActorId = queueActorId;
                }
            }

            return std::make_shared<TBlobStorageGroupSharedState>(TBlobStorageGroupSharedState{
                .ConnectionEpoch = 1,
                .GroupGeneration = groupInfo->GroupGeneration,
                .IsReadyForGet = true,
                .GroupInfo = groupInfo,
                .GroupQueues = groupQueues,
                .NodeLayout = nullptr,
                .AccelerationParams = {},
                .LongRequestThreshold = TDuration::Seconds(1),
            });
        }

    } // namespace

    Y_UNIT_TEST_SUITE(ReadTask) {

        Y_UNIT_TEST(ForwardsToProxyWhenSharedStateMissing) {
            TTestActorSystem runtime(1);
            runtime.Start();

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            TFakeProxyState proxyState{
                .Status = NKikimrProto::OK,
                .GroupId = 777,
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            auto executionRelay = std::make_shared<TEvBlobStorage::TExecutionRelay>();
            taskSystem.Enqueue(RunReadTaskAndSendTo(edgeId, TReadTaskArgs{
                .GroupId = proxyState.GroupId,
                .Request = {
                    .Event = MakeGetRequest(),
                    .RestartCounter = 42,
                    .ExecutionRelay = executionRelay,
                    .ForceGroupGeneration = 123u,
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GroupId, 777);
            UNIT_ASSERT_EQUAL(ev->Get()->ExecutionRelay, executionRelay);

            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 1u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastRestartCounter, 42u);
            UNIT_ASSERT(proxyState.LastForceGroupGeneration);
            UNIT_ASSERT_VALUES_EQUAL(*proxyState.LastForceGroupGeneration, 123u);
        }

        Y_UNIT_TEST(ForwardsToProxyWhenSharedStateNotFound) {
            TTestActorSystem runtime(1);
            StartRuntimeWithSharedStateSubsystem(runtime);

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            TFakeProxyState proxyState{
                .Status = NKikimrProto::ERROR,
                .GroupId = 11,
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            taskSystem.Enqueue(RunReadTaskAndSendTo(edgeId, TReadTaskArgs{
                .GroupId = proxyState.GroupId,
                .Request = {
                    .Event = MakeGetRequest(),
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::ERROR);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GroupId, 11);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 1u);
        }

        Y_UNIT_TEST(RequestEventNullReturnsErrorWithoutFallback) {
            TTestActorSystem runtime(1);
            runtime.Start();

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            TFakeProxyState proxyState{
                .Status = NKikimrProto::OK,
                .GroupId = 42,
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            taskSystem.Enqueue(RunReadTaskAndSendTo(edgeId, TReadTaskArgs{
                .GroupId = proxyState.GroupId,
                .Request = {
                    .Event = nullptr,
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::ERROR);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 0u);
        }

        Y_UNIT_TEST(ForwardsProxyPayloadAsIs) {
            TTestActorSystem runtime(1);
            runtime.Start();

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            TFakeProxyState proxyState{
                .Status = NKikimrProto::OK,
                .GroupId = 13,
                .ReturnPayload = true,
                .Payload = "proxy-payload",
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            taskSystem.Enqueue(RunReadTaskAndSendTo(edgeId, TReadTaskArgs{
                .GroupId = proxyState.GroupId,
                .Request = {
                    .Event = MakeGetRequest(),
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ResponseSz, 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Buffer.ConvertToString(), TString("proxy-payload"));
            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 1u);
        }

        Y_UNIT_TEST(ForwardsToProxyWithGroupIdZeroByDefault) {
            TTestActorSystem runtime(1);
            runtime.Start();

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            TFakeProxyState proxyState{
                .Status = NKikimrProto::OK,
                .GroupId = 0,
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            taskSystem.Enqueue(RunReadTaskAndSendTo(edgeId, TReadTaskArgs{
                .Request = {
                    .Event = MakeGetRequest(),
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GroupId, 0u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 1u);
        }

        Y_UNIT_TEST(UsesReadySharedStatePathWithoutFallback) {
            TTestActorSystem runtime(1);
            StartRuntimeWithSharedStateSubsystem(runtime);

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            TFakeQueueState queueState{
                .Status = NKikimrProto::BLOCKED,
            };
            const NActors::TActorId queueActorId = runtime.Register(new TFakeQueueActor(queueState), 1);

            constexpr ui32 groupId = 51;
            auto* sharedStateSubSystem = actorSystem->GetSubSystem<TBlobStorageGroupSharedStateSubSystem>();
            UNIT_ASSERT(sharedStateSubSystem);
            sharedStateSubSystem->Update(groupId, MakeReadySharedState(runtime, queueActorId));

            TFakeProxyState proxyState{
                .Status = NKikimrProto::OK,
                .GroupId = groupId,
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            taskSystem.Enqueue(RunReadTaskAndSendTo(edgeId, TReadTaskArgs{
                .GroupId = groupId,
                .Request = {
                    .Event = MakeGetRequest(),
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::BLOCKED);
            UNIT_ASSERT_VALUES_EQUAL(queueState.Requests, 1u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 0u);
        }

        Y_UNIT_TEST(UsesQueuePathWithoutFallbackOnDeadline) {
            TTestActorSystem runtime(1);
            StartRuntimeWithSharedStateSubsystem(runtime);

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            TFakeQueueState queueState{
                .Status = NKikimrProto::DEADLINE,
            };
            const NActors::TActorId queueActorId = runtime.Register(new TFakeQueueActor(queueState), 1);

            constexpr ui32 groupId = 81;
            auto* sharedStateSubSystem = actorSystem->GetSubSystem<TBlobStorageGroupSharedStateSubSystem>();
            UNIT_ASSERT(sharedStateSubSystem);
            sharedStateSubSystem->Update(groupId, MakeReadySharedState(runtime, queueActorId));

            TFakeProxyState proxyState{
                .Status = NKikimrProto::OK,
                .GroupId = groupId,
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            taskSystem.Enqueue(RunReadTaskAndSendTo(edgeId, TReadTaskArgs{
                .GroupId = groupId,
                .Request = {
                    .Event = MakeGetRequest(),
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::DEADLINE);
            UNIT_ASSERT_VALUES_EQUAL(queueState.Requests, 1u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 0u);
        }

        Y_UNIT_TEST(UsesQueueHotPathWithoutFallbackOnOk) {
            TTestActorSystem runtime(1);
            StartRuntimeWithSharedStateSubsystem(runtime);

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            TFakeQueueState queueState{
                .Status = NKikimrProto::OK,
                .Payload = "queue-hotpath-data",
            };
            const NActors::TActorId queueActorId = runtime.Register(new TFakeQueueActor(queueState), 1);

            constexpr ui32 groupId = 91;
            auto* sharedStateSubSystem = actorSystem->GetSubSystem<TBlobStorageGroupSharedStateSubSystem>();
            UNIT_ASSERT(sharedStateSubSystem);
            sharedStateSubSystem->Update(groupId, MakeReadySharedState(runtime, queueActorId));

            TFakeProxyState proxyState{
                .Status = NKikimrProto::OK,
                .GroupId = groupId,
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            taskSystem.Enqueue(RunReadTaskAndSendTo(edgeId, TReadTaskArgs{
                .GroupId = groupId,
                .Request = {
                    .Event = MakePartialGetRequest(),
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ResponseSz, 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Buffer.size(), 16u);
            UNIT_ASSERT_VALUES_EQUAL(queueState.Requests, 1u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 0u);
        }

        Y_UNIT_TEST(UsesQueueThenFallsBackToProxyOnRace) {
            TTestActorSystem runtime(1);
            StartRuntimeWithSharedStateSubsystem(runtime);

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            TFakeQueueState queueState{
                .Status = NKikimrProto::RACE,
            };
            const NActors::TActorId queueActorId = runtime.Register(new TFakeQueueActor(queueState), 1);

            constexpr ui32 groupId = 73;
            auto* sharedStateSubSystem = actorSystem->GetSubSystem<TBlobStorageGroupSharedStateSubSystem>();
            UNIT_ASSERT(sharedStateSubSystem);
            sharedStateSubSystem->Update(groupId, MakeReadySharedState(runtime, queueActorId));

            TFakeProxyState proxyState{
                .Status = NKikimrProto::OK,
                .GroupId = groupId,
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            taskSystem.Enqueue(RunReadTaskAndSendTo(edgeId, TReadTaskArgs{
                .GroupId = groupId,
                .Request = {
                    .Event = MakeGetRequest(),
                    .RestartCounter = 9,
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(queueState.Requests, 1u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 1u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastRestartCounter, 10u);
        }

    } // Y_UNIT_TEST_SUITE(ReadTask)

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
