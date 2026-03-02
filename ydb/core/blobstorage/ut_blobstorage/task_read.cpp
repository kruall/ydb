#include <ydb/core/blobstorage/dsproxy/task/read.h>

#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
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
            std::optional<TEvBlobStorage::TEvGet::TReaderTabletData> LastReaderTabletData = std::nullopt;
            std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> LastForceBlockTabletData = std::nullopt;
            bool LastMustRestoreFirst = false;
            bool LastIsIndexOnly = false;
            NKikimrBlobStorage::EGetHandleClass LastGetHandleClass = NKikimrBlobStorage::FastRead;
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
                State_.LastReaderTabletData = ev->Get()->ReaderTabletData;
                State_.LastForceBlockTabletData = ev->Get()->ForceBlockTabletData;
                State_.LastMustRestoreFirst = ev->Get()->MustRestoreFirst;
                State_.LastIsIndexOnly = ev->Get()->IsIndexOnly;
                State_.LastGetHandleClass = ev->Get()->GetHandleClass;

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
                UNIT_ASSERT(request.HasVDiskID());
                const TVDiskID vdiskId = VDiskIDFromVDiskID(request.GetVDiskID());
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
                        const TLogoBlobID id = LogoBlobIDFromLogoBlobID(query.GetId());
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

        std::unique_ptr<TEvBlobStorage::TEvGet> MakeIndexRestoreGetRequest() {
            return std::make_unique<TEvBlobStorage::TEvGet>(
                TLogoBlobID(1, 1, 1, 0, 64, 0),
                0,
                16,
                TInstant::Max(),
                NKikimrBlobStorage::FastRead,
                true,
                true,
                TEvBlobStorage::TEvGet::TForceBlockTabletData(42, 9));
        }

        task<void> RunReadTaskAndSendTo(const NActors::TActorId& target, TReadTaskArgs args) {
            auto result = co_await RunReadTask(std::move(args));
            UNIT_ASSERT(result);
            NActors::TActivationContext::AsActorContext().Send(target, result.release());
            co_return;
        }

        void StartRuntimeWithSharedStateSubsystem(TTestActorSystem& runtime) {
            runtime.AddActorSystemSetupCallback([](ui32, NActors::TActorSystemSetup& setup) {
                setup.AfterCreateCallbacks.emplace_back([](NActors::TActorSystem& actorSystem) {
                    actorSystem.RegisterSubSystem(std::make_unique<TBlobStorageGroupSharedStateSubSystem>());
                });
            });
            runtime.Start();
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

    Y_UNIT_TEST_SUITE(TaskRead) {

        Y_UNIT_TEST(QueueHotPathWithoutFallbackOnOk) {
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

            constexpr ui32 groupId = 301;
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

        Y_UNIT_TEST(FallbackKeepsReaderTabletDataWhenSharedStateMissing) {
            TTestActorSystem runtime(1);
            runtime.Start();

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            TFakeProxyState proxyState{
                .Status = NKikimrProto::BLOCKED,
                .GroupId = 302,
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            auto request = MakeGetRequest();
            request->ReaderTabletData = TEvBlobStorage::TEvGet::TReaderTabletData(1001, 12);
            taskSystem.Enqueue(RunReadTaskAndSendTo(edgeId, TReadTaskArgs{
                .GroupId = proxyState.GroupId,
                .Request = {
                    .Event = std::move(request),
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::BLOCKED);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 1u);
            UNIT_ASSERT(proxyState.LastReaderTabletData);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastReaderTabletData->Id, 1001u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastReaderTabletData->Generation, 12u);
        }

        Y_UNIT_TEST(PreservesIndexRestoreFlagsAndForceBlockOnRaceFallback) {
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

            constexpr ui32 groupId = 303;
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

            auto request = MakeIndexRestoreGetRequest();
            request->ReaderTabletData = TEvBlobStorage::TEvGet::TReaderTabletData(42, 8);
            taskSystem.Enqueue(RunReadTaskAndSendTo(edgeId, TReadTaskArgs{
                .GroupId = groupId,
                .Request = {
                    .Event = std::move(request),
                    .RestartCounter = 7,
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(queueState.Requests, 1u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 1u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastRestartCounter, 8u);
            UNIT_ASSERT(proxyState.LastReaderTabletData);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastReaderTabletData->Id, 42u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastReaderTabletData->Generation, 8u);
            UNIT_ASSERT(proxyState.LastForceBlockTabletData);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastForceBlockTabletData->Id, 42u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastForceBlockTabletData->Generation, 9u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastMustRestoreFirst, true);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastIsIndexOnly, true);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastGetHandleClass, NKikimrBlobStorage::FastRead);
        }

    } // Y_UNIT_TEST_SUITE(TaskRead)

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
