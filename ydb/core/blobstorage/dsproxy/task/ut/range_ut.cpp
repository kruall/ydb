#include <ydb/core/blobstorage/dsproxy/task/range.h>

#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/task/task_system.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBlobStorage::NDSProxy::NTask {

    namespace {

        using NActors::NTask::task;

        struct TFakeRangeProxyState {
            NKikimrProto::EReplyStatus Status = NKikimrProto::OK;
            ui32 GroupId = 1;
            ui32 Requests = 0;
            ui32 LastRestartCounter = 0;
            std::optional<ui32> LastForceGroupGeneration = std::nullopt;
            bool ReturnResponse = false;
        };

        class TFakeRangeProxyActor final : public NActors::TActor<TFakeRangeProxyActor> {
        public:
            explicit TFakeRangeProxyActor(TFakeRangeProxyState& state)
                : TActor(&TThis::StateWork)
                , State_(state)
            {
            }

            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvBlobStorage::TEvRange, HandleRange);
                }
            }

        private:
            void HandleRange(TEvBlobStorage::TEvRange::TPtr& ev) {
                ++State_.Requests;
                State_.LastRestartCounter = ev->Get()->RestartCounter;
                State_.LastForceGroupGeneration = ev->Get()->ForceGroupGeneration;

                auto result = std::make_unique<TEvBlobStorage::TEvRangeResult>(
                    State_.Status,
                    ev->Get()->From,
                    ev->Get()->To,
                    State_.GroupId);

                if (State_.ReturnResponse && State_.Status == NKikimrProto::OK) {
                    result->Responses.emplace_back(ev->Get()->From.FullID(), TString(), false, false);
                }

                Send(ev->Sender, result.release(), 0, ev->Cookie);
            }

        private:
            TFakeRangeProxyState& State_;
        };

        std::unique_ptr<TEvBlobStorage::TEvRange> MakeRangeRequest() {
            const TLogoBlobID from(1, 1, 1, 0, 16, 0);
            const TLogoBlobID to(1, 1, 1, 0, 16, 0);
            return std::make_unique<TEvBlobStorage::TEvRange>(
                1,
                from,
                to,
                false,
                TInstant::Max(),
                true);
        }

        task<void> RunReadRangeTaskAndSendTo(const NActors::TActorId& target, TReadRangeTaskArgs args) {
            auto result = co_await RunReadRangeTask(std::move(args));
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

        TBlobStorageGroupSharedStatePtr MakeReadySharedState(TTestActorSystem& runtime) {
            TVector<TActorId> vdisks = {runtime.AllocateEdgeActor(1)};
            auto groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(
                TBlobStorageGroupType::ErasureNone,
                ui32(1),
                ui32(1),
                ui32(1),
                &vdisks);
            auto groupQueues = MakeIntrusive<TGroupQueues>(groupInfo->GetTopology());

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

    Y_UNIT_TEST_SUITE(ReadRangeTask) {

        Y_UNIT_TEST(ForwardsToProxyWhenSharedStateMissing) {
            TTestActorSystem runtime(1);
            runtime.Start();

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            TFakeRangeProxyState proxyState{
                .Status = NKikimrProto::OK,
                .GroupId = 777,
                .ReturnResponse = true,
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeRangeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            auto executionRelay = std::make_shared<TEvBlobStorage::TExecutionRelay>();
            taskSystem.Enqueue(RunReadRangeTaskAndSendTo(edgeId, TReadRangeTaskArgs{
                .GroupId = proxyState.GroupId,
                .Request = {
                    .Event = MakeRangeRequest(),
                    .RestartCounter = 42,
                    .ExecutionRelay = executionRelay,
                    .ForceGroupGeneration = 123u,
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GroupId, 777u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses.size(), 1u);
            UNIT_ASSERT_EQUAL(ev->Get()->ExecutionRelay, executionRelay);

            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 1u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.LastRestartCounter, 42u);
            UNIT_ASSERT(proxyState.LastForceGroupGeneration);
            UNIT_ASSERT_VALUES_EQUAL(*proxyState.LastForceGroupGeneration, 123u);
        }

        Y_UNIT_TEST(RequestEventNullReturnsErrorWithoutFallback) {
            TTestActorSystem runtime(1);
            runtime.Start();

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            TFakeRangeProxyState proxyState{
                .Status = NKikimrProto::OK,
                .GroupId = 42,
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeRangeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            taskSystem.Enqueue(RunReadRangeTaskAndSendTo(edgeId, TReadRangeTaskArgs{
                .GroupId = proxyState.GroupId,
                .Request = {
                    .Event = nullptr,
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::ERROR);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 0u);
        }

        Y_UNIT_TEST(ForwardsToProxyWhenSharedStateReady) {
            TTestActorSystem runtime(1);
            StartRuntimeWithSharedStateSubsystem(runtime);

            auto* actorSystem = runtime.GetNode(1)->ActorSystem.get();
            UNIT_ASSERT(actorSystem);

            NActors::NTask::TTaskSystem taskSystem;
            taskSystem.Initialize(actorSystem, 1);

            constexpr ui32 groupId = 9;
            auto* sharedStateSubSystem = actorSystem->GetSubSystem<TBlobStorageGroupSharedStateSubSystem>();
            UNIT_ASSERT(sharedStateSubSystem);
            sharedStateSubSystem->Update(groupId, MakeReadySharedState(runtime));

            TFakeRangeProxyState proxyState{
                .Status = NKikimrProto::OK,
                .GroupId = groupId,
                .ReturnResponse = true,
            };
            const NActors::TActorId proxyActorId = runtime.Register(new TFakeRangeProxyActor(proxyState), 1);
            runtime.RegisterService(MakeBlobStorageProxyID(proxyState.GroupId), proxyActorId);
            const NActors::TActorId edgeId = runtime.AllocateEdgeActor(1);

            taskSystem.Enqueue(RunReadRangeTaskAndSendTo(edgeId, TReadRangeTaskArgs{
                .GroupId = groupId,
                .Request = {
                    .Event = MakeRangeRequest(),
                }
            }));

            auto ev = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edgeId);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(proxyState.Requests, 1u);
        }

    } // Y_UNIT_TEST_SUITE(ReadRangeTask)

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
