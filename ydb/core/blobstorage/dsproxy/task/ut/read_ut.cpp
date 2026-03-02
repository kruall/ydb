#include <ydb/core/blobstorage/dsproxy/task/read.h>

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
            std::optional<ui32> LastForceGroupGeneration = std::nullopt;
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

                auto result = std::make_unique<TEvBlobStorage::TEvGetResult>(State_.Status, 0, State_.GroupId);
                Send(ev->Sender, result.release(), 0, ev->Cookie);
            }

        private:
            TFakeProxyState& State_;
        };

        std::unique_ptr<TEvBlobStorage::TEvGet> MakeGetRequest() {
            return std::make_unique<TEvBlobStorage::TEvGet>(
                TLogoBlobID(1, 1, 1, 0, 16, 0),
                0,
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
            runtime.Start();

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

    } // Y_UNIT_TEST_SUITE(ReadTask)

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
