#include <ydb/core/blobstorage/dsproxy/task/read.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <ydb/library/actors/task/task_system.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBlobStorage::NDSProxy::NTask {

    namespace {

        using NActors::NTask::task;
        using NActors::NTask::TTaskSystem;

        constexpr ui32 TaskExecutorCount = 1;

        TEnvironmentSetup::TSettings MakeEnvSettings() {
            return TEnvironmentSetup::TSettings{
                .PrepareActorSystemSetup = [](ui32, NActors::TActorSystemSetup& setup) {
                    setup.AfterCreateCallbacks.emplace_back([](NActors::TActorSystem& actorSystem) {
                        actorSystem.RegisterSubSystem(std::make_unique<TBlobStorageGroupSharedStateSubSystem>());

                        auto taskSystem = std::make_unique<TTaskSystem>();
                        taskSystem->Initialize(&actorSystem, TaskExecutorCount);
                        actorSystem.RegisterSubSystem(std::move(taskSystem));
                    });
                },
            };
        }

        NActors::TActorSystem& GetActorSystem(TEnvironmentSetup& env) {
            auto* node = env.Runtime->GetNode(1);
            UNIT_ASSERT(node && node->ActorSystem);
            return *node->ActorSystem;
        }

        TTaskSystem& GetTaskSystem(TEnvironmentSetup& env) {
            auto* taskSystem = GetActorSystem(env).GetSubSystem<TTaskSystem>();
            UNIT_ASSERT(taskSystem && taskSystem->IsInitialized());
            return *taskSystem;
        }

        TBlobStorageGroupSharedStateSubSystem& GetSharedStateSubSystem(TEnvironmentSetup& env) {
            auto* subSystem = GetActorSystem(env).GetSubSystem<TBlobStorageGroupSharedStateSubSystem>();
            UNIT_ASSERT(subSystem);
            return *subSystem;
        }

        void WaitForSharedStateReady(TEnvironmentSetup& env, ui32 groupId) {
            auto& subSystem = GetSharedStateSubSystem(env);
            for (ui32 i = 0; i < 100; ++i) {
                if (auto state = subSystem.Find(groupId)) {
                    if (state->IsReadyForGet && state->GroupInfo && state->GroupQueues) {
                        return;
                    }
                }
                env.Sim(TDuration::MilliSeconds(100));
            }
            UNIT_FAIL("shared state did not become ready");
        }

        std::unique_ptr<TEvBlobStorage::TEvGet> MakeGetRequest(const TLogoBlobID& id, ui32 shift = 0, ui32 size = 0) {
            if (!size) {
                size = id.BlobSize();
            }
            return std::make_unique<TEvBlobStorage::TEvGet>(
                id,
                shift,
                size,
                TInstant::Max(),
                NKikimrBlobStorage::FastRead);
        }

        task<void> RunReadTaskAndSendTo(const NActors::TActorId& target, TReadTaskArgs args) {
            auto result = co_await RunReadTask(std::move(args));
            UNIT_ASSERT(result);
            NActors::TActivationContext::AsActorContext().Send(target, result.release());
            co_return;
        }

        ui32 CountProxyGetEvents(TEnvironmentSetup& env, ui32 groupId, const std::function<void()>& action) {
            const NActors::TActorId proxyId = MakeBlobStorageProxyID(groupId);
            ui32 count = 0;

            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev && ev->GetTypeRewrite() == TEvBlobStorage::EvGet && ev->GetRecipientRewrite() == proxyId) {
                    ++count;
                }
                return true;
            };

            action();
            env.Runtime->FilterFunction = {};
            return count;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TaskRead) {

        Y_UNIT_TEST(UsesRealQueueHotPathWithoutProxyFallback) {
            TEnvironmentSetup env(MakeEnvSettings());
            TTestInfo test = InitTest(env);

            const ui32 groupId = test.Info->GroupID.GetRawId();
            const TLogoBlobID blobId(1, 1, 1, 0, 64, 0);
            const TString data = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/";
            env.PutBlob(groupId, blobId, data);

            WaitForSharedStateReady(env, groupId);

            auto& taskSystem = GetTaskSystem(env);
            const NActors::TActorId edge = test.Runtime->AllocateEdgeActor(1);

            const ui32 proxyGetEvents = CountProxyGetEvents(env, groupId, [&] {
                taskSystem.Enqueue(RunReadTaskAndSendTo(edge, TReadTaskArgs{
                    .GroupId = groupId,
                    .Request = {
                        .Event = MakeGetRequest(blobId, 8, 16),
                    }
                }));
            });

            auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ResponseSz, 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Buffer.ConvertToString(), data.substr(8, 16));
            UNIT_ASSERT_VALUES_EQUAL(proxyGetEvents, 0u);
        }

        Y_UNIT_TEST(FallsBackToRealProxyWhenSharedStateMissing) {
            TEnvironmentSetup env(MakeEnvSettings());
            TTestInfo test = InitTest(env);

            const ui32 groupId = test.Info->GroupID.GetRawId();
            const TLogoBlobID blobId(1, 1, 2, 0, 16, 0);
            const TString data = "0123456789ABCDEF";
            env.PutBlob(groupId, blobId, data);

            WaitForSharedStateReady(env, groupId);
            auto& subSystem = GetSharedStateSubSystem(env);
            subSystem.Erase(groupId);
            UNIT_ASSERT(!subSystem.Find(groupId));

            auto request = MakeGetRequest(blobId);
            request->ReaderTabletData = TEvBlobStorage::TEvGet::TReaderTabletData(777, 3);

            auto& taskSystem = GetTaskSystem(env);
            const NActors::TActorId edge = test.Runtime->AllocateEdgeActor(1);

            const ui32 proxyGetEvents = CountProxyGetEvents(env, groupId, [&] {
                taskSystem.Enqueue(RunReadTaskAndSendTo(edge, TReadTaskArgs{
                    .GroupId = groupId,
                    .Request = {
                        .Event = std::move(request),
                    }
                }));
            });

            auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ResponseSz, 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Buffer.ConvertToString(), data);
            UNIT_ASSERT(proxyGetEvents > 0);
        }

        Y_UNIT_TEST(ReturnsErrorForMissingRequestEvent) {
            TEnvironmentSetup env(MakeEnvSettings());
            auto& taskSystem = GetTaskSystem(env);

            const NActors::TActorId edge = env.Runtime->AllocateEdgeActor(1);
            taskSystem.Enqueue(RunReadTaskAndSendTo(edge, TReadTaskArgs{
                .GroupId = 0,
            }));

            auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::ERROR);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ResponseSz, 0u);
        }

    } // Y_UNIT_TEST_SUITE(TaskRead)

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
