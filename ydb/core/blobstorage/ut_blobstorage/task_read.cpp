#include <ydb/core/blobstorage/dsproxy/task/read.h>
#include <ydb/core/blobstorage/dsproxy/task/range.h>
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
                .VDiskReplPausedAtStart = true,
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

        std::unique_ptr<TEvBlobStorage::TEvRange> MakeRangeRequest(
                ui64 tabletId,
                const TLogoBlobID& from,
                const TLogoBlobID& to,
                bool mustRestoreFirst = false,
                bool isIndexOnly = true) {
            return std::make_unique<TEvBlobStorage::TEvRange>(
                tabletId,
                from,
                to,
                mustRestoreFirst,
                TInstant::Max(),
                isIndexOnly);
        }

        task<void> RunReadTaskAndSendTo(const NActors::TActorId& target, TReadTaskArgs args) {
            auto result = co_await RunReadTask(std::move(args));
            UNIT_ASSERT(result);
            NActors::TActivationContext::AsActorContext().Send(target, result.release());
            co_return;
        }

        task<void> RunReadRangeTaskAndSendTo(const NActors::TActorId& target, TReadRangeTaskArgs args) {
            auto result = co_await RunReadRangeTask(std::move(args));
            UNIT_ASSERT(result);
            NActors::TActivationContext::AsActorContext().Send(target, result.release());
            co_return;
        }

        task<void> EraseStateAndRunReadTaskAndSendTo(
                const NActors::TActorId& target,
                TBlobStorageGroupSharedStateSubSystem* subSystem,
                ui32 groupId,
                TReadTaskArgs args) {
            subSystem->Erase(groupId);
            auto result = co_await RunReadTask(std::move(args));
            UNIT_ASSERT(result);
            NActors::TActivationContext::AsActorContext().Send(target, result.release());
            co_return;
        }

        task<void> EraseStateAndRunReadRangeTaskAndSendTo(
                const NActors::TActorId& target,
                TBlobStorageGroupSharedStateSubSystem* subSystem,
                ui32 groupId,
                TReadRangeTaskArgs args) {
            subSystem->Erase(groupId);
            auto result = co_await RunReadRangeTask(std::move(args));
            UNIT_ASSERT(result);
            NActors::TActivationContext::AsActorContext().Send(target, result.release());
            co_return;
        }

        class TProxyGetEventCounterGuard {
        public:
            TProxyGetEventCounterGuard(TEnvironmentSetup& env, ui32 groupId)
                : Env_(env)
                , PrevFilter_(std::move(env.Runtime->FilterFunction))
                , ProxyServiceId_(MakeBlobStorageProxyID(groupId))
                , ProxyActorId_(GetActorSystem(env).LookupLocalService(ProxyServiceId_))
            {
                Env_.Runtime->FilterFunction = [this](ui32, std::unique_ptr<IEventHandle>& ev) {
                    if (ev && ev->GetTypeRewrite() == TEvBlobStorage::EvGet) {
                        const NActors::TActorId& recipient = ev->Recipient;
                        const NActors::TActorId& recipientRewrite = ev->GetRecipientRewrite();
                        if (recipient == ProxyServiceId_
                            || recipientRewrite == ProxyServiceId_
                            || recipient == ProxyActorId_
                            || recipientRewrite == ProxyActorId_)
                        {
                            ++Count_;
                        }
                    }
                    return true;
                };
            }

            ~TProxyGetEventCounterGuard() {
                Env_.Runtime->FilterFunction = std::move(PrevFilter_);
            }

            ui32 GetCount() const {
                return Count_;
            }

        private:
            TEnvironmentSetup& Env_;
            std::function<bool(ui32, std::unique_ptr<IEventHandle>&)> PrevFilter_;
            NActors::TActorId ProxyServiceId_;
            NActors::TActorId ProxyActorId_;
            ui32 Count_ = 0;
        };

        class TProxyRangeEventCounterGuard {
        public:
            TProxyRangeEventCounterGuard(TEnvironmentSetup& env, ui32 groupId)
                : Env_(env)
                , PrevFilter_(std::move(env.Runtime->FilterFunction))
                , ProxyServiceId_(MakeBlobStorageProxyID(groupId))
                , ProxyActorId_(GetActorSystem(env).LookupLocalService(ProxyServiceId_))
            {
                Env_.Runtime->FilterFunction = [this](ui32, std::unique_ptr<IEventHandle>& ev) {
                    if (ev && ev->GetTypeRewrite() == TEvBlobStorage::EvRange) {
                        const NActors::TActorId& recipient = ev->Recipient;
                        const NActors::TActorId& recipientRewrite = ev->GetRecipientRewrite();
                        if (recipient == ProxyServiceId_
                            || recipientRewrite == ProxyServiceId_
                            || recipient == ProxyActorId_
                            || recipientRewrite == ProxyActorId_)
                        {
                            ++Count_;
                        }
                    }
                    return true;
                };
            }

            ~TProxyRangeEventCounterGuard() {
                Env_.Runtime->FilterFunction = std::move(PrevFilter_);
            }

            ui32 GetCount() const {
                return Count_;
            }

        private:
            TEnvironmentSetup& Env_;
            std::function<bool(ui32, std::unique_ptr<IEventHandle>&)> PrevFilter_;
            NActors::TActorId ProxyServiceId_;
            NActors::TActorId ProxyActorId_;
            ui32 Count_ = 0;
        };

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

            TProxyGetEventCounterGuard proxyGetCounter(env, groupId);
            taskSystem.Enqueue(RunReadTaskAndSendTo(edge, TReadTaskArgs{
                .GroupId = groupId,
                .Request = {
                    .Event = MakeGetRequest(blobId, 8, 16),
                }
            }));

            auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ResponseSz, 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Buffer.ConvertToString(), data.substr(8, 16));
            UNIT_ASSERT_VALUES_EQUAL(proxyGetCounter.GetCount(), 0u);
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

            auto request = MakeGetRequest(blobId);
            request->ReaderTabletData = TEvBlobStorage::TEvGet::TReaderTabletData(777, 3);

            auto& taskSystem = GetTaskSystem(env);
            const NActors::TActorId edge = test.Runtime->AllocateEdgeActor(1);
            TProxyGetEventCounterGuard proxyGetCounter(env, groupId);
            taskSystem.Enqueue(EraseStateAndRunReadTaskAndSendTo(edge, &subSystem, groupId, TReadTaskArgs{
                .GroupId = groupId,
                .Request = {
                    .Event = std::move(request),
                }
            }));

            auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ResponseSz, 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Buffer.ConvertToString(), data);
            UNIT_ASSERT(proxyGetCounter.GetCount() > 0);
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

    Y_UNIT_TEST_SUITE(TaskRange) {

        Y_UNIT_TEST(UsesRealQueueHotPathWhenSharedStateReady) {
            TEnvironmentSetup env(MakeEnvSettings());
            TTestInfo test = InitTest(env);

            const ui32 groupId = test.Info->GroupID.GetRawId();
            const TLogoBlobID blobId(1, 1, 3, 0, 64, 0);
            const TString data = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/";
            env.PutBlob(groupId, blobId, data);

            WaitForSharedStateReady(env, groupId);

            auto& taskSystem = GetTaskSystem(env);
            const NActors::TActorId edge = test.Runtime->AllocateEdgeActor(1);

            TProxyRangeEventCounterGuard proxyRangeCounter(env, groupId);
            taskSystem.Enqueue(RunReadRangeTaskAndSendTo(edge, TReadRangeTaskArgs{
                .GroupId = groupId,
                .Request = {
                    .Event = MakeRangeRequest(blobId.TabletID(), blobId, blobId, false, true),
                }
            }));

            auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Id, blobId.FullID());
            UNIT_ASSERT_VALUES_EQUAL(proxyRangeCounter.GetCount(), 0u);
        }

        Y_UNIT_TEST(UsesRealQueueHotPathForFullReadWhenSharedStateReady) {
            TEnvironmentSetup env(MakeEnvSettings());
            TTestInfo test = InitTest(env);

            const ui32 groupId = test.Info->GroupID.GetRawId();
            const TLogoBlobID blobId(1, 1, 8, 0, 64, 0);
            const TString data = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/";
            env.PutBlob(groupId, blobId, data);

            WaitForSharedStateReady(env, groupId);

            auto& taskSystem = GetTaskSystem(env);
            const NActors::TActorId edge = test.Runtime->AllocateEdgeActor(1);

            TProxyRangeEventCounterGuard proxyRangeCounter(env, groupId);
            TProxyGetEventCounterGuard proxyGetCounter(env, groupId);
            taskSystem.Enqueue(RunReadRangeTaskAndSendTo(edge, TReadRangeTaskArgs{
                .GroupId = groupId,
                .Request = {
                    .Event = MakeRangeRequest(blobId.TabletID(), blobId, blobId, false, false),
                }
            }));

            auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Id, blobId.FullID());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses[0].Buffer, data);
            UNIT_ASSERT_VALUES_EQUAL(proxyRangeCounter.GetCount(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(proxyGetCounter.GetCount(), 0u);
        }

        Y_UNIT_TEST(FallsBackToRealProxyWhenSharedStateMissing) {
            TEnvironmentSetup env(MakeEnvSettings());
            TTestInfo test = InitTest(env);

            const ui32 groupId = test.Info->GroupID.GetRawId();
            const TLogoBlobID blobId(1, 1, 4, 0, 16, 0);
            const TString data = "0123456789ABCDEF";
            env.PutBlob(groupId, blobId, data);

            WaitForSharedStateReady(env, groupId);
            auto& subSystem = GetSharedStateSubSystem(env);

            auto& taskSystem = GetTaskSystem(env);
            const NActors::TActorId edge = test.Runtime->AllocateEdgeActor(1);
            TProxyRangeEventCounterGuard proxyRangeCounter(env, groupId);
            taskSystem.Enqueue(EraseStateAndRunReadRangeTaskAndSendTo(edge, &subSystem, groupId, TReadRangeTaskArgs{
                .GroupId = groupId,
                .Request = {
                    .Event = MakeRangeRequest(blobId.TabletID(), blobId, blobId, false, true),
                }
            }));

            auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses.size(), 1u);
            UNIT_ASSERT(proxyRangeCounter.GetCount() > 0);
        }

        Y_UNIT_TEST(ReturnsErrorForMissingRangeRequestEvent) {
            TEnvironmentSetup env(MakeEnvSettings());
            auto& taskSystem = GetTaskSystem(env);

            const NActors::TActorId edge = env.Runtime->AllocateEdgeActor(1);
            taskSystem.Enqueue(RunReadRangeTaskAndSendTo(edge, TReadRangeTaskArgs{
                .GroupId = 0,
            }));

            auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::ERROR);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Responses.size(), 0u);
        }

    } // Y_UNIT_TEST_SUITE(TaskRange)

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
